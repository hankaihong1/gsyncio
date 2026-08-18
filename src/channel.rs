use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[pyclass(module = "multiloop._multiloop_core")]
pub struct Channel {
    sender: flume::Sender<Py<PyAny>>,
    receiver: flume::Receiver<Py<PyAny>>,
    is_closed: Arc<AtomicBool>,
}

#[pymethods]
impl Channel {
    #[new]
    pub fn new(maxsize: usize) -> Self {
        let (sender, receiver) = if maxsize > 0 {
            flume::bounded(maxsize)
        } else {
            flume::unbounded()
        };
        Channel {
            sender,
            receiver,
            is_closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire) || self.sender.is_disconnected()
    }

    pub fn try_send(&self, item: Py<PyAny>) -> PyResult<bool> {
        if self.is_closed() {
            return Err(PyRuntimeError::new_err("Channel is closed"));
        }
        match self.sender.try_send(item) {
            Ok(_) => Ok(true),
            Err(flume::TrySendError::Full(_)) => Ok(false),
            Err(flume::TrySendError::Disconnected(_)) => {
                Err(PyRuntimeError::new_err("Channel is closed"))
            }
        }
    }

    /// Non-blocking receive.
    ///
    /// Returns `(has_item, item)`: `has_item` distinguishes "channel empty"
    /// from "item is None" — PyO3 maps both `Ok(None)` and `Ok(Some(None))`
    /// to Python `None`, so a bare `Option` return would lose None payloads.
    pub fn try_recv(&self, _py: Python<'_>) -> PyResult<(bool, Option<Py<PyAny>>)> {
        match self.receiver.try_recv() {
            Ok(item) => Ok((true, Some(item))),
            Err(flume::TryRecvError::Empty) => {
                if self.is_closed() {
                    Err(PyRuntimeError::new_err("Channel is closed"))
                } else {
                    Ok((false, None))
                }
            }
            Err(flume::TryRecvError::Disconnected) => {
                Err(PyRuntimeError::new_err("Channel is closed"))
            }
        }
    }

    pub fn qsize(&self) -> usize {
        self.receiver.len()
    }
}

/// A registered async waiter: (event loop, future, optional channel token).
pub(crate) struct ChannelWaiter {
    pub(crate) event_loop: Py<PyAny>,
    pub(crate) future: Py<PyAny>,
    pub(crate) channel_token: Option<Py<PyAny>>,
}

pub(crate) struct ChannelState {
    pub(crate) buffer: VecDeque<Py<PyAny>>,
    pub(crate) maxsize: usize,
    pub(crate) is_closed: bool,
    pub(crate) getters: VecDeque<ChannelWaiter>,
    pub(crate) putters: VecDeque<ChannelWaiter>,
    pub(crate) select_watchers: VecDeque<ChannelWaiter>,
}

#[pyclass(module = "multiloop._multiloop_core")]
pub struct RawAsyncChannel {
    pub(crate) state: Arc<Mutex<ChannelState>>,
    wake_fn: Option<Py<PyAny>>,
    select_wake_fn: Option<Py<PyAny>>,
}

/// Target to wake outside of the Mutex lock to eliminate GIL-Mutex deadlocks.
pub(crate) enum WakeTarget {
    Waiter(ChannelWaiter, Option<Py<PyAny>>, bool),
    SelectWatcher(ChannelWaiter, Py<PyAny>),
}

impl RawAsyncChannel {
    fn wake_waiter(
        &self,
        py: Python<'_>,
        waiter: &ChannelWaiter,
        val: Option<&Bound<'_, PyAny>>,
        is_exc: bool,
    ) {
        if let Some(ref w_fn) = self.wake_fn {
            let loop_obj = waiter.event_loop.bind(py);
            let fut = waiter.future.bind(py);
            let res = match val {
                Some(v) => loop_obj.call_method1(
                    "call_soon_threadsafe",
                    (w_fn.bind(py), fut, v, is_exc, true),
                ),
                None => loop_obj.call_method1(
                    "call_soon_threadsafe",
                    (w_fn.bind(py), fut, py.None(), is_exc, false),
                ),
            };
            let _ = res;
        }
    }

    fn wake_select_watcher(
        &self,
        py: Python<'_>,
        watcher: &ChannelWaiter,
        channel_obj: &Bound<'_, PyAny>,
    ) {
        if let Some(ref sw_fn) = self.select_wake_fn {
            let loop_obj = watcher.event_loop.bind(py);
            let fut = watcher.future.bind(py);
            let _ =
                loop_obj.call_method1("call_soon_threadsafe", (sw_fn.bind(py), fut, channel_obj));
        }
    }

    pub(crate) fn dispatch_wake(&self, py: Python<'_>, target: WakeTarget) {
        match target {
            WakeTarget::Waiter(w, val, is_exc) => {
                self.wake_waiter(py, &w, val.as_ref().map(|v| v.bind(py)), is_exc);
            }
            WakeTarget::SelectWatcher(w, token) => {
                self.wake_select_watcher(py, &w, token.bind(py));
            }
        }
    }
}

#[pymethods]
impl RawAsyncChannel {
    #[new]
    #[pyo3(signature = (maxsize = 0, wake_fn = None, select_wake_fn = None))]
    pub fn new(
        maxsize: usize,
        wake_fn: Option<Py<PyAny>>,
        select_wake_fn: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ChannelState {
                maxsize,
                buffer: VecDeque::new(),
                is_closed: false,
                getters: VecDeque::new(),
                putters: VecDeque::new(),
                select_watchers: VecDeque::new(),
            })),
            wake_fn,
            select_wake_fn,
        }
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let mut to_wake = Vec::new();
        {
            let mut guard = self.state.lock();
            if guard.is_closed {
                return Ok(());
            }
            guard.is_closed = true;
            let closed_exc: Py<PyAny> = PyRuntimeError::new_err("Channel is closed")
                .into_value(py)
                .into_any();
            for g in guard.getters.drain(..) {
                to_wake.push(WakeTarget::Waiter(g, Some(closed_exc.clone_ref(py)), true));
            }
            for p in guard.putters.drain(..) {
                to_wake.push(WakeTarget::Waiter(p, Some(closed_exc.clone_ref(py)), true));
            }
            for w in guard.select_watchers.drain(..) {
                let token = w
                    .channel_token
                    .as_ref()
                    .map(|t| t.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                to_wake.push(WakeTarget::SelectWatcher(w, token));
            }
        }
        for target in to_wake {
            self.dispatch_wake(py, target);
        }
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().is_closed
    }

    pub fn qsize(&self) -> usize {
        self.state.lock().buffer.len()
    }

    #[getter]
    pub fn maxsize(&self) -> usize {
        self.state.lock().maxsize
    }

    pub fn empty(&self) -> bool {
        self.state.lock().buffer.is_empty()
    }

    pub fn full(&self) -> bool {
        let guard = self.state.lock();
        guard.maxsize > 0 && guard.buffer.len() >= guard.maxsize
    }

    pub fn try_send(&self, py: Python<'_>, item: Py<PyAny>) -> PyResult<bool> {
        let mut to_wake = Vec::new();
        {
            let mut guard = self.state.lock();
            if guard.is_closed {
                return Err(PyRuntimeError::new_err("Channel is closed"));
            }
            if let Some(getter) = guard.getters.pop_front() {
                to_wake.push(WakeTarget::Waiter(getter, Some(item), false));
                if guard.maxsize > 0 {
                    while !guard.putters.is_empty() && guard.buffer.len() < guard.maxsize {
                        if let Some(next_putter) = guard.putters.pop_front() {
                            to_wake.push(WakeTarget::Waiter(next_putter, None, false));
                        }
                    }
                }
            } else if let Some(watcher) = guard.select_watchers.pop_front() {
                guard.buffer.push_back(item);
                let token = watcher
                    .channel_token
                    .as_ref()
                    .map(|t| t.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                to_wake.push(WakeTarget::SelectWatcher(watcher, token));
            } else {
                if guard.maxsize > 0 && guard.buffer.len() >= guard.maxsize {
                    return Ok(false);
                }
                guard.buffer.push_back(item);
            }
        }
        for target in to_wake {
            self.dispatch_wake(py, target);
        }
        Ok(true)
    }

    pub fn try_recv(&self, py: Python<'_>) -> PyResult<(bool, Option<Py<PyAny>>)> {
        let mut to_wake = None;
        let res = {
            let mut guard = self.state.lock();
            if let Some(item) = guard.buffer.pop_front() {
                if guard.maxsize > 0 {
                    if let Some(putter) = guard.putters.pop_front() {
                        to_wake = Some(WakeTarget::Waiter(putter, None, false));
                    }
                }
                Ok((true, Some(item)))
            } else if guard.is_closed {
                Err(PyRuntimeError::new_err("Channel is closed"))
            } else {
                Ok((false, None))
            }
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        res
    }

    pub fn register_getter(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<(bool, Option<Py<PyAny>>)> {
        let mut to_wake = None;
        let res = {
            let mut guard = self.state.lock();
            if guard.getters.is_empty() && !guard.buffer.is_empty() {
                let item = guard.buffer.pop_front().unwrap();
                if guard.maxsize > 0 {
                    if let Some(putter) = guard.putters.pop_front() {
                        to_wake = Some(WakeTarget::Waiter(putter, None, false));
                    }
                }
                Ok((true, Some(item)))
            } else if guard.is_closed {
                Err(PyRuntimeError::new_err("Channel is closed"))
            } else {
                guard.getters.push_back(ChannelWaiter {
                    event_loop: loop_obj,
                    future: fut,
                    channel_token: None,
                });
                Ok((false, None))
            }
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        res
    }

    pub fn unregister_getter(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> PyResult<bool> {
        let mut to_wake = None;
        let removed = {
            let mut guard = self.state.lock();
            let before = guard.getters.len();
            guard.getters.retain(|g| !g.future.bind(py).is(fut));
            let removed = guard.getters.len() != before;
            if !removed && !guard.buffer.is_empty() {
                if let Some(next_getter) = guard.getters.pop_front() {
                    let item = guard.buffer.pop_front().unwrap();
                    to_wake = Some(WakeTarget::Waiter(next_getter, Some(item), false));
                } else if let Some(watcher) = guard.select_watchers.pop_front() {
                    let token = watcher
                        .channel_token
                        .as_ref()
                        .map(|t| t.clone_ref(py))
                        .unwrap_or_else(|| py.None());
                    to_wake = Some(WakeTarget::SelectWatcher(watcher, token));
                }
            }
            removed
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        Ok(removed)
    }

    pub fn register_putter(
        &self,
        _py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<bool> {
        let mut guard = self.state.lock();
        if guard.is_closed {
            return Err(PyRuntimeError::new_err("Channel is closed"));
        }
        if guard.putters.is_empty() && (guard.maxsize == 0 || guard.buffer.len() < guard.maxsize) {
            return Ok(true);
        }
        guard.putters.push_back(ChannelWaiter {
            event_loop: loop_obj,
            future: fut,
            channel_token: None,
        });
        Ok(false)
    }

    pub fn unregister_putter(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> PyResult<bool> {
        let mut to_wake = None;
        let removed = {
            let mut guard = self.state.lock();
            let before = guard.putters.len();
            guard.putters.retain(|p| !p.future.bind(py).is(fut));
            let removed = guard.putters.len() != before;
            if !removed && (guard.maxsize == 0 || guard.buffer.len() < guard.maxsize) {
                if let Some(next_putter) = guard.putters.pop_front() {
                    to_wake = Some(WakeTarget::Waiter(next_putter, None, false));
                }
            }
            removed
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        Ok(removed)
    }

    pub fn register_select_watcher(
        &self,
        _py: Python<'_>,
        loop_obj: Py<PyAny>,
        arbiter_fut: Py<PyAny>,
        channel_token: Py<PyAny>,
    ) -> PyResult<bool> {
        let mut guard = self.state.lock();
        if guard.is_closed && guard.buffer.is_empty() {
            return Ok(false);
        }
        if !guard.buffer.is_empty() {
            return Ok(false);
        }
        guard.select_watchers.push_back(ChannelWaiter {
            event_loop: loop_obj,
            future: arbiter_fut,
            channel_token: Some(channel_token),
        });
        Ok(true)
    }

    pub fn unregister_select_watcher(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> bool {
        let mut guard = self.state.lock();
        let before = guard.select_watchers.len();
        guard.select_watchers.retain(|w| !w.future.bind(py).is(fut));
        guard.select_watchers.len() != before
    }

    pub fn forward_select_wakeup(&self, py: Python<'_>) {
        let mut to_wake = None;
        {
            let mut guard = self.state.lock();
            if !guard.buffer.is_empty() && guard.getters.is_empty() {
                if let Some(watcher) = guard.select_watchers.pop_front() {
                    let token = watcher
                        .channel_token
                        .as_ref()
                        .map(|t| t.clone_ref(py))
                        .unwrap_or_else(|| py.None());
                    to_wake = Some(WakeTarget::SelectWatcher(watcher, token));
                }
            }
        }
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
    }

    pub fn getters_len(&self) -> usize {
        self.state.lock().getters.len()
    }

    pub fn putters_len(&self) -> usize {
        self.state.lock().putters.len()
    }

    pub fn notifiers_len(&self) -> usize {
        self.state.lock().select_watchers.len()
    }
}
