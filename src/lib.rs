use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

create_exception!(
    _gsyncio_core,
    ThreadPoolClosedError,
    pyo3::exceptions::PyException
);

/// Bounded capacity of each per-worker local queue.
const _LOCAL_QUEUE_CAPACITY: usize = 256;

/// Each counter is 64-byte-aligned to prevent false sharing between
/// adjacent worker counters on multi-core systems.
#[repr(align(64))]
struct PaddedAtomic(AtomicUsize);

#[pyclass(module = "gsyncio._gsyncio_core")]
pub struct AtomicMetrics {
    active: Vec<PaddedAtomic>,
    completed: Vec<PaddedAtomic>,
    global_pull_count: Vec<PaddedAtomic>,
    park_count: Vec<PaddedAtomic>,
    injection_queue_depth: Vec<PaddedAtomic>,
    remote_schedule_count: Vec<PaddedAtomic>,
}

#[pymethods]
impl AtomicMetrics {
    #[new]
    fn new(num_threads: usize) -> Self {
        let mut active = Vec::with_capacity(num_threads);
        let mut completed = Vec::with_capacity(num_threads);
        let mut global_pull_count = Vec::with_capacity(num_threads);
        let mut park_count = Vec::with_capacity(num_threads);
        let mut injection_queue_depth = Vec::with_capacity(num_threads);
        let mut remote_schedule_count = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            active.push(PaddedAtomic(AtomicUsize::new(0)));
            completed.push(PaddedAtomic(AtomicUsize::new(0)));
            global_pull_count.push(PaddedAtomic(AtomicUsize::new(0)));
            park_count.push(PaddedAtomic(AtomicUsize::new(0)));
            injection_queue_depth.push(PaddedAtomic(AtomicUsize::new(0)));
            remote_schedule_count.push(PaddedAtomic(AtomicUsize::new(0)));
        }
        AtomicMetrics {
            active,
            completed,
            global_pull_count,
            park_count,
            injection_queue_depth,
            remote_schedule_count,
        }
    }

    fn inc_active(&self, index: usize) {
        if index < self.active.len() {
            self.active[index].0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dec_active(&self, index: usize) {
        if index < self.active.len() {
            self.active[index].0.fetch_sub(1, Ordering::Relaxed);
            self.completed[index].0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_active(&self, index: usize) -> usize {
        if index < self.active.len() {
            self.active[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn get_completed(&self, index: usize) -> usize {
        if index < self.completed.len() {
            self.completed[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn inc_global_pull(&self, index: usize) {
        if index < self.global_pull_count.len() {
            self.global_pull_count[index]
                .0
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_global_pull(&self, index: usize) -> usize {
        if index < self.global_pull_count.len() {
            self.global_pull_count[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn inc_park(&self, index: usize) {
        if index < self.park_count.len() {
            self.park_count[index].0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_park(&self, index: usize) -> usize {
        if index < self.park_count.len() {
            self.park_count[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn set_injection_queue_depth(&self, index: usize, depth: usize) {
        if index < self.injection_queue_depth.len() {
            self.injection_queue_depth[index]
                .0
                .store(depth, Ordering::Relaxed);
        }
    }

    fn get_injection_queue_depth(&self, index: usize) -> usize {
        if index < self.injection_queue_depth.len() {
            self.injection_queue_depth[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn inc_remote_schedule(&self, index: usize) {
        if index < self.remote_schedule_count.len() {
            self.remote_schedule_count[index]
                .0
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_remote_schedule(&self, index: usize) -> usize {
        if index < self.remote_schedule_count.len() {
            self.remote_schedule_count[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }
}

/// RAII guard that decrements an `AtomicUsize` counter on drop.
/// Used to ensure `num_polling` is always decremented even if a panic
/// occurs between `fetch_add` and the corresponding `fetch_sub`.
struct PollerGuard<'a>(&'a AtomicUsize);

impl Drop for PollerGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Native Rust Worker Pool Core for zero-overhead task queueing and work stealing.
#[pyclass(module = "gsyncio._gsyncio_core")]
pub struct NativeWorkerPool {
    global_sender: Mutex<Option<flume::Sender<Py<PyAny>>>>,
    global_receiver: flume::Receiver<Py<PyAny>>,
    local_senders: Mutex<Vec<flume::Sender<Py<PyAny>>>>,
    local_receivers: Vec<flume::Receiver<Py<PyAny>>>,
    buffers: Vec<Mutex<VecDeque<Py<PyAny>>>>,
    is_closed: Arc<AtomicBool>,
    num_polling: AtomicUsize,
    metrics: Mutex<Option<Py<AtomicMetrics>>>,
}

#[pymethods]
impl NativeWorkerPool {
    #[new]
    fn new(num_threads: usize) -> Self {
        let (global_sender, global_receiver) = flume::unbounded();
        let mut local_senders = Vec::with_capacity(num_threads);
        let mut local_receivers = Vec::with_capacity(num_threads);
        let mut buffers = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let (tx, rx) = flume::bounded(_LOCAL_QUEUE_CAPACITY);
            local_senders.push(tx);
            local_receivers.push(rx);
            buffers.push(Mutex::new(VecDeque::new()));
        }

        NativeWorkerPool {
            global_sender: Mutex::new(Some(global_sender)),
            global_receiver,
            local_senders: Mutex::new(local_senders),
            local_receivers,
            buffers,
            is_closed: Arc::new(AtomicBool::new(false)),
            num_polling: AtomicUsize::new(0),
            metrics: Mutex::new(None),
        }
    }

    fn close(&self) {
        // Drop senders first so receivers become disconnected after draining,
        // then set the advisory flag so is_closed() returns true immediately.
        *self.global_sender.lock() = None;
        self.local_senders.lock().clear();
        self.is_closed.store(true, Ordering::Release); // Flag store: Release makes the close visible to Acquire loads
    }

    fn set_metrics(&self, metrics: Py<AtomicMetrics>) {
        *self.metrics.lock() = Some(metrics);
    }

    fn is_closed(&self) -> bool {
        if self.is_closed.load(Ordering::Acquire) {
            // Flag load: Acquire observes close store
            return true;
        }
        self.global_sender
            .lock()
            .as_ref()
            .is_none_or(|s| s.is_disconnected())
    }

    fn is_drained(&self) -> bool {
        if !self.global_receiver.is_empty() {
            return false;
        }
        for buf in &self.buffers {
            if !buf.lock().is_empty() {
                return false;
            }
        }
        for rx in &self.local_receivers {
            if !rx.is_empty() {
                return false;
            }
        }
        true
    }

    fn push_global(&self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        // WHY: pop_work() signals "no work" with Ok(None).  A None task pushed
        // here would therefore be silently swallowed by every worker — reject
        // it at the boundary instead of losing work.
        if task.is_none(py) {
            return Err(PyTypeError::new_err("pool task cannot be None"));
        }
        // Fast path: check advisory flag before acquiring lock — Acquire observes close store
        if self.is_closed.load(Ordering::Acquire) {
            return Err(ThreadPoolClosedError::new_err("Pool is closed"));
        }
        let guard = self.global_sender.lock();
        match guard.as_ref() {
            Some(sender) => {
                sender
                    .send(task)
                    .map_err(|_| ThreadPoolClosedError::new_err("Pool is closed"))?;
                Ok(())
            }
            None => Err(ThreadPoolClosedError::new_err("Pool is closed")),
        }
    }

    fn push_local(&self, index: usize, task: Py<PyAny>, py: Python<'_>) -> PyResult<()> {
        // Same None rejection as push_global: a None "task" would be read by
        // pop_work() as "no work" and dropped.
        if task.is_none(py) {
            return Err(PyTypeError::new_err("pool task cannot be None"));
        }
        // Fast path: check advisory flag before acquiring lock — Acquire observes close store
        if self.is_closed.load(Ordering::Acquire) {
            return Err(ThreadPoolClosedError::new_err("Pool is closed"));
        }
        let guard = self.local_senders.lock();
        // WHY (R10): close() clears the senders before setting the flag, so
        // a lock-side is_closed() check can still see False mid-close; an
        // empty sender list IS the closed state — report it as such instead
        // of the misleading "Worker index out of range".
        if guard.is_empty() {
            return Err(ThreadPoolClosedError::new_err("Pool is closed"));
        }
        if index >= guard.len() {
            return Err(PyRuntimeError::new_err("Worker index out of range"));
        }
        match guard[index].try_send(task) {
            Ok(_) => {
                // Track remote schedule: explicit routing to a specific worker.
                if let Some(ref metrics) = *self.metrics.lock() {
                    metrics.borrow(py).inc_remote_schedule(index);
                }
                Ok(())
            }
            Err(flume::TrySendError::Full(task)) => {
                // Local queue full — fall back to global (drop lock first)
                drop(guard);
                self.push_global(py, task)
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                Err(ThreadPoolClosedError::new_err("Pool is closed"))
            }
        }
    }

    fn pop_work(&self, index: usize, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        // Helper to sample injection queue depth from the live channel state.
        let sample_depth = |metrics: &Py<AtomicMetrics>, idx: usize, depth: usize| {
            metrics.borrow(py).set_injection_queue_depth(idx, depth);
        };

        // 1. Check per-worker buffer first (cached from previous batch pulls).
        if index < self.buffers.len() {
            let mut buffer = self.buffers[index].lock();
            if let Some(task) = buffer.pop_front() {
                return Ok(Some(task));
            }
            // Buffer empty – try batch pull from global (lock still held).
            let is_draining = self.is_closed.load(Ordering::Acquire);
            let num_workers = self.local_receivers.len();
            let max_pollers = std::cmp::max(1, num_workers / 2);
            let is_poller =
                is_draining || (self.num_polling.fetch_add(1, Ordering::Relaxed) < max_pollers);
            let _guard = if !is_draining {
                Some(PollerGuard(&self.num_polling))
            } else {
                None
            };
            if is_poller {
                let global_len = self.global_receiver.len();
                if global_len > 0 {
                    let batch_size = if is_draining {
                        std::cmp::min(global_len, 128)
                    } else {
                        global_len
                            .checked_div(num_workers)
                            .map_or(1, |d| std::cmp::min(d + 1, 128))
                    };
                    let mut pulled = 0usize;
                    for _ in 0..batch_size {
                        match self.global_receiver.try_recv() {
                            Ok(task) => {
                                buffer.push_back(task);
                                pulled += 1;
                            }
                            Err(_) => break,
                        }
                    }
                    if pulled > 0 {
                        if let Some(ref metrics) = *self.metrics.lock() {
                            for _ in 0..pulled {
                                metrics.borrow(py).inc_global_pull(index);
                            }
                            let new_depth = self.global_receiver.len();
                            sample_depth(metrics, index, new_depth);
                        }
                    }
                    if let Some(task) = buffer.pop_front() {
                        return Ok(Some(task));
                    }
                }
            }
        }

        // 2. Check local dedicated queue.
        if index < self.local_receivers.len() {
            if let Ok(task) = self.local_receivers[index].try_recv() {
                return Ok(Some(task));
            }
        }

        // 3. Exit condition: only when in Draining state and all sources (global queue, buffer, local queue) are completely empty!
        let is_draining = self.is_closed.load(Ordering::Acquire);
        if is_draining {
            let buffer_empty = index >= self.buffers.len() || self.buffers[index].lock().is_empty();
            let local_empty =
                index >= self.local_receivers.len() || self.local_receivers[index].is_empty();
            if self.global_receiver.is_empty() && buffer_empty && local_empty {
                return Err(ThreadPoolClosedError::new_err("Pool is closed and drained"));
            } else {
                return Ok(None); // Still draining, yield to let other workers/events run
            }
        }

        // Worker idle — increment park count.
        if let Some(ref metrics) = *self.metrics.lock() {
            metrics.borrow(py).inc_park(index);
            let depth = self.global_receiver.len();
            sample_depth(metrics, index, depth);
        }
        Ok(None)
    }
}

#[pyclass(module = "gsyncio._gsyncio_core")]
pub struct FastChannel {
    sender: flume::Sender<Py<PyAny>>,
    receiver: flume::Receiver<Py<PyAny>>,
    is_closed: Arc<AtomicBool>,
}

#[pymethods]
impl FastChannel {
    #[new]
    fn new(maxsize: usize) -> Self {
        let (sender, receiver) = if maxsize > 0 {
            flume::bounded(maxsize)
        } else {
            flume::unbounded()
        };
        FastChannel {
            sender,
            receiver,
            is_closed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn close(&self) {
        self.is_closed.store(true, Ordering::Release); // Flag store: Release makes the close visible to Acquire loads
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire) || self.sender.is_disconnected() // Flag load: Acquire observes close store
    }

    fn try_send(&self, item: Py<PyAny>) -> PyResult<bool> {
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
    fn try_recv(&self, _py: Python<'_>) -> PyResult<(bool, Option<Py<PyAny>>)> {
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

    fn qsize(&self) -> usize {
        self.receiver.len()
    }
}

/// A registered async waiter: (event loop, future, optional channel token).
struct ChannelWaiter {
    event_loop: Py<PyAny>,
    future: Py<PyAny>,
    channel_token: Option<Py<PyAny>>,
}

struct ChannelState {
    buffer: VecDeque<Py<PyAny>>,
    maxsize: usize,
    is_closed: bool,
    getters: VecDeque<ChannelWaiter>,
    putters: VecDeque<ChannelWaiter>,
    select_watchers: VecDeque<ChannelWaiter>,
}

#[pyclass(module = "gsyncio._gsyncio_core")]
pub struct RawAsyncChannel {
    state: Arc<Mutex<ChannelState>>,
    wake_fn: Option<Py<PyAny>>,
    select_wake_fn: Option<Py<PyAny>>,
}

/// Target to wake outside of the Mutex lock to eliminate GIL-Mutex deadlocks.
enum WakeTarget {
    Waiter(ChannelWaiter, Option<Py<PyAny>>),
    SelectWatcher(ChannelWaiter, Py<PyAny>),
}

impl RawAsyncChannel {
    fn wake_waiter(&self, py: Python<'_>, waiter: &ChannelWaiter, exc: Option<&Bound<'_, PyAny>>) {
        if let Some(ref w_fn) = self.wake_fn {
            let loop_obj = waiter.event_loop.bind(py);
            let fut = waiter.future.bind(py);
            let res = match exc {
                Some(e) => loop_obj.call_method1("call_soon_threadsafe", (w_fn.bind(py), fut, e)),
                None => loop_obj.call_method1("call_soon_threadsafe", (w_fn.bind(py), fut)),
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

    fn dispatch_wake(&self, py: Python<'_>, target: WakeTarget) {
        match target {
            WakeTarget::Waiter(w, exc) => {
                self.wake_waiter(py, &w, exc.as_ref().map(|e| e.bind(py)));
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
    fn new(maxsize: usize, wake_fn: Option<Py<PyAny>>, select_wake_fn: Option<Py<PyAny>>) -> Self {
        RawAsyncChannel {
            state: Arc::new(Mutex::new(ChannelState {
                buffer: VecDeque::new(),
                maxsize,
                is_closed: false,
                getters: VecDeque::new(),
                putters: VecDeque::new(),
                select_watchers: VecDeque::new(),
            })),
            wake_fn,
            select_wake_fn,
        }
    }

    fn close(&self, py: Python<'_>) {
        let mut to_wake = Vec::new();
        {
            let mut guard = self.state.lock();
            if guard.is_closed {
                return;
            }
            guard.is_closed = true;
            let closed_exc: Py<PyAny> = PyRuntimeError::new_err("Channel is closed")
                .into_value(py)
                .into_any();

            let getters = std::mem::take(&mut guard.getters);
            for g in getters {
                to_wake.push(WakeTarget::Waiter(g, Some(closed_exc.clone_ref(py))));
            }
            let putters = std::mem::take(&mut guard.putters);
            for p in putters {
                to_wake.push(WakeTarget::Waiter(p, Some(closed_exc.clone_ref(py))));
            }
            let watchers = std::mem::take(&mut guard.select_watchers);
            for w in watchers {
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
    }

    fn is_closed(&self) -> bool {
        self.state.lock().is_closed
    }

    fn qsize(&self) -> usize {
        self.state.lock().buffer.len()
    }

    #[getter]
    fn maxsize(&self) -> usize {
        self.state.lock().maxsize
    }

    fn empty(&self) -> bool {
        self.state.lock().buffer.is_empty()
    }

    fn full(&self) -> bool {
        let guard = self.state.lock();
        guard.maxsize > 0 && guard.buffer.len() >= guard.maxsize
    }

    fn try_send(&self, py: Python<'_>, item: Py<PyAny>) -> PyResult<bool> {
        let mut to_wake = None;
        {
            let mut guard = self.state.lock();
            if guard.is_closed {
                return Err(PyRuntimeError::new_err("Channel is closed"));
            }
            if guard.maxsize > 0 && guard.buffer.len() >= guard.maxsize {
                return Ok(false);
            }
            guard.buffer.push_back(item);

            if let Some(getter) = guard.getters.pop_front() {
                to_wake = Some(WakeTarget::Waiter(getter, None));
            } else if let Some(watcher) = guard.select_watchers.pop_front() {
                let token = watcher
                    .channel_token
                    .as_ref()
                    .map(|t| t.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                to_wake = Some(WakeTarget::SelectWatcher(watcher, token));
            }
        }
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        Ok(true)
    }

    fn try_recv(&self, py: Python<'_>) -> PyResult<(bool, Option<Py<PyAny>>)> {
        let mut to_wake = None;
        let res = {
            let mut guard = self.state.lock();
            if let Some(item) = guard.buffer.pop_front() {
                if guard.maxsize > 0 {
                    if let Some(putter) = guard.putters.pop_front() {
                        to_wake = Some(WakeTarget::Waiter(putter, None));
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

    fn register_getter(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<(bool, Option<Py<PyAny>>)> {
        let mut to_wake = None;
        let res = {
            let mut guard = self.state.lock();
            if let Some(item) = guard.buffer.pop_front() {
                if guard.maxsize > 0 {
                    if let Some(putter) = guard.putters.pop_front() {
                        to_wake = Some(WakeTarget::Waiter(putter, None));
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

    fn unregister_getter(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> PyResult<bool> {
        let mut to_wake = None;
        let removed = {
            let mut guard = self.state.lock();
            let before = guard.getters.len();
            guard.getters.retain(|g| !g.future.bind(py).is(fut));
            let removed = guard.getters.len() != before;
            if !removed && !guard.buffer.is_empty() {
                if let Some(next_getter) = guard.getters.pop_front() {
                    to_wake = Some(WakeTarget::Waiter(next_getter, None));
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

    fn register_putter(
        &self,
        _py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<bool> {
        let mut guard = self.state.lock();
        if guard.is_closed {
            return Err(PyRuntimeError::new_err("Channel is closed"));
        }
        if guard.maxsize == 0 || guard.buffer.len() < guard.maxsize {
            return Ok(true);
        }
        guard.putters.push_back(ChannelWaiter {
            event_loop: loop_obj,
            future: fut,
            channel_token: None,
        });
        Ok(false)
    }

    fn unregister_putter(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> PyResult<bool> {
        let mut to_wake = None;
        let removed = {
            let mut guard = self.state.lock();
            let before = guard.putters.len();
            guard.putters.retain(|p| !p.future.bind(py).is(fut));
            let removed = guard.putters.len() != before;
            if !removed && (guard.maxsize == 0 || guard.buffer.len() < guard.maxsize) {
                if let Some(next_putter) = guard.putters.pop_front() {
                    to_wake = Some(WakeTarget::Waiter(next_putter, None));
                }
            }
            removed
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        Ok(removed)
    }

    fn register_select_watcher(
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

    fn unregister_select_watcher(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> bool {
        let mut guard = self.state.lock();
        let before = guard.select_watchers.len();
        guard.select_watchers.retain(|w| !w.future.bind(py).is(fut));
        guard.select_watchers.len() != before
    }

    fn forward_select_wakeup(&self, py: Python<'_>) {
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

    fn getters_len(&self) -> usize {
        self.state.lock().getters.len()
    }

    fn putters_len(&self) -> usize {
        self.state.lock().putters.len()
    }

    fn notifiers_len(&self) -> usize {
        self.state.lock().select_watchers.len()
    }
}

/// A registered waiter: (event loop, future) pair used to wake a suspended
/// `wait()` coroutine from whichever thread calls `done()`.
type Waiter = (Py<PyAny>, Py<PyAny>);

struct WaitGroupInner {
    counter: usize,
    generation: u64,
    waiters: Vec<Waiter>,
}

#[pyclass(module = "gsyncio._gsyncio_core")]
pub struct RawAsyncWaitGroup {
    state: Arc<Mutex<WaitGroupInner>>,
}

#[pymethods]
impl RawAsyncWaitGroup {
    #[new]
    fn new() -> Self {
        RawAsyncWaitGroup {
            state: Arc::new(Mutex::new(WaitGroupInner {
                counter: 0,
                generation: 0,
                waiters: Vec::new(),
            })),
        }
    }

    /// Add `delta` to the counter (Go `sync.WaitGroup.Add` semantics).
    ///
    /// Negative deltas are legal, but the counter must never go below zero:
    /// an `add()` that would underflow raises `RuntimeError`.
    /// Returns `Some(waiters)` when counter reaches 0 to wake registered waiters.
    fn add(&self, delta: isize) -> PyResult<Option<Vec<Waiter>>> {
        let mut guard = self.state.lock();
        if delta < 0 {
            let magnitude = delta.unsigned_abs();
            if guard.counter < magnitude {
                return Err(PyRuntimeError::new_err(
                    "WaitGroup counter went negative: add() with negative delta",
                ));
            }
            guard.counter -= magnitude;
            if guard.counter == 0 {
                guard.generation = guard.generation.wrapping_add(1);
                let waiters = std::mem::take(&mut guard.waiters);
                return Ok(Some(waiters));
            }
        } else {
            match guard.counter.checked_add(delta as usize) {
                Some(v) => guard.counter = v,
                None => {
                    return Err(PyRuntimeError::new_err(
                        "WaitGroup counter overflowed: add() with positive delta",
                    ));
                }
            }
        }
        Ok(None)
    }

    fn done(&self) -> PyResult<Option<Vec<Waiter>>> {
        let mut guard = self.state.lock();
        if guard.counter == 0 {
            return Err(PyRuntimeError::new_err(
                "WaitGroup counter went negative: too many done() calls",
            ));
        }
        guard.counter -= 1;
        if guard.counter == 0 {
            guard.generation = guard.generation.wrapping_add(1);
            let waiters = std::mem::take(&mut guard.waiters);
            Ok(Some(waiters))
        } else {
            Ok(None)
        }
    }

    /// Registers a waiter that will be notified when the counter reaches zero.
    ///
    /// Returns `true` if the counter is already zero (waiter is not registered
    /// and the caller should proceed immediately). Returns `false` if the waiter
    /// was registered and will be notified by a future `done()` call.
    fn register_waiter(&self, waiter: Waiter) -> bool {
        let mut guard = self.state.lock();
        if guard.counter == 0 {
            true
        } else {
            guard.waiters.push(waiter);
            false
        }
    }

    /// Removes a previously registered waiter by future identity.
    ///
    /// Returns `true` if the waiter was still queued and is now removed,
    /// `false` if it had already been handed over by a `done()`-to-zero.
    fn unregister_waiter(&self, fut: Py<PyAny>) -> bool {
        let mut guard = self.state.lock();
        let before = guard.waiters.len();
        guard.waiters.retain(|(_, f)| !f.is(&fut));
        guard.waiters.len() != before
    }
}

#[pymodule]
fn _gsyncio_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "ThreadPoolClosedError",
        m.py().get_type::<ThreadPoolClosedError>(),
    )?;
    m.add_class::<AtomicMetrics>()?;
    m.add_class::<NativeWorkerPool>()?;
    m.add_class::<FastChannel>()?;
    m.add_class::<RawAsyncChannel>()?;
    m.add_class::<RawAsyncWaitGroup>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::{Py, PyAny, Python};

    #[test]
    fn test_waitgroup_done_underflow() {
        Python::attach(|_py| {
            let wg = RawAsyncWaitGroup::new();
            // Counter starts at 0, done() should return error (not wrap around).
            let result = wg.done();
            assert!(result.is_err(), "done() on counter=0 should return Err");
        });
    }

    #[test]
    fn test_waitgroup_double_check() {
        Python::attach(|py| {
            let wg = RawAsyncWaitGroup::new();
            wg.add(1).unwrap();
            // done() decrements from 1 → 0, returns the waiters list.
            let result = wg.done();
            assert!(result.is_ok(), "done() on counter=1 should succeed");
            let waiters = result.unwrap();
            assert!(
                waiters.is_some(),
                "waiters should be Some when counter reaches 0"
            );
            let waiters = waiters.unwrap();
            assert!(waiters.is_empty(), "waiters list should be empty");
            // After done(), counter is 0, so register_waiter should return true
            // (meaning "already done, wake immediately").
            let waiter: Waiter = (py.None(), py.None());
            assert!(
                wg.register_waiter(waiter),
                "register_waiter after done should return true"
            );
        });
    }

    #[test]
    fn test_waitgroup_add_negative() {
        Python::attach(|_py| {
            let wg = RawAsyncWaitGroup::new();
            // add(-1) on a zero counter must error (Go panics on a negative
            // counter; we mirror that with RuntimeError).
            assert!(
                wg.add(-1).is_err(),
                "add(-1) on counter=0 should return Err"
            );
            wg.add(2).unwrap();
            // add(-3) would drive 2 → -1: must error.
            assert!(
                wg.add(-3).is_err(),
                "add(-3) on counter=2 should return Err"
            );
            // add(-2) brings 2 → 0: legal, mirrors done().
            wg.add(-2).unwrap();
            // Counter is 0 again: a positive add is legal.
            wg.add(1).unwrap();
        });
    }

    #[test]
    fn test_waitgroup_unregister_waiter() {
        Python::attach(|py| {
            let wg = RawAsyncWaitGroup::new();
            wg.add(1).unwrap();
            let fut = py.None();
            let waiter: Waiter = (py.None(), fut.clone_ref(py));
            assert!(
                !wg.register_waiter(waiter),
                "register on counter=1 should queue"
            );
            assert!(
                wg.unregister_waiter(fut.clone_ref(py)),
                "first unregister should remove the entry"
            );
            assert!(
                !wg.unregister_waiter(fut.clone_ref(py)),
                "second unregister is a no-op"
            );
            // done() to zero hands over an EMPTY list — the cancelled
            // waiter's entry is gone.
            let waiters = wg.done().unwrap();
            assert!(waiters.is_some());
            assert!(
                waiters.unwrap().is_empty(),
                "no stale entries after unregister"
            );
        });
    }

    #[test]
    fn test_fastchannel_try_send_recv() {
        Python::attach(|py| {
            let ch = FastChannel::new(0); // unbounded channel
            let item: Py<PyAny> = py.None();
            let send_result = ch.try_send(item);
            assert!(send_result.is_ok(), "try_send should succeed");
            assert!(send_result.unwrap(), "try_send should return Ok(true)");
            let recv_result = ch.try_recv(py);
            assert!(recv_result.is_ok(), "try_recv should succeed");
            let recv_val = recv_result.unwrap();
            assert!(recv_val.0, "try_recv should report has_item=true");
            assert!(recv_val.1.is_some(), "try_recv should return Some(item)");
        });
    }

    #[test]
    fn test_fastchannel_close_then_recv() {
        Python::attach(|py| {
            let ch = FastChannel::new(0); // unbounded channel
            ch.close();
            // After close, try_recv on an empty channel returns Err.
            let result = ch.try_recv(py);
            assert!(
                result.is_err(),
                "try_recv on closed empty channel should return Err"
            );
        });
    }

    #[test]
    fn test_atomicmetrics_inc_dec() {
        let metrics = AtomicMetrics::new(4);
        metrics.inc_active(0);
        metrics.inc_active(0);
        metrics.inc_active(0);
        metrics.dec_active(0);
        metrics.dec_active(0);
        assert_eq!(metrics.get_active(0), 1, "active should be 3 - 2 = 1");
        assert_eq!(metrics.get_completed(0), 2, "completed should be 2");
    }

    #[test]
    fn test_atomicmetrics_no_crash() {
        let metrics = AtomicMetrics::new(4);
        // dec without prior inc: must not panic.
        // AtomicUsize wraps around on underflow (well-defined behavior).
        metrics.dec_active(0);
        // If we got here without panicking, the test passes.
    }

    #[test]
    fn test_waitgroup_add_negative_returns_waiters() {
        Python::attach(|py| {
            let wg = RawAsyncWaitGroup::new();
            wg.add(2).unwrap();
            let fut = py.None();
            let waiter: Waiter = (py.None(), fut.clone_ref(py));
            assert!(!wg.register_waiter(waiter));
            let res = wg.add(-2).unwrap();
            assert!(res.is_some());
            let waiters = res.unwrap();
            assert_eq!(waiters.len(), 1);
        });
    }

    #[test]
    fn test_nativeworkerpool_shutdown_drain_is_drained() {
        Python::attach(|py| {
            let pool = NativeWorkerPool::new(2);
            let task1: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
            let task2: Py<PyAny> = pyo3::types::PyInt::new(py, 2).into_any().unbind();
            pool.push_global(py, task1).unwrap();
            pool.push_local(0, task2, py).unwrap();

            assert!(!pool.is_drained());
            pool.close();
            assert!(pool.is_closed());
            assert!(!pool.is_drained());

            // Pop first task (from global queue batch pull)
            let popped1 = pool.pop_work(0, py).unwrap();
            assert!(popped1.is_some());

            // Pop second task (from worker 0's local queue)
            let popped2 = pool.pop_work(0, py).unwrap();
            assert!(popped2.is_some());

            assert!(pool.is_drained());
            // Once drained, pop_work must return ThreadPoolClosedError
            let result = pool.pop_work(0, py);
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_fastchannel_bounded_full_and_fifo() {
        Python::attach(|py| {
            let ch = FastChannel::new(2);
            assert_eq!(ch.qsize(), 0);
            let item1: Py<PyAny> = pyo3::types::PyInt::new(py, 10).into_any().unbind();
            let item2: Py<PyAny> = pyo3::types::PyInt::new(py, 20).into_any().unbind();
            let item3: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();

            assert_eq!(ch.try_send(item1).unwrap(), true);
            assert_eq!(ch.try_send(item2).unwrap(), true);
            assert_eq!(ch.qsize(), 2);
            // Channel is full: try_send returns Ok(false)
            assert_eq!(ch.try_send(item3).unwrap(), false);

            // Receive item 1 (FIFO order: 10)
            let (has1, val1) = ch.try_recv(py).unwrap();
            assert!(has1);
            let num1: i32 = val1.unwrap().extract(py).unwrap();
            assert_eq!(num1, 10);

            // Now space is available: send item 3
            let item3_new: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();
            assert_eq!(ch.try_send(item3_new).unwrap(), true);

            // Receive item 2 (20)
            let (has2, val2) = ch.try_recv(py).unwrap();
            assert!(has2);
            let num2: i32 = val2.unwrap().extract(py).unwrap();
            assert_eq!(num2, 20);

            // Receive item 3 (30)
            let (has3, val3) = ch.try_recv(py).unwrap();
            assert!(has3);
            let num3: i32 = val3.unwrap().extract(py).unwrap();
            assert_eq!(num3, 30);

            // Channel now empty
            let (has4, _) = ch.try_recv(py).unwrap();
            assert!(!has4);
        });
    }

    #[test]
    fn test_fastchannel_drain_after_close() {
        Python::attach(|py| {
            let ch = FastChannel::new(0);
            let item1: Py<PyAny> = pyo3::types::PyString::new(py, "first").into_any().unbind();
            let item2: Py<PyAny> = pyo3::types::PyString::new(py, "second").into_any().unbind();
            ch.try_send(item1).unwrap();
            ch.try_send(item2).unwrap();

            ch.close();
            assert!(ch.is_closed());

            // After close, items remaining in the channel must drain cleanly
            let (has1, val1) = ch.try_recv(py).unwrap();
            assert!(has1);
            let s1: String = val1.unwrap().extract(py).unwrap();
            assert_eq!(s1, "first");

            let (has2, val2) = ch.try_recv(py).unwrap();
            assert!(has2);
            let s2: String = val2.unwrap().extract(py).unwrap();
            assert_eq!(s2, "second");

            // Once fully drained, try_recv on closed channel must return Err
            assert!(ch.try_recv(py).is_err());
        });
    }

    #[test]
    fn test_nativeworkerpool_local_to_global_fallback() {
        Python::attach(|py| {
            let pool = NativeWorkerPool::new(1);
            // Fill worker 0 local channel to capacity (256)
            for i in 0..256 {
                let task: Py<PyAny> = pyo3::types::PyInt::new(py, i).into_any().unbind();
                pool.push_local(0, task, py).unwrap();
            }
            // 257th push to local should fall back to global queue without error
            let task257: Py<PyAny> = pyo3::types::PyInt::new(py, 256).into_any().unbind();
            pool.push_local(0, task257, py).unwrap();

            // Verify all 257 tasks can be popped
            let mut popped_count = 0;
            for _ in 0..257 {
                let popped = pool.pop_work(0, py).unwrap();
                if popped.is_some() {
                    popped_count += 1;
                }
            }
            assert_eq!(popped_count, 257);
        });
    }

    #[test]
    fn test_atomicmetrics_concurrent_multiworker() {
        use std::sync::Arc;
        use std::thread;

        let num_workers = 4;
        let metrics = Arc::new(AtomicMetrics::new(num_workers));
        let iterations = 10_000;

        let mut handles = Vec::new();
        for worker_id in 0..num_workers {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..iterations {
                    m.inc_active(worker_id);
                    m.inc_global_pull(worker_id);
                    m.dec_active(worker_id);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for worker_id in 0..num_workers {
            assert_eq!(metrics.get_active(worker_id), 0);
            assert_eq!(metrics.get_completed(worker_id), iterations);
            assert_eq!(metrics.get_global_pull(worker_id), iterations);
        }
    }

    #[test]
    fn test_raw_async_channel_send_recv_basic() {
        Python::attach(|py| {
            let ch = RawAsyncChannel::new(0, None, None);
            assert_eq!(ch.qsize(), 0);
            assert!(ch.empty());
            assert!(!ch.full());
            assert!(!ch.is_closed());

            let item1: Py<PyAny> = pyo3::types::PyString::new(py, "hello").into_any().unbind();
            assert!(ch.try_send(py, item1).unwrap());
            assert_eq!(ch.qsize(), 1);
            assert!(!ch.empty());

            let (has_item, item_opt) = ch.try_recv(py).unwrap();
            assert!(has_item);
            let s: String = item_opt.unwrap().extract(py).unwrap();
            assert_eq!(s, "hello");
            assert_eq!(ch.qsize(), 0);
            assert!(ch.empty());

            // Empty channel recv returns (false, None)
            let (has2, val2) = ch.try_recv(py).unwrap();
            assert!(!has2);
            assert!(val2.is_none());
        });
    }

    #[test]
    fn test_raw_async_channel_bounded_full_and_drain() {
        Python::attach(|py| {
            let ch = RawAsyncChannel::new(2, None, None);
            assert_eq!(ch.maxsize(), 2);
            let item1: Py<PyAny> = pyo3::types::PyInt::new(py, 10).into_any().unbind();
            let item2: Py<PyAny> = pyo3::types::PyInt::new(py, 20).into_any().unbind();
            let item3: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();

            assert!(ch.try_send(py, item1).unwrap());
            assert!(ch.try_send(py, item2).unwrap());
            assert!(ch.full());

            // Bounded full: try_send returns Ok(false)
            assert!(!ch.try_send(py, item3).unwrap());

            // Drain first item
            let (has1, val1) = ch.try_recv(py).unwrap();
            assert!(has1);
            assert_eq!(val1.unwrap().extract::<i32>(py).unwrap(), 10);
            assert!(!ch.full());

            // Now space available: send third item
            let item3_new: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();
            assert!(ch.try_send(py, item3_new).unwrap());

            // Drain remaining
            let (_, val2) = ch.try_recv(py).unwrap();
            assert_eq!(val2.unwrap().extract::<i32>(py).unwrap(), 20);
            let (_, val3) = ch.try_recv(py).unwrap();
            assert_eq!(val3.unwrap().extract::<i32>(py).unwrap(), 30);
            assert!(ch.empty());
        });
    }

    #[test]
    fn test_raw_async_channel_register_unregister_getter() {
        Python::attach(|py| {
            let ch = RawAsyncChannel::new(0, None, None);
            let loop_obj = py.None();
            let fut = py.None();

            // Register getter on empty channel
            let (has_item, item) = ch
                .register_getter(py, loop_obj.clone_ref(py), fut.clone_ref(py))
                .unwrap();
            assert!(!has_item);
            assert!(item.is_none());
            assert_eq!(ch.state.lock().getters.len(), 1);

            // Unregister getter
            let removed = ch.unregister_getter(py, fut.bind(py)).unwrap();
            assert!(removed);
            assert_eq!(ch.state.lock().getters.len(), 0);

            // Double-check fast path: if item is already present, register_getter returns it immediately
            let item_val: Py<PyAny> = pyo3::types::PyString::new(py, "fast").into_any().unbind();
            ch.try_send(py, item_val).unwrap();

            let (has2, val2) = ch
                .register_getter(py, loop_obj.clone_ref(py), fut.clone_ref(py))
                .unwrap();
            assert!(has2);
            assert_eq!(val2.unwrap().extract::<String>(py).unwrap(), "fast");
            assert_eq!(ch.state.lock().getters.len(), 0);
        });
    }

    #[test]
    fn test_raw_async_channel_register_unregister_putter() {
        Python::attach(|py| {
            let ch = RawAsyncChannel::new(1, None, None);
            let item: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
            ch.try_send(py, item).unwrap();
            assert!(ch.full());

            let loop_obj = py.None();
            let fut = py.None();

            // Register putter on full channel
            let can_send = ch
                .register_putter(py, loop_obj.clone_ref(py), fut.clone_ref(py))
                .unwrap();
            assert!(!can_send);
            assert_eq!(ch.state.lock().putters.len(), 1);

            // Unregister putter
            let removed = ch.unregister_putter(py, fut.bind(py)).unwrap();
            assert!(removed);
            assert_eq!(ch.state.lock().putters.len(), 0);

            // Drain item so space is available
            let (has, _) = ch.try_recv(py).unwrap();
            assert!(has);

            // Double-check: when space available, register_putter returns true immediately
            let can_send_now = ch.register_putter(py, loop_obj, fut).unwrap();
            assert!(can_send_now);
        });
    }

    #[test]
    fn test_raw_async_channel_select_watcher_and_forwarding() {
        Python::attach(|py| {
            let ch = RawAsyncChannel::new(0, None, None);
            let loop_obj = py.None();
            let fut = py.None();
            let token = py.None();

            // Register watcher on empty channel
            let registered = ch
                .register_select_watcher(
                    py,
                    loop_obj.clone_ref(py),
                    fut.clone_ref(py),
                    token.clone_ref(py),
                )
                .unwrap();
            assert!(registered);
            assert_eq!(ch.state.lock().select_watchers.len(), 1);

            // Unregister watcher
            let removed = ch.unregister_select_watcher(py, fut.bind(py));
            assert!(removed);
            assert_eq!(ch.state.lock().select_watchers.len(), 0);

            // If channel has item, register_select_watcher returns false (caller probes immediately)
            let item: Py<PyAny> = pyo3::types::PyInt::new(py, 99).into_any().unbind();
            ch.try_send(py, item).unwrap();
            let reg2 = ch
                .register_select_watcher(py, loop_obj, fut, token)
                .unwrap();
            assert!(!reg2);
        });
    }

    #[test]
    fn test_raw_async_channel_close_wakes_all() {
        Python::attach(|py| {
            let ch = RawAsyncChannel::new(1, None, None);
            let loop_obj = py.None();
            let fut1 = py.None();
            let fut2 = py.None();

            // Fill channel
            let item: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
            ch.try_send(py, item).unwrap();

            // Queue a putter
            ch.register_putter(py, loop_obj.clone_ref(py), fut1)
                .unwrap();
            assert_eq!(ch.state.lock().putters.len(), 1);

            // Queue a select watcher
            ch.register_select_watcher(py, loop_obj, fut2, py.None())
                .unwrap();

            ch.close(py);
            assert!(ch.is_closed());
            assert_eq!(ch.state.lock().putters.len(), 0);
            assert_eq!(ch.state.lock().select_watchers.len(), 0);

            // Send on closed channel raises error
            let item2: Py<PyAny> = pyo3::types::PyInt::new(py, 2).into_any().unbind();
            assert!(ch.try_send(py, item2).is_err());

            // Drain buffered item
            let (has, val) = ch.try_recv(py).unwrap();
            assert!(has);
            assert_eq!(val.unwrap().extract::<i32>(py).unwrap(), 1);

            // After drain, recv on closed channel raises error
            assert!(ch.try_recv(py).is_err());
        });
    }

    #[test]
    fn test_raw_async_channel_concurrent_threads() {
        use std::sync::Arc;
        use std::thread;

        let ch = Arc::new(RawAsyncChannel::new(10, None, None));
        let total_items = 200;
        let num_producers = 4;
        let num_consumers = 4;
        let items_per_producer = total_items / num_producers;
        let items_per_consumer = total_items / num_consumers;

        let received = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        // Spawn producers
        for p in 0..num_producers {
            let channel = Arc::clone(&ch);
            handles.push(thread::spawn(move || {
                for i in 0..items_per_producer {
                    let val = (p * items_per_producer + i) as i64;
                    loop {
                        let sent = Python::attach(|py| {
                            let py_val: Py<PyAny> =
                                pyo3::types::PyInt::new(py, val).into_any().unbind();
                            channel.try_send(py, py_val).unwrap()
                        });
                        if sent {
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }));
        }

        // Spawn consumers concurrently
        for _ in 0..num_consumers {
            let channel = Arc::clone(&ch);
            let rec = Arc::clone(&received);
            handles.push(thread::spawn(move || {
                for _ in 0..items_per_consumer {
                    loop {
                        let rec_opt = Python::attach(|py| {
                            let (has, val) = channel.try_recv(py).unwrap();
                            if has {
                                let num: i64 = val.unwrap().extract(py).unwrap();
                                Some(num)
                            } else {
                                None
                            }
                        });
                        if let Some(num) = rec_opt {
                            rec.lock().push(num);
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(ch.qsize(), 0);
        let mut results = received.lock().clone();
        assert_eq!(results.len(), total_items);
        results.sort();
        let expected: Vec<i64> = (0..total_items as i64).collect();
        assert_eq!(results, expected);
    }
}
