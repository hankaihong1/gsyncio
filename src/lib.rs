use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
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

#[pyclass]
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

    fn get_metrics(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let n = self.active.len();
        let active: Vec<usize> = (0..n).map(|i| self.get_active(i)).collect();
        let completed: Vec<usize> = (0..n).map(|i| self.get_completed(i)).collect();
        let global_pull: Vec<usize> = (0..n).map(|i| self.get_global_pull(i)).collect();
        let park: Vec<usize> = (0..n).map(|i| self.get_park(i)).collect();
        let injection_depth: Vec<usize> =
            (0..n).map(|i| self.get_injection_queue_depth(i)).collect();
        let remote_schedule: Vec<usize> = (0..n).map(|i| self.get_remote_schedule(i)).collect();

        let dict = PyDict::new(py);
        dict.set_item("active_tasks", PyList::new(py, active)?)?;
        dict.set_item("completed_tasks", PyList::new(py, completed)?)?;
        dict.set_item("global_pull_count", PyList::new(py, global_pull)?)?;
        dict.set_item("park_count", PyList::new(py, park)?)?;
        dict.set_item("injection_queue_depth", PyList::new(py, injection_depth)?)?;
        dict.set_item("remote_schedule_count", PyList::new(py, remote_schedule)?)?;
        Ok(dict.unbind().into())
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
#[pyclass]
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
            let num_workers = self.local_receivers.len();
            let max_pollers = std::cmp::max(1, num_workers / 2);
            let is_poller = self.num_polling.fetch_add(1, Ordering::Relaxed) < max_pollers;
            let _guard = PollerGuard(&self.num_polling);
            if is_poller {
                let global_len = self.global_receiver.len();
                if num_workers > 0 && global_len > 0 {
                    let batch_size = std::cmp::min(global_len / num_workers + 1, 128);
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

        // 3. Nothing available.
        if self.is_closed() {
            Err(ThreadPoolClosedError::new_err("Pool is closed"))
        } else {
            // Worker idle — increment park count.
            if let Some(ref metrics) = *self.metrics.lock() {
                metrics.borrow(py).inc_park(index);
                let depth = self.global_receiver.len();
                sample_depth(metrics, index, depth);
            }
            Ok(None)
        }
    }
}

#[pyclass]
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

#[pyclass]
#[allow(clippy::type_complexity)]
pub struct RawAsyncWaitGroup {
    counter: Arc<AtomicUsize>,
    waiters: Arc<Mutex<Vec<(Py<PyAny>, Py<PyAny>)>>>,
}

#[pymethods]
impl RawAsyncWaitGroup {
    #[new]
    fn new() -> Self {
        RawAsyncWaitGroup {
            counter: Arc::new(AtomicUsize::new(0)),
            waiters: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn add(&self, delta: usize) {
        self.counter.fetch_add(delta, Ordering::Release); // add(): Release makes increment visible to done's AcqRel
    }

    #[allow(clippy::type_complexity)]
    fn done(&self) -> PyResult<Option<Vec<(Py<PyAny>, Py<PyAny>)>>> {
        let prev = self.counter.fetch_sub(1, Ordering::AcqRel); // done(): AcqRel — Acquire observes add()s, Release so register_waiter sees decrement
        if prev == 0 {
            return Err(PyRuntimeError::new_err(
                "WaitGroup counter went negative: too many done() calls",
            ));
        }
        if prev == 1 {
            let mut guard = self.waiters.lock();
            let res = std::mem::take(&mut *guard);
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    /// Registers a waiter that will be notified when the counter reaches zero.
    ///
    /// Returns `true` if the counter is already zero (waiter is not registered
    /// and the caller should proceed immediately). Returns `false` if the waiter
    /// was registered and will be notified by a future `done()` call.
    ///
    /// # Ordering constraint (matches Go `sync.WaitGroup.Wait`)
    ///
    /// The unlocked fast-path in this method (checking `counter == 0` without
    /// holding the waiters lock) can lose against a concurrent `add()`:
    /// if `add()` increments from 0→1 *after* the counter check but *before*
    /// the waiter is pushed, the waiter will never be woken. Callers must
    /// ensure all `add()` calls with a positive delta happen-before
    /// `register_waiter()`.
    fn register_waiter(&self, waiter: (Py<PyAny>, Py<PyAny>)) -> bool {
        if self.counter.load(Ordering::Acquire) == 0 {
            // register_waiter: Acquire sees done()'s decrement
            true
        } else {
            let mut guard = self.waiters.lock();
            if self.counter.load(Ordering::Relaxed) == 0 {
                // Inside Mutex lock — ordering from lock acquire
                true
            } else {
                guard.push(waiter);
                false
            }
        }
    }
}

#[pymodule]
fn _gsyncio_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ThreadPoolClosedError", m.py().get_type::<ThreadPoolClosedError>())?;
    m.add_class::<AtomicMetrics>()?;
    m.add_class::<NativeWorkerPool>()?;
    m.add_class::<FastChannel>()?;
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
            wg.add(1);
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
            let waiter: (Py<PyAny>, Py<PyAny>) = (Py::from(py.None()), Py::from(py.None()));
            assert!(
                wg.register_waiter(waiter),
                "register_waiter after done should return true"
            );
        });
    }

    #[test]
    fn test_fastchannel_try_send_recv() {
        Python::attach(|py| {
            let ch = FastChannel::new(0); // unbounded channel
            let item: Py<PyAny> = Py::from(py.None());
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
}
