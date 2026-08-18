use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::metrics::AtomicMetrics;
use crate::ThreadPoolClosedError;

/// Bounded capacity of each per-worker local queue.
const _LOCAL_QUEUE_CAPACITY: usize = 256;

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
#[pyclass(module = "multiloop._multiloop_core")]
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
    pub fn new(num_threads: usize) -> Self {
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

    pub fn close(&self) {
        // Drop senders first so receivers become disconnected after draining,
        // then set the advisory flag so is_closed() returns true immediately.
        *self.global_sender.lock() = None;
        self.local_senders.lock().clear();
        self.is_closed.store(true, Ordering::Release); // Flag store: Release makes the close visible to Acquire loads
    }

    pub fn set_metrics(&self, metrics: Py<AtomicMetrics>) {
        *self.metrics.lock() = Some(metrics);
    }

    pub fn is_closed(&self) -> bool {
        if self.is_closed.load(Ordering::Acquire) {
            // Flag load: Acquire observes close store
            return true;
        }
        self.global_sender
            .lock()
            .as_ref()
            .is_none_or(|s| s.is_disconnected())
    }

    pub fn is_drained(&self) -> bool {
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

    pub fn push_global(&self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
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

    pub fn push_local(&self, index: usize, task: Py<PyAny>, py: Python<'_>) -> PyResult<()> {
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

    pub fn pop_work(&self, index: usize, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
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
