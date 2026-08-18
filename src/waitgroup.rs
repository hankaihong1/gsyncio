use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;

/// A registered waiter: (event loop, future) pair used to wake a suspended
/// `wait()` coroutine from whichever thread calls `done()`.
pub(crate) type Waiter = (Py<PyAny>, Py<PyAny>);

pub(crate) struct WaitGroupInner {
    pub(crate) counter: usize,
    pub(crate) generation: u64,
    pub(crate) waiters: Vec<Waiter>,
}

#[pyclass(module = "multiloop._multiloop_core")]
pub struct RawAsyncWaitGroup {
    pub(crate) state: Arc<Mutex<WaitGroupInner>>,
}

impl Default for RawAsyncWaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl RawAsyncWaitGroup {
    #[new]
    pub fn new() -> Self {
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
    pub fn add(&self, delta: isize) -> PyResult<Option<Vec<Waiter>>> {
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

    pub fn done(&self) -> PyResult<Option<Vec<Waiter>>> {
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
    pub fn register_waiter(&self, waiter: Waiter) -> bool {
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
    pub fn unregister_waiter(&self, fut: Py<PyAny>) -> bool {
        let mut guard = self.state.lock();
        let before = guard.waiters.len();
        guard.waiters.retain(|(_, f)| !f.is(&fut));
        guard.waiters.len() != before
    }
}
