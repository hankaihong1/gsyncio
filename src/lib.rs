pub mod channel;
pub mod http;
pub mod metrics;
pub mod pool;
pub mod rwlock;
pub mod waitgroup;
pub mod websocket;

use pyo3::create_exception;
use pyo3::prelude::*;

pub use channel::{Channel, RawAsyncChannel};
pub use http::{FastHttpConnection, FastHttpParser};
pub use metrics::AtomicMetrics;
pub use pool::NativeWorkerPool;
pub use rwlock::RawAsyncRWMutex;
pub use waitgroup::RawAsyncWaitGroup;
pub use websocket::{fast_websocket_unmask, fast_websocket_unmask_slice, FastWebSocketParser};

create_exception!(
    _multiloop_core,
    ThreadPoolClosedError,
    pyo3::exceptions::PyException
);

#[pymodule]
fn _multiloop_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "ThreadPoolClosedError",
        m.py().get_type::<ThreadPoolClosedError>(),
    )?;
    m.add_class::<AtomicMetrics>()?;
    m.add_class::<NativeWorkerPool>()?;
    m.add_class::<Channel>()?;
    m.add_class::<RawAsyncChannel>()?;
    m.add_class::<RawAsyncWaitGroup>()?;
    m.add_class::<RawAsyncRWMutex>()?;
    m.add_class::<FastHttpParser>()?;
    m.add_class::<FastHttpConnection>()?;
    m.add_class::<FastWebSocketParser>()?;
    m.add_function(wrap_pyfunction!(fast_websocket_unmask, m)?)?;
    m.add_function(wrap_pyfunction!(fast_websocket_unmask_slice, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use pyo3::types::PyBytes;
    use pyo3::{Py, PyAny, Python};
    use waitgroup::Waiter;

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
    fn test_channel_try_send_recv() {
        Python::attach(|py| {
            let ch = Channel::new(0); // unbounded channel
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
    fn test_channel_close_then_recv() {
        Python::attach(|py| {
            let ch = Channel::new(0); // unbounded channel
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
    fn test_channel_bounded_full_and_fifo() {
        Python::attach(|py| {
            let ch = Channel::new(2);
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
    fn test_channel_drain_after_close() {
        Python::attach(|py| {
            let ch = Channel::new(0);
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
            assert_eq!(ch.getters_len(), 1);

            // Unregister getter
            let removed = ch.unregister_getter(py, fut.bind(py)).unwrap();
            assert!(removed);
            assert_eq!(ch.getters_len(), 0);

            // Double-check fast path: if item is already present, register_getter returns it immediately
            let item_val: Py<PyAny> = pyo3::types::PyString::new(py, "fast").into_any().unbind();
            ch.try_send(py, item_val).unwrap();

            let (has2, val2) = ch
                .register_getter(py, loop_obj.clone_ref(py), fut.clone_ref(py))
                .unwrap();
            assert!(has2);
            assert_eq!(val2.unwrap().extract::<String>(py).unwrap(), "fast");
            assert_eq!(ch.getters_len(), 0);
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
            assert_eq!(ch.putters_len(), 1);

            // Unregister putter
            let removed = ch.unregister_putter(py, fut.bind(py)).unwrap();
            assert!(removed);
            assert_eq!(ch.putters_len(), 0);

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
            assert_eq!(ch.notifiers_len(), 1);

            // Unregister watcher
            let removed = ch.unregister_select_watcher(py, fut.bind(py));
            assert!(removed);
            assert_eq!(ch.notifiers_len(), 0);

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
            assert_eq!(ch.putters_len(), 1);

            // Queue a select watcher
            ch.register_select_watcher(py, loop_obj, fut2, py.None())
                .unwrap();

            ch.close(py).unwrap();
            assert!(ch.is_closed());
            assert_eq!(ch.putters_len(), 0);
            assert_eq!(ch.notifiers_len(), 0);

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

    #[test]
    fn test_fast_http_parser_complete_and_partial() {
        Python::attach(|py| {
            // Complete request
            let req_bytes = b"POST /api/v1/resource?query=test&limit=10 HTTP/1.1\r\nHost: localhost:8000\r\nContent-Length: 5\r\n\r\nhello";
            let py_req = PyBytes::new(py, req_bytes);
            let parsed = FastHttpParser::parse_request(py, &py_req).unwrap();
            assert!(parsed.is_some());
            let (
                method,
                path,
                raw_path,
                query,
                version,
                headers,
                body_offset,
                cl,
                keep_alive,
                is_chunked,
                is_upgrade,
                _upg_p,
            ) = parsed.unwrap();
            assert_eq!(method.to_str().unwrap(), "POST");
            assert_eq!(path.to_str().unwrap(), "/api/v1/resource");
            assert_eq!(raw_path.as_bytes(), b"/api/v1/resource");
            assert_eq!(query.as_bytes(), b"query=test&limit=10");
            assert_eq!(version.to_str().unwrap(), "1.1");
            assert_eq!(headers.len(), 2);
            assert_eq!(cl, 5);
            assert!(keep_alive);
            assert!(!is_chunked);
            assert!(!is_upgrade);
            assert_eq!(&req_bytes[body_offset..], b"hello");

            // Partial request
            let partial_bytes = b"GET /incomplete HTTP/1.1\r\nHost: localhost\r\n";
            let py_partial = PyBytes::new(py, partial_bytes);
            let partial_parsed = FastHttpParser::parse_request(py, &py_partial).unwrap();
            assert!(partial_parsed.is_none());

            // Malformed request
            let malformed = b"INVALID HTTP BUFFER WITHOUT PROPER FORMAT";
            let py_malformed = PyBytes::new(py, malformed);
            assert!(FastHttpParser::parse_request(py, &py_malformed).is_err());
        });
    }
}
