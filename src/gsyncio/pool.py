"""Multi-event-loop thread pool implementation with Work-Stealing Shared Queue Architecture."""

from __future__ import annotations

import asyncio
import contextvars
import os
import threading
import types
from collections.abc import Callable
from typing import Any, Protocol, Self

from gsyncio._cancel import CancelScope
from gsyncio._logging import get_logger
from gsyncio._metrics import MetricsCollector
from gsyncio._options import PoolOptions
from gsyncio._rust import _try_import_rust_class
from gsyncio.exceptions import ThreadPoolClosedError

_logger = get_logger("pool")

_WORKER_POLL_INTERVAL = 0.05  # Dispatcher wait_for timeout while idle (s)
_MAX_DRAIN_ITERATIONS = 50  # Max drain polls (~5 s) before force-stopping loops
_DRAIN_GRACE_PERIOD = 0.05  # Initial grace before first drain active==0 check (s)


def _safe_complete(
    loop: asyncio.AbstractEventLoop,
    fut: asyncio.Future[Any],
    result: Any = None,
    exc: BaseException | None = None,
) -> None:
    """Complete *fut* on its owning loop, tolerating a raced completion.

    WHY: several parties can race to complete the same future — the worker
    task delivering the real result, and abort() completing it with a
    ThreadPoolClosedError.  The guard lives INSIDE the scheduled callback
    (same shape as ``_channel_base._set_soon``), so an InvalidStateError from
    a lost race is contained instead of surfacing in the loop exception
    handler (R2 FIX-13 revision B).  If the caller loop is already closed, fall
    back to completing the future directly — nobody can be consuming it, so
    there is no concurrent reader to race (TS-12 pattern).
    """

    def _do() -> None:
        try:
            if exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(result)
        except (asyncio.InvalidStateError, RuntimeError):
            # A concurrent completion (abort vs worker delivery) won the
            # race or loop was closed — nothing to deliver.
            pass

    try:
        loop.call_soon_threadsafe(_do)
    except RuntimeError:
        try:
            if exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(result)
        except (asyncio.InvalidStateError, RuntimeError):
            pass


class _WorkerPoolProtocol(Protocol):
    """Protocol for the Rust NativeWorkerPool class."""

    def __init__(self, num_threads: int) -> None: ...
    def pop_work(self, index: int) -> Any: ...
    def is_closed(self) -> bool: ...
    def is_drained(self) -> bool: ...
    def close(self) -> None: ...
    def push_global(self, task: Any) -> None: ...
    def push_local(self, index: int, task: Any) -> None: ...
    def set_metrics(self, metrics: Any) -> None: ...


NativeWorkerPool: type[_WorkerPoolProtocol] | None = _try_import_rust_class(
    "gsyncio._gsyncio_core", "NativeWorkerPool"
)
# WHY: the Rust core raises its own ThreadPoolClosedError type; we translate
# it to the public gsyncio.exceptions type by isinstance (PRA-1 — type
# discrimination, never message matching).
_RustPoolClosedError = _try_import_rust_class("gsyncio._gsyncio_core", "ThreadPoolClosedError")


class EventLoopThreadPool:
    """Adaptive Multi-Event-Loop Thread Pool with Work-Stealing Shared Queue Architecture.

    This class orchestrates multiple OS worker threads, each executing an isolated
    :class:`asyncio.AbstractEventLoop`. Unpinned tasks are pushed to a global lock-free shared queue
    where idle worker loops pull and execute them instantly. Tasks requiring event loop affinity
    (e.g., `asyncssh` connections) can be pinned to specific target loops.

    :param options:
        Optional :class:`PoolOptions` providing defaults for all pool parameters.
        Individual keyword arguments override corresponding fields in `options`.
    :type options: PoolOptions or None

    :param num_threads:
        The number of worker threads to create. Defaults to :func:`os.cpu_count` or 4.
        Set to ``0`` for auto-detect (via ``PoolOptions`` or directly).
    :type num_threads: int or None

    :param loop_factory:
        Optional callable returning a new :class:`asyncio.AbstractEventLoop`
        (e.g. a third-party loop factory — gsyncio never probes for such
        loops itself; pass one explicitly if you want it). Defaults to
        :func:`asyncio.new_event_loop`.
    :type loop_factory: callable or None

    :raises ValueError:
        If `num_threads` is negative.

    """

    def __init__(
        self,
        options: PoolOptions | None = None,
        num_threads: int | None = None,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
    ) -> None:
        if options is not None:
            if num_threads is None:
                num_threads = options.num_threads
            if loop_factory is None:
                loop_factory = options.loop_factory

        if num_threads is None:
            num_threads = os.cpu_count() or 4
        if num_threads < 0:
            raise ValueError("num_threads must not be negative")
        if num_threads == 0:
            num_threads = os.cpu_count() or 4

        self.num_threads = num_threads
        self._loop_factory = loop_factory or asyncio.new_event_loop
        self._threads: list[threading.Thread] = []
        self._loops: list[asyncio.AbstractEventLoop] = []
        self._notify_events: list[asyncio.Event] = []
        self._lock = threading.Lock()
        self._running = False
        # WHY (R10 P4): distinguishes "never started" from "started but
        # currently closing" for wait_closed(); written under _lock in
        # start(), read under _lock in wait_closed().
        self._started = False
        self._index = 0
        self._idle_workers: set[int] = set()
        # WHY: every live submit future is registered here so abort() can
        # complete the ones that never ran.  All access under _lock: the
        # caller thread registers, worker threads discard on completion.
        self._outstanding: set[asyncio.Future[Any]] = set()

        # Native Rust lock-free pool controller
        self._native_pool: _WorkerPoolProtocol | None = None
        if NativeWorkerPool is not None:
            self._native_pool = NativeWorkerPool(self.num_threads)

        self._metrics_collector = MetricsCollector(self.num_threads)
        # Wire the native pool's scheduler counters to the shared AtomicMetrics
        # instance so pop_work / push_local increment the same counters that
        # get_metrics() reads.
        if self._native_pool is not None and self._metrics_collector._metrics is not None:
            self._native_pool.set_metrics(self._metrics_collector._metrics)
        # WHY: a threading.Event — asyncio.Event.set() from a foreign loop is
        # a data race on free-threaded builds (W19); wait_closed() polls it
        # via asyncio.to_thread.
        self._closed_event = threading.Event()

    def __repr__(self) -> str:
        return (
            f"<EventLoopThreadPool running={self.is_running} "
            f"threads={self.num_threads} metrics={self.get_metrics()}>"
        )

    @property
    def is_running(self) -> bool:
        """Return whether the thread pool is running and ready for task submission.

        :returns: ``True`` if active, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        with self._lock:
            return self._running

    def _get_loop(self, index: int) -> asyncio.AbstractEventLoop:
        """Return the event loop at the given worker index.

        :param index: Worker index (0-based).
        :returns: The :class:`asyncio.AbstractEventLoop` for that worker.
        :raises RuntimeError: If the pool is not running (TS-1: the shared
            ``_loops`` list is cleared by close(), so reads must be locked —
            a bare read raced with close() on free-threaded builds).
        """
        with self._lock:
            if not self._running or index >= len(self._loops):
                raise RuntimeError("pool is not running")
            return self._loops[index]

    def get_metrics(self) -> dict[str, Any]:
        """Return JSON-serializable health & performance metrics of the pool.

        :returns: A dictionary containing pool status (`is_running`), thread count (`thread_count`),
                  completed task counters (`completed_tasks`), and active task counters (`active_tasks`).
        :rtype: :class:`dict`

        """
        with self._lock:
            return self._metrics_collector.get_snapshot(self._running)

    async def _run_task_wrapper(self, worker_idx: int, task_func: Callable[[], Any]) -> None:
        """Execute a task pulled from queue while updating active task metrics."""
        self._metrics_collector.inc_active(worker_idx)
        try:
            await task_func()
        finally:
            self._metrics_collector.dec_active(worker_idx)

    async def _worker_dispatcher(self, index: int, notify_event: asyncio.Event) -> None:
        """Worker dispatch loop: uses Instant Pipe Event Wakeup instead of sleep polling."""
        loop = asyncio.get_running_loop()

        def _process_one(task_func: Callable[[], Any]) -> None:
            # WHY: submit() stamps the task function with the caller's
            # contextvars; run the worker task under that context so the
            # caller's task-local state (spans, request ids, …) survives
            # the thread hop (R4 FIX-24).  getattr keeps the pre-existing
            # behaviour (loop's own context) when the attribute is absent.
            ctx = getattr(task_func, "_gsyncio_ctx", None)
            loop.create_task(self._run_task_wrapper(index, task_func), context=ctx)

        while True:
            if self._native_pool is None:
                break

            processed_any = False
            # WHY: Phase order is load-bearing. Phase 1 must drain unconditionally
            # so close() flushes queued work before Phase 2 reports closed; Phase 3
            # yields to avoid busy-waiting while stopping; Phase 4 sleeps only when
            # truly idle. Reordering any phase can orphan work or spin the CPU.
            # Phase 1 — Drain: pull every pending task from the Rust lock-free
            # queue.  This runs unconditionally, even after _running=False,
            # so that close() → pop_work() drains buffered items before
            # signalling "Pool is closed" via Disconnected.
            while True:
                try:
                    task_func = self._native_pool.pop_work(index)
                    if task_func is not None:
                        _process_one(task_func)
                        processed_any = True
                        # Yield to event loop to allow the popped task to start.
                        # This prevents a single fast worker from greedily draining
                        # the entire lock-free queue before other workers wake up.
                        await asyncio.sleep(0)
                    else:
                        break
                except Exception as exc:  # noqa: BLE001
                    if (
                        _RustPoolClosedError is not None and isinstance(exc, _RustPoolClosedError)
                    ) or (isinstance(exc, ThreadPoolClosedError)):
                        return
                    if self._native_pool.is_closed():
                        return
                    break

            # Phase 2 — Shutdown gate: if the pool is stopping/closed and pop_work returned None,
            # yield briefly to allow active tasks or concurrent drain to make progress.
            if not self.is_running or self._native_pool.is_closed():
                await asyncio.sleep(0.01)
                continue

            # Phase 4 — Normal idle path: only sleep when nothing was processed
            # and the pool is still healthy.
            if not processed_any:
                notify_event.clear()
                # Double-check queue right after clearing to prevent race condition
                try:
                    task_func = self._native_pool.pop_work(index)
                    if task_func is not None:
                        _process_one(task_func)
                        continue
                except Exception:  # noqa: BLE001, S110
                    pass

                with self._lock:
                    self._idle_workers.add(index)
                try:
                    await asyncio.wait_for(notify_event.wait(), timeout=_WORKER_POLL_INTERVAL)
                except TimeoutError:
                    pass
                except asyncio.CancelledError:
                    break
                finally:
                    with self._lock:
                        self._idle_workers.discard(index)

    def _worker(self, index: int, loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        notify_event = asyncio.Event()
        with self._lock:
            if index < len(self._notify_events):
                self._notify_events[index] = notify_event

        dispatcher_task = loop.create_task(self._worker_dispatcher(index, notify_event))
        try:
            loop.run_forever()
        except BaseException:  # noqa: BLE001, S110
            pass
        finally:
            if not dispatcher_task.done():
                dispatcher_task.cancel()
            pending = asyncio.all_tasks(loop)
            # WHY: a task created by the drain but never stepped (loop.stop
            # raced create_task) would be cancelled at its OUTERMOST await —
            # the inner _execute_task except never runs and the caller's
            # future stays pending forever (U9).  Run every task to its
            # first suspension point first, so the cancel lands inside the
            # coroutine's try block where its except completes the future.
            if pending:
                loop.run_until_complete(asyncio.sleep(0))
                pending = asyncio.all_tasks(loop)  # some finished in that tick
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            # WHY: close() relies on this thread exiting; a loop that is never
            # closed leaks its selector/file-descriptor set (C1).
            loop.close()

    async def start(self) -> None:
        """Start all worker threads and work-stealing queue dispatchers."""
        with self._lock:
            if self._running:
                return
            if self._native_pool is None:
                raise RuntimeError(
                    "gsyncio Rust extension (_gsyncio_core) is not installed. "
                    "Install the package with the compiled extension."
                )
            if self._native_pool.is_closed():
                # WHY: the native pool is consumed by close(); restarting the
                # pool after close() would silently accept tasks that can
                # never run (BUG-7).
                raise RuntimeError("pool cannot be restarted after close()")
            self._running = True
            self._started = True
            self._notify_events = [asyncio.Event() for _ in range(self.num_threads)]

            for i in range(self.num_threads):
                loop = self._loop_factory()
                t = threading.Thread(
                    target=self._worker,
                    args=(i, loop),
                    name=f"EventLoopThread-{i}",
                    daemon=True,
                )
                self._loops.append(loop)
                self._threads.append(t)

        # WHY: start the threads outside the lock (TS-11).  Thread.start()
        # has a happens-before edge, so workers observe the state published
        # under the lock above; starting inside the lock made each fresh
        # thread immediately contend for the same lock.
        for t in self._threads:
            t.start()

        _logger.info(
            "EventLoopThreadPool started with %d threads",
            self.num_threads,
            extra={"event": "pool_start", "thread_count": self.num_threads},
        )

    def _notify_worker(self, worker_idx: int) -> None:
        """Trigger instant Pipe/EventFD wakeup on the target worker's event loop."""
        with self._lock:
            if not self._running or worker_idx >= len(self._loops):
                return
            loop = self._loops[worker_idx]
            event = self._notify_events[worker_idx]

        try:
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            # The worker's loop was closed between the snapshot and the
            # wakeup (shutdown race) — nothing to wake (FIX-E).
            pass

    async def close(self) -> None:
        """Gracefully stop all event loop threads and join worker threads."""
        with self._lock:
            if not self._running:
                self._closed_event.set()
                return
            self._running = False
            loops = list(self._loops)
            threads = list(self._threads)
            self._loops.clear()
            self._threads.clear()

        if self._native_pool:
            self._native_pool.close()

        # Wait for workers to drain remaining buffered tasks and complete
        # them.  Workers pull items from the queue, execute them, then exit
        # their dispatch loop when pop_work() signals the pool is closed.
        # Poll active-task counters, native pool drained state, and outstanding tasks
        # so we only stop loops after all tasks finish.
        await asyncio.sleep(_DRAIN_GRACE_PERIOD)
        for _ in range(_MAX_DRAIN_ITERATIONS):  # Max ~5 seconds
            with self._lock:
                outstanding_count = len(self._outstanding)
            if self._metrics_collector.is_enabled:
                active = sum(self._metrics_collector.get_active(i) for i in range(self.num_threads))
            else:
                active = 0
            is_drained = (
                self._native_pool.is_drained()
                if self._native_pool and hasattr(self._native_pool, "is_drained")
                else True
            )
            if active == 0 and is_drained and outstanding_count == 0:
                break
            await asyncio.sleep(0.05)

        for loop in loops:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass  # worker loop already closed (shutdown race)

        for t in threads:
            try:
                t.join(timeout=2.0)
            except RuntimeError:
                # WHY: a thread not yet started cannot be joined (start()
                # launches threads outside _lock; close() may race the
                # launch window).  The worker exits on its own once started:
                # the native pool is already closed and the stop callback
                # is queued on its loop (R9 F-2).
                pass

        # WHY: a task popped by the drain but never stepped is cancelled in
        # the worker's finally at its outermost await — its future would
        # stay pending forever.  Complete everything that never ran, the
        # same safety net abort() has (U9 / R5 FIX-B).  The done() guard
        # makes a raced worker delivery win cleanly.
        with self._lock:
            leftover = list(self._outstanding)
            self._outstanding.clear()
        close_exc = ThreadPoolClosedError("Pool closed before task ran")
        for fut in leftover:
            target_obj = getattr(fut, "_gsyncio_target", None)
            if target_obj is not None and asyncio.iscoroutine(target_obj):
                target_obj.close()
            if not fut.done():
                _safe_complete(fut.get_loop(), fut, exc=close_exc)

        self._closed_event.set()
        _logger.info(
            "EventLoopThreadPool closed",
            extra={"event": "pool_close", "thread_count": len(threads)},
        )

    async def abort(self) -> None:
        """Forcefully stop all event loop threads without draining pending tasks.

        Unlike :meth:`close`, this skips the drain-grace period and immediately
        stops every worker loop, discarding any queued but unexecuted work.
        Every outstanding submit future is completed with
        :class:`ThreadPoolClosedError` so callers never hang on work that
        will not run (R2 FIX-13).
        """
        with self._lock:
            if not self._running:
                self._closed_event.set()
                return
            self._running = False
            loops = list(self._loops)
            threads = list(self._threads)
            self._loops.clear()
            self._threads.clear()
            # WHY: snapshot under the lock and clear — tasks still queued in
            # the Rust pool will never execute (no _execute_task finally to
            # discard them), so the set must be emptied here or the futures
            # leak for the pool's lifetime.
            outstanding = list(self._outstanding)
            self._outstanding.clear()

        if self._native_pool:
            self._native_pool.close()

        for loop in loops:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass  # worker loop already closed (shutdown race)

        for t in threads:
            try:
                t.join(timeout=2.0)
            except RuntimeError:
                # WHY: same launch-window race as close() — a worker thread
                # not yet started cannot be joined; it exits on its own once
                # started (native pool closed, stop queued) (R9 F-2).
                pass

        # Complete every future that never ran.  Delivery goes through the
        # same safe wrapper as the worker path, so a worker that did finish
        # just before the abort wins the race cleanly (revision B).
        abort_exc = ThreadPoolClosedError("Pool aborted")
        for fut in outstanding:
            target_obj = getattr(fut, "_gsyncio_target", None)
            if target_obj is not None and asyncio.iscoroutine(target_obj):
                target_obj.close()
            if not fut.done():
                _safe_complete(fut.get_loop(), fut, exc=abort_exc)

        self._closed_event.set()
        _logger.info(
            "EventLoopThreadPool aborted",
            extra={"event": "pool_abort", "thread_count": len(threads)},
        )

    async def wait_closed(self) -> None:
        """Wait until the pool has been fully stopped.

        Returns immediately if the pool was never started or is already
        closed.  Note: calling this concurrently with start() is a misuse —
        the never-started check may observe the pre-start state and return
        while the pool is actually coming up.
        """
        # WHY: a never-started pool has no close to wait for.  Reading both
        # flags under _lock keeps the check consistent with start()/close().
        # WHY: polling instead of asyncio.to_thread(threading.Event.wait) —
        # the to_thread thread cannot be cancelled and would stay blocked on
        # the event forever, hanging asyncio.run's executor shutdown (R10
        # P4).  A 10 ms poll is invisible next to a close() that takes
        # milliseconds anyway.
        with self._lock:
            started = self._started
        if not started or self._closed_event.is_set():
            return
        while not self._closed_event.is_set():
            await asyncio.sleep(0.01)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.close()

    def _resolve_target_worker(
        self, pin_target: asyncio.AbstractEventLoop | int | None
    ) -> tuple[int, asyncio.AbstractEventLoop] | None:
        with self._lock:
            loops = self._loops
            if not self._running or not loops:
                raise ThreadPoolClosedError("ThreadPool is not running")

            if pin_target is None:
                return None

            if isinstance(pin_target, int):
                if 0 <= pin_target < len(loops):
                    return pin_target, loops[pin_target]
                raise ValueError(f"Worker index {pin_target} out of range (0-{len(loops) - 1})")

            if isinstance(pin_target, asyncio.AbstractEventLoop):  # pyright: ignore[reportUnnecessaryIsInstance]
                for idx, l in enumerate(loops):
                    if l is pin_target:
                        return idx, l
                raise ValueError("Target AbstractEventLoop is not managed by this thread pool")

            raise TypeError("pin_to argument must be an AbstractEventLoop, int index, or None")

    def submit(
        self,
        target: Callable[..., Any],
        *args: Any,
        pin_to: asyncio.AbstractEventLoop | int | None = None,
        cancel_scope: CancelScope | None = None,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        """Submit a task to the thread pool using Native Rust Work-Stealing Pull model with Instant Wakeup.

        :param target:
            A coroutine function, coroutine object, or callable task to execute.
        :type target: callable or coroutine

        :param args:
            Positional arguments to pass to `target`.

        :param pin_to:
            Optional target event loop instance or worker index `int` for explicit pinning
            (e.g. for `asyncssh` connection affinity). If ``None``, pushed to the global
            shared queue for idle workers to pull instantly.
        :type pin_to: asyncio.AbstractEventLoop or int or None

        :param kwargs:
            Keyword arguments to pass to `target`.

        :returns: An :class:`asyncio.Future` representing the pending task execution.
        :rtype: :class:`asyncio.Future`

        :raises ThreadPoolClosedError:
            If task submission is attempted on a closed or unstarted pool.
        :raises ValueError:
            If `pin_to` is an invalid worker index or unmanaged event loop.

        """
        if asyncio.isfuture(target):
            raise TypeError(
                "submit() target cannot be an asyncio.Future (futures are thread-bound). "
                "Pass a coroutine function, callable, or coroutine instead."
            )
        if asyncio.iscoroutine(target) and (args or kwargs):
            raise TypeError(
                "Cannot pass positional or keyword arguments when target is already a coroutine object"
            )

        pinned_info = self._resolve_target_worker(pin_to)

        try:
            caller_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            caller_loop = None

        if caller_loop is None or not caller_loop.is_running():
            raise RuntimeError(
                "submit() must be called from a thread with a running asyncio event loop"
            )
        fut: asyncio.Future[Any] = caller_loop.create_future()
        if asyncio.iscoroutine(target):
            setattr(fut, "_gsyncio_target", target)  # noqa: B010
        with self._lock:
            self._outstanding.add(fut)

        _logger.debug(
            "EventLoopThreadPool submit",
            extra={"event": "pool_submit", "pinned": pinned_info is not None},
        )

        async def _execute_task() -> None:
            try:
                if cancel_scope is not None:
                    async with cancel_scope:
                        if asyncio.iscoroutine(target):
                            res = await target
                        elif callable(target):
                            res = target(*args, **kwargs)
                            if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                                res = await res
                        else:
                            res = target
                else:
                    if asyncio.iscoroutine(target):
                        res = await target
                    elif callable(target):
                        res = target(*args, **kwargs)
                        if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                            res = await res
                    else:
                        res = target

                if cancel_scope is not None and cancel_scope.cancel_called:
                    # WHY: the scope was cancelled — the caller must not be
                    # left awaiting a future that will never be completed.
                    # Deliver CancelledError exactly like a cancel would
                    # (R5 FIX-A).
                    if not fut.done():
                        _safe_complete(caller_loop, fut, exc=asyncio.CancelledError())
                    return
                if not fut.done():
                    _safe_complete(caller_loop, fut, result=res)
            except BaseException as exc:  # noqa: BLE001
                if cancel_scope is not None and cancel_scope.cancel_called:
                    if not fut.done():
                        _safe_complete(caller_loop, fut, exc=asyncio.CancelledError())
                    return
                if not fut.done():
                    _safe_complete(caller_loop, fut, exc=exc)
            finally:
                # WHY: every executed task releases its outstanding slot;
                # abort() snapshots the set to complete whatever never ran
                # (R2 FIX-13).
                with self._lock:
                    self._outstanding.discard(fut)

        # WHY: the caller's contextvars must reach the worker task — without
        # this, a submit from inside a task-local context (e.g. a tracing
        # span or a request-scoped variable) would run with the worker
        # loop's bare context (R4 probe A: 'missing' instead of the caller's
        # value).  _process_one reads it back via getattr; None keeps the
        # existing behaviour when the attribute is absent.  setattr (not
        # plain assignment) keeps mypy strict happy: the attribute is added
        # dynamically to a closure function.
        setattr(_execute_task, "_gsyncio_ctx", contextvars.copy_context())  # noqa: B010

        if pinned_info is not None:
            # Pinned to specific local queue in Rust Native Pool & trigger instant wakeup
            target_idx, _target_loop = pinned_info
            if self._native_pool:
                try:
                    self._native_pool.push_local(target_idx, _execute_task)
                except Exception as exc:  # noqa: BLE001
                    # WHY: a failed push means the task never entered a queue
                    # and the future can never complete — release its
                    # outstanding slot on EVERY failure, not just the Rust
                    # closed error (R5 FIX-H).
                    with self._lock:
                        self._outstanding.discard(fut)
                    if _RustPoolClosedError is not None and isinstance(exc, _RustPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise
                self._notify_worker(target_idx)
        else:
            # Pushed to global shared queue in Rust Native Pool
            if self._native_pool:
                try:
                    self._native_pool.push_global(_execute_task)
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        self._outstanding.discard(fut)
                    if _RustPoolClosedError is not None and isinstance(exc, _RustPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise

            # Directed Pull-Model Wakeup: preferentially notify an idle worker
            event: asyncio.Event | None = None
            loop: asyncio.AbstractEventLoop | None = None
            with self._lock:
                if self._loops and self._running:
                    if self._idle_workers:
                        idx = next(iter(self._idle_workers))
                    else:
                        idx = self._index
                        self._index = (idx + 1) % len(self._loops)
                    loop = self._loops[idx]
                    event = self._notify_events[idx]

            if loop is not None and event is not None:
                try:
                    loop.call_soon_threadsafe(event.set)
                except RuntimeError:
                    pass  # worker loop closed mid-submit (shutdown race)

        return fut


async def create_pool(
    num_threads: int = 0,
    options: PoolOptions | None = None,
    **kwargs: Any,
) -> EventLoopThreadPool:
    """Create and start a pool, returning it ready for use (asyncssh-style facade).

    The returned :class:`EventLoopThreadPool` is already started. Use it as an
    async context manager (``async with pool:`` auto-closes) or call
    :meth:`~EventLoopThreadPool.close` manually when finished.

    :param num_threads:
        The number of worker threads (0 = auto-detect via :func:`os.cpu_count`).
    :type num_threads: int
    :param options:
        Optional :class:`PoolOptions` providing defaults for pool parameters.
    :type options: PoolOptions or None
    :param kwargs:
        Additional keyword arguments passed to :class:`EventLoopThreadPool`.
    :returns: A started :class:`EventLoopThreadPool`.
    :rtype: :class:`EventLoopThreadPool`
    """
    pool = EventLoopThreadPool(num_threads=num_threads, options=options, **kwargs)
    await pool.start()
    return pool
