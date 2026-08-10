"""Multi-event-loop thread pool implementation with Work-Stealing Shared Queue Architecture."""

from __future__ import annotations

import asyncio
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


class _WorkerPoolProtocol(Protocol):
    """Protocol for the Rust NativeWorkerPool class."""

    def __init__(self, num_threads: int) -> None: ...
    def pop_work(self, index: int) -> Any: ...
    def is_closed(self) -> bool: ...
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
        self._index = 0

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
            loop.create_task(self._run_task_wrapper(index, task_func))

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
                except Exception:  # noqa: BLE001
                    if self._native_pool.is_closed():
                        return
                    break

            # Phase 2 — Shutdown gate: if the pool is closed and the queue is
            # drained (we reached here after Phase 1 finished), exit.
            if self._native_pool.is_closed():
                return

            # Phase 3 — Shutting down but not yet closed: brief yield to avoid
            # busy-waiting, then retry drain (close() may arrive soon).
            if not self.is_running:
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

                try:
                    await asyncio.wait_for(notify_event.wait(), timeout=_WORKER_POLL_INTERVAL)
                except TimeoutError:
                    pass
                except asyncio.CancelledError:
                    break

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

        loop.call_soon_threadsafe(event.set)

    def _notify_all_workers(self) -> None:
        """Trigger instant Pipe/EventFD wakeup across all worker event loops."""
        with self._lock:
            if not self._running:
                return
            loops = list(self._loops)
            events = list(self._notify_events)

        for loop, event in zip(loops, events, strict=False):

            def _wake(evt: asyncio.Event = event) -> None:
                evt.set()

            loop.call_soon_threadsafe(_wake)

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

        self._notify_all_workers()

        # Wait for workers to drain remaining buffered tasks and complete
        # them.  Workers pull items from the queue, execute them, then exit
        # their dispatch loop when pop_work() signals the pool is closed.
        # Poll active-task counters so we only stop loops after tasks finish.
        # Include an initial grace period so workers have time to wake up
        # and start draining before the first active==0 check.
        if self._metrics_collector.is_enabled:
            await asyncio.sleep(_DRAIN_GRACE_PERIOD)
            for _ in range(_MAX_DRAIN_ITERATIONS):  # Max ~5 seconds
                active = sum(self._metrics_collector.get_active(i) for i in range(self.num_threads))
                if active == 0:
                    break
                await asyncio.sleep(0.1)

        for loop in loops:
            loop.call_soon_threadsafe(loop.stop)

        for t in threads:
            t.join(timeout=2.0)

        self._closed_event.set()
        _logger.info(
            "EventLoopThreadPool closed",
            extra={"event": "pool_close", "thread_count": len(threads)},
        )

    async def abort(self) -> None:
        """Forcefully stop all event loop threads without draining pending tasks.

        Unlike :meth:`close`, this skips the drain-grace period and immediately
        stops every worker loop, discarding any queued but unexecuted work.
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

        if self._native_pool:
            self._native_pool.close()

        self._notify_all_workers()

        for loop in loops:
            loop.call_soon_threadsafe(loop.stop)

        for t in threads:
            t.join(timeout=2.0)

        self._closed_event.set()
        _logger.info(
            "EventLoopThreadPool aborted",
            extra={"event": "pool_abort", "thread_count": len(threads)},
        )

    async def wait_closed(self) -> None:
        """Wait until the pool has been fully stopped.

        Returns immediately if the pool is not running or is already closed.
        """
        # WHY: to_thread — the event is a threading.Event (see __init__).
        await asyncio.to_thread(self._closed_event.wait)

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
        self, loop_target: asyncio.AbstractEventLoop | int | None
    ) -> tuple[int, asyncio.AbstractEventLoop] | None:
        with self._lock:
            loops = self._loops
            if not self._running or not loops:
                raise ThreadPoolClosedError("ThreadPool is not running")

            if loop_target is None:
                return None

            if isinstance(loop_target, int):
                if 0 <= loop_target < len(loops):
                    return loop_target, loops[loop_target]
                raise ValueError(f"Worker index {loop_target} out of range (0-{len(loops) - 1})")

            if isinstance(loop_target, asyncio.AbstractEventLoop):  # pyright: ignore[reportUnnecessaryIsInstance]
                for idx, l in enumerate(loops):
                    if l is loop_target:
                        return idx, l
                raise ValueError("Target AbstractEventLoop is not managed by this thread pool")

            raise TypeError("loop argument must be an AbstractEventLoop, int index, or None")

    def submit(
        self,
        target: Callable[..., Any],
        *args: Any,
        loop: asyncio.AbstractEventLoop | int | None = None,
        cancel_scope: CancelScope | None = None,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        """Submit a task to the thread pool using Native Rust Work-Stealing Pull model with Instant Wakeup.

        :param target:
            A coroutine function, coroutine object, or callable task to execute.
        :type target: callable or coroutine

        :param args:
            Positional arguments to pass to `target`.

        :param loop:
            Optional target event loop instance or worker index `int` for explicit pinning
            (e.g. for `asyncssh` connection affinity). If ``None``, pushed to the global
            shared queue for idle workers to pull instantly.
        :type loop: asyncio.AbstractEventLoop or int or None

        :param kwargs:
            Keyword arguments to pass to `target`.

        :returns: An :class:`asyncio.Future` representing the pending task execution.
        :rtype: :class:`asyncio.Future`

        :raises ThreadPoolClosedError:
            If task submission is attempted on a closed or unstarted pool.
        :raises ValueError:
            If `loop` is an invalid worker index or unmanaged event loop.

        """
        pinned_info = self._resolve_target_worker(loop)

        try:
            caller_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            caller_loop = None

        if caller_loop is None or not caller_loop.is_running():
            raise RuntimeError(
                "submit() must be called from a thread with a running asyncio event loop"
            )
        fut: asyncio.Future[Any] = caller_loop.create_future()

        _logger.debug(
            "EventLoopThreadPool submit",
            extra={"event": "pool_submit", "pinned": pinned_info is not None},
        )

        async def _execute_task() -> None:
            try:
                if asyncio.iscoroutine(target) or asyncio.isfuture(target):
                    res = await target
                elif callable(target):
                    res = target(*args, **kwargs)
                    if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                        res = await res
                else:
                    res = target

                if cancel_scope is not None and cancel_scope.cancel_called:
                    return
                if not fut.done():
                    if caller_loop and not caller_loop.is_closed():
                        try:
                            caller_loop.call_soon_threadsafe(fut.set_result, res)
                        except RuntimeError:
                            # WHY: the caller loop closed between the
                            # is_closed() check and the delivery (TS-12) —
                            # nobody will ever consume the future.
                            fut.set_result(res)
                    else:
                        fut.set_result(res)
            except BaseException as exc:  # noqa: BLE001
                if cancel_scope is not None and cancel_scope.cancel_called:
                    return
                if not fut.done():
                    if caller_loop and not caller_loop.is_closed():
                        try:
                            caller_loop.call_soon_threadsafe(fut.set_exception, exc)
                        except RuntimeError:
                            fut.set_exception(exc)
                    else:
                        fut.set_exception(exc)

        if pinned_info is not None:
            # Pinned to specific local queue in Rust Native Pool & trigger instant wakeup
            target_idx, _target_loop = pinned_info
            if self._native_pool:
                try:
                    self._native_pool.push_local(target_idx, _execute_task)
                    self._notify_worker(target_idx)
                except Exception as exc:  # noqa: BLE001
                    if _RustPoolClosedError is not None and isinstance(exc, _RustPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise
        else:
            # Pushed to global shared queue in Rust Native Pool
            if self._native_pool:
                try:
                    self._native_pool.push_global(_execute_task)
                except Exception as exc:  # noqa: BLE001
                    if _RustPoolClosedError is not None and isinstance(exc, _RustPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise

            # Pure Pull-Model Wakeup: Blind Round-Robin Notify
            event: asyncio.Event | None = None
            with self._lock:
                if self._loops and self._running:
                    idx = self._index
                    self._index = (idx + 1) % len(self._loops)
                    loop = self._loops[idx]
                    event = self._notify_events[idx]
                else:
                    loop = None

            if loop is not None and event is not None:
                loop.call_soon_threadsafe(event.set)

        return fut

    def submit_daemon(
        self,
        target: Callable[..., Any],
        *args: Any,
        loop: asyncio.AbstractEventLoop | int | None = None,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        """Submit a long-running daemon task to a worker event loop.

        :param target:
            A coroutine function or callable daemon task.
        :type target: callable or coroutine

        :param loop:
            Optional target event loop instance or worker index `int` for explicit pinning.
        :type loop: asyncio.AbstractEventLoop or int or None

        :returns: An :class:`asyncio.Future` representing the pending daemon task.
        :rtype: :class:`asyncio.Future`

        """
        return self.submit(target, *args, loop=loop, **kwargs)


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
