"""Multi-event-loop thread pool engine with work-stealing shared queue architecture."""

from __future__ import annotations

import asyncio
import contextvars
import os
import threading
import types
from collections.abc import Callable
from typing import Any, Protocol, Self

from multiloop._cancel import CancelScope
from multiloop._logging import get_logger
from multiloop._metrics import MetricsCollector
from multiloop._options import PoolOptions
from multiloop._rust import _try_import_rust_class
from multiloop.exceptions import ThreadPoolClosedError

__all__ = [
    "EventLoopThreadPool",
    "create_pool",
]

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
    """Complete *fut* on its owning loop safely, tolerating concurrent cancellation or abort."""

    def _do() -> None:
        try:
            if exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(result)
        except (asyncio.InvalidStateError, RuntimeError):
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
    """Protocol for the native Rust NativeWorkerPool class."""

    def __init__(self, num_threads: int) -> None: ...
    def pop_work(self, index: int) -> Any: ...
    def is_closed(self) -> bool: ...
    def is_drained(self) -> bool: ...
    def close(self) -> None: ...
    def push_global(self, task: Any) -> None: ...
    def push_local(self, index: int, task: Any) -> None: ...
    def set_metrics(self, metrics: Any) -> None: ...


NativeWorkerPool: type[_WorkerPoolProtocol] | None = _try_import_rust_class(
    "multiloop._multiloop_core", "NativeWorkerPool"
)
_RustPoolClosedError = _try_import_rust_class("multiloop._multiloop_core", "ThreadPoolClosedError")


class EventLoopThreadPool:
    """Multi-Event-Loop Thread Pool with Work-Stealing Shared Queue Architecture.

    Orchestrates multiple OS worker threads, each executing an isolated :class:`asyncio.AbstractEventLoop`.
    Unpinned tasks are pushed to a global lock-free shared queue where idle worker loops pull and
    execute them dynamically. Tasks requiring event loop affinity (e.g., protocol connections)
    can be pinned to specific target loops.

    :param options: Optional :class:`PoolOptions` providing configuration defaults.
    :param num_threads: Number of worker threads (0 = auto-detect via :func:`os.cpu_count`).
    :param loop_factory: Factory callable returning a new event loop instance.
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
        self._started = False
        self._index = 0
        self._idle_workers: set[int] = set()
        self._outstanding: set[asyncio.Future[Any]] = set()

        # Native Rust lock-free pool controller
        self._native_pool: _WorkerPoolProtocol | None = None
        if NativeWorkerPool is not None:
            self._native_pool = NativeWorkerPool(self.num_threads)

        self._metrics_collector = MetricsCollector(self.num_threads)
        if self._native_pool is not None and self._metrics_collector._metrics is not None:
            self._native_pool.set_metrics(self._metrics_collector._metrics)
        self._closed_event = threading.Event()

    def __repr__(self) -> str:
        return (
            f"<EventLoopThreadPool running={self.is_running} "
            f"threads={self.num_threads} metrics={self.get_metrics()}>"
        )

    @property
    def is_running(self) -> bool:
        """Return True if the thread pool is currently running and accepting tasks."""
        with self._lock:
            return self._running

    def _get_loop(self, index: int) -> asyncio.AbstractEventLoop:
        """Return the event loop at the given worker index."""
        with self._lock:
            if not self._running or index >= len(self._loops):
                raise RuntimeError("pool is not running")
            return self._loops[index]

    def get_metrics(self) -> dict[str, Any]:
        """Return JSON-serializable health & performance metrics of the thread pool."""
        with self._lock:
            return self._metrics_collector.get_snapshot(self._running)

    async def _run_task_wrapper(self, worker_idx: int, task_func: Callable[[], Any]) -> None:
        """Execute a task pulled from the queue while tracking active metrics."""
        self._metrics_collector.inc_active(worker_idx)
        try:
            await task_func()
        finally:
            self._metrics_collector.dec_active(worker_idx)

    async def _worker_dispatcher(self, index: int, notify_event: asyncio.Event) -> None:
        """Worker dispatch loop executing 3-source drain and event-driven task consumption."""
        loop = asyncio.get_running_loop()

        def _process_one(task_func: Callable[[], Any]) -> None:
            # Propagate caller's contextvars across OS thread boundaries
            ctx = getattr(task_func, "_multiloop_ctx", None)
            loop.create_task(self._run_task_wrapper(index, task_func), context=ctx)

        while True:
            if self._native_pool is None:
                break

            processed_any = False
            # Phase 1: Drain pending tasks from the Rust lock-free queues
            while True:
                try:
                    task_func = self._native_pool.pop_work(index)
                    if task_func is not None:
                        _process_one(task_func)
                        processed_any = True
                        # Yield to avoid one fast worker starving other workers from batch pulls
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

            # Phase 2: Shutdown check
            if not self.is_running or self._native_pool.is_closed():
                await asyncio.sleep(0.01)
                continue

            # Phase 3: Idle wait on notify_event
            if not processed_any:
                notify_event.clear()
                # Double-check after clearing event to avoid lost wakeups
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
            if pending:
                loop.run_until_complete(asyncio.sleep(0))
                pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    async def start(self) -> None:
        """Start all worker threads and work-stealing queue dispatchers."""
        with self._lock:
            if self._running:
                return
            if self._native_pool is None:
                raise RuntimeError(
                    "multiloop Rust extension (_multiloop_core) is not installed. "
                    "Install the package with the compiled extension."
                )
            if self._native_pool.is_closed():
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
            pass

    async def close(self, timeout: float | None = None) -> None:
        """Gracefully shut down the thread pool and release all resources.

        Drains active tasks, stops each worker event loop, and joins threads.
        Safe to call multiple times (idempotent).

        :param timeout: Maximum seconds to wait for active tasks to drain.
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

        await asyncio.sleep(_DRAIN_GRACE_PERIOD)
        drain_iterations = (
            max(1, int(timeout / 0.05)) if timeout is not None else _MAX_DRAIN_ITERATIONS
        )
        for _ in range(drain_iterations):
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
                pass

        for t in threads:
            try:
                t.join(timeout=2.0)
            except RuntimeError:
                pass

        with self._lock:
            leftover = list(self._outstanding)
            self._outstanding.clear()
        close_exc = ThreadPoolClosedError("Pool closed before task ran")
        for fut in leftover:
            target_obj = getattr(fut, "_multiloop_target", None)
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
        """Forcefully stop all event loop threads without waiting for pending tasks to drain.

        Immediately stops every worker loop and completes outstanding futures with
        :class:`ThreadPoolClosedError`.
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
            outstanding = list(self._outstanding)
            self._outstanding.clear()

        if self._native_pool:
            self._native_pool.close()

        for loop in loops:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass

        for t in threads:
            try:
                t.join(timeout=2.0)
            except RuntimeError:
                pass

        abort_exc = ThreadPoolClosedError("Pool aborted")
        for fut in outstanding:
            target_obj = getattr(fut, "_multiloop_target", None)
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
        """Wait until the pool has completely terminated."""
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
        """Submit a task to the thread pool for execution across worker event loops.

        :param target: A coroutine function, coroutine object, or callable task.
        :param args: Positional arguments to pass to `target`.
        :param pin_to: Optional target loop instance or worker index for task affinity pinning.
        :param cancel_scope: Optional :class:`CancelScope` tracking task cancellation.
        :param kwargs: Keyword arguments to pass to `target`.
        :returns: An :class:`asyncio.Future` tracking the task's execution.
        :raises ThreadPoolClosedError: If submitted to an unstarted or closed pool.
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
            setattr(fut, "_multiloop_target", target)  # noqa: B010
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
                with self._lock:
                    self._outstanding.discard(fut)

        setattr(_execute_task, "_multiloop_ctx", contextvars.copy_context())  # noqa: B010

        if pinned_info is not None:
            target_idx, _target_loop = pinned_info
            if self._native_pool:
                try:
                    self._native_pool.push_local(target_idx, _execute_task)
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        self._outstanding.discard(fut)
                    if _RustPoolClosedError is not None and isinstance(exc, _RustPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise
                self._notify_worker(target_idx)
        else:
            if self._native_pool:
                try:
                    self._native_pool.push_global(_execute_task)
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        self._outstanding.discard(fut)
                    if _RustPoolClosedError is not None and isinstance(exc, _RustPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise

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
                    pass

        return fut


async def create_pool(
    num_threads: int = 0,
    options: PoolOptions | None = None,
    **kwargs: Any,
) -> EventLoopThreadPool:
    """Create, start, and return an active :class:`EventLoopThreadPool` instance.

    :param num_threads: Number of worker threads (0 = auto-detect via :func:`os.cpu_count`).
    :param options: Optional :class:`PoolOptions` defaults.
    :param kwargs: Additional arguments forwarded to :class:`EventLoopThreadPool`.
    :returns: An active, already-started :class:`EventLoopThreadPool`.
    """
    pool = EventLoopThreadPool(num_threads=num_threads, options=options, **kwargs)
    await pool.start()
    return pool
