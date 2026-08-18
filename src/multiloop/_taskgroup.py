"""Structured concurrency nurseries (TaskGroup) for multiloop."""

from __future__ import annotations

import asyncio
import enum
import threading
from typing import Any, Self

from multiloop._cancel import CancelScope

__all__ = ["TaskGroup", "TaskHandle", "TaskStatus"]


def _retrieve_task_exception(task: asyncio.Task[Any]) -> None:
    """Consume a finished task's exception so asyncio does not log unretrieved exception warnings.

    Used for orphan tasks cancelled on start_soon after group exit.
    """
    if not task.cancelled():
        task.exception()


class _TaskStatus(enum.Enum):
    """Internal task lifecycle states."""

    PENDING = 0
    STARTED = 1
    FINISHED = 2


class TaskStatus:
    """Status tracker used with :meth:`TaskGroup.start`.

    Call :meth:`started` once the spawned coroutine has initialized so that
    :meth:`TaskGroup.start` can unblock and return the handle. A child that exits
    without calling :meth:`started` causes :meth:`TaskGroup.start` to raise :class:`RuntimeError`.
    """

    def __init__(self) -> None:
        self._started: asyncio.Event = asyncio.Event()
        self._called = False

    def started(self) -> None:
        """Mark the task as started, unblocking :meth:`TaskGroup.start`."""
        self._called = True
        self._started.set()


class TaskHandle:
    """A handle to a child task spawned inside a :class:`TaskGroup`.

    Awaiting the handle returns the task's result or raises its exception.
    """

    def __init__(self, task: asyncio.Task[Any]) -> None:
        self._task: asyncio.Task[Any] = task
        self._start_event: asyncio.Event | None = None

    @property
    def status(self) -> _TaskStatus:
        """Current lifecycle status of the wrapped task."""
        if self._task.done():
            return _TaskStatus.FINISHED
        if self._start_event is not None and self._start_event.is_set():
            return _TaskStatus.STARTED
        return _TaskStatus.PENDING

    @property
    def result(self) -> Any:
        """Return the task result once finished.

        :raises RuntimeError: If the task has not finished yet.
        :raises asyncio.CancelledError: If the task was cancelled.
        """
        if not self._task.done():
            raise RuntimeError("Task is not finished")
        if self._task.cancelled():
            raise asyncio.CancelledError()
        return self._task.result()

    @property
    def exception(self) -> BaseException | None:
        """Return the exception if the task failed, or None.

        :raises RuntimeError: If the task has not finished yet.
        :raises asyncio.CancelledError: If the task was cancelled.
        """
        if not self._task.done():
            raise RuntimeError("Task is not finished")
        if self._task.cancelled():
            return asyncio.CancelledError()
        return self._task.exception()

    def __await__(self) -> Any:
        return self._task.__await__()


class TaskGroup:
    """An async context manager for structured concurrency (nursery).

    Guarantees that all spawned child tasks finish (or are cleanly cancelled) before
    the context manager block exits. If any child task fails, sibling tasks are automatically
    cancelled and exceptions are aggregated into an exception group.

    Usage::

        async with TaskGroup() as tg:
            h1 = tg.start_soon(worker, "a")
            h2 = tg.start_soon(worker, "b")
        # All child tasks are guaranteed finished here.
    """

    def __init__(self, name: str | None = None) -> None:
        self._name: str | None = name
        self._loop: asyncio.AbstractEventLoop | None = None
        self._children: set[TaskHandle] = set()
        self._children_lock = threading.Lock()
        self._cancel_scope: CancelScope = CancelScope()
        self._exited = False
        self._consumed: set[asyncio.Task[Any]] = set()

    # -- context manager -------------------------------------------------------

    async def __aenter__(self) -> Self:
        if self._cancel_scope.cancel_called:
            raise RuntimeError("TaskGroup is not reusable after failure")
        self._loop = asyncio.get_running_loop()
        with self._children_lock:
            was_exited = self._exited
            self._exited = False
            if was_exited:
                self._children.clear()
                self._consumed.clear()
        await self._cancel_scope.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        try:
            return await self._aexit_impl(exc_type, exc_val, exc_tb)
        finally:
            with self._children_lock:
                self._exited = True
            self._loop = None

    async def _aexit_impl(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        # Structured concurrency: when the group body raises or cancels,
        # cancel all remaining child tasks before awaiting them.
        pre_cancelled: set[asyncio.Task[Any]] = set()
        if exc_val is not None:
            with self._children_lock:
                remaining = [h._task for h in self._children if not h._task.done()]
            for task in remaining:
                task.cancel()
                pre_cancelled.add(task)

        try:
            child_exceptions = await self._wait_children(pre_cancelled)
        except BaseException:
            # If the group's host task is cancelled while waiting for children,
            # cancel remaining tasks and wait for their completion before re-raising.
            with self._children_lock:
                self._exited = True
                remaining = [h._task for h in self._children if not h._task.done()]
            for task in remaining:
                task.cancel()
            await self._drain_cancelled_children(remaining)
            raise

        with self._children_lock:
            self._exited = True

        # External cancellation takes precedence
        if exc_val is not None and isinstance(exc_val, asyncio.CancelledError):
            await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)
            return None

        if not child_exceptions:
            return await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)

        # Soft exit: all children raised CancelledError while body finished normally
        if exc_val is None and all(isinstance(e, asyncio.CancelledError) for e in child_exceptions):
            await self._cancel_scope.__aexit__(None, None, exc_tb)
            if len(child_exceptions) == 1:
                raise child_exceptions[0]
            raise BaseExceptionGroup("taskgroup soft exit", child_exceptions)

        # At least one child task raised a non-cancellation exception
        self._cancel_scope.cancel()

        scope_exc_type = exc_type
        scope_exc_val = exc_val
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            scope_exc_type = None
            scope_exc_val = None

        await self._cancel_scope.__aexit__(scope_exc_type, scope_exc_val, exc_tb)

        all_exceptions = list(child_exceptions)
        if exc_val is not None:
            all_exceptions.insert(0, exc_val)

        if len(all_exceptions) == 1:
            raise all_exceptions[0]
        raise BaseExceptionGroup("taskgroup crashed", all_exceptions)

    async def _wait_children(
        self, pre_cancelled: set[asyncio.Task[Any]] | None = None
    ) -> list[BaseException]:
        """Wait for all child tasks to complete, collecting non-trivial exceptions."""
        exceptions: list[BaseException] = []
        cancelled_by_scope: set[asyncio.Task[Any]] = set(pre_cancelled or ())
        scope_cancelled = False
        processed: set[asyncio.Task[Any]] = set()
        pending: set[asyncio.Task[Any]] = set()

        def cancel_siblings() -> None:
            nonlocal scope_cancelled
            for p in pending:
                p.cancel()
                cancelled_by_scope.add(p)
            self._cancel_scope.cancel()
            cur = asyncio.current_task()
            if cur is not None and self._cancel_scope._take_injected():
                cur.uncancel()
            scope_cancelled = True

        def collect_one(task: asyncio.Task[Any]) -> None:
            with self._children_lock:
                if task in self._consumed:
                    return
            if task.cancelled():
                exc: BaseException = asyncio.CancelledError()
            else:
                task_exc = task.exception()
                if task_exc is None:
                    return
                exc = task_exc

            if isinstance(exc, asyncio.CancelledError) and (
                task in cancelled_by_scope or task.cancelling() > 0
            ):
                return
            exceptions.append(exc)
            if not scope_cancelled:
                cancel_siblings()

        def absorb() -> None:
            with self._children_lock:
                current = [h._task for h in self._children]
            for task in current:
                if task in processed or task in pending:
                    continue
                if task.done():
                    collect_one(task)
                    processed.add(task)
                elif scope_cancelled:
                    task.cancel()
                    cancelled_by_scope.add(task)
                    pending.add(task)
                else:
                    pending.add(task)

        async with CancelScope(shield=True):
            while True:
                absorb()
                if not pending:
                    break
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    processed.add(task)
                    collect_one(task)
        return exceptions

    async def _drain_cancelled_children(self, tasks: list[asyncio.Task[Any]]) -> None:
        """Wait for cancelled children to complete before propagating cancellation outwards."""
        pending = {t for t in tasks if not t.done()}
        while pending:
            try:
                done, pending = await asyncio.wait(pending)
            except asyncio.CancelledError:
                continue
            for t in done:
                _retrieve_task_exception(t)

    # -- public API ------------------------------------------------------------

    def start_soon(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task and return its handle immediately without blocking.

        :param coro_fn: Coroutine function to spawn.
        :param args: Arguments to forward to `coro_fn`.
        :returns: A :class:`TaskHandle` referencing the child task.
        :raises RuntimeError: If called after the TaskGroup context has exited.
        """
        current_loop = asyncio.get_running_loop()
        if self._loop is not None and current_loop is not self._loop:
            raise RuntimeError(
                "TaskGroup is physically scoped to a single event loop and cannot spawn tasks "
                "from a foreign event loop or thread. Use EventLoopThreadPool for cross-loop tasks."
            )
        task = asyncio.create_task(coro_fn(*args))
        handle = TaskHandle(task)
        with self._children_lock:
            if self._exited:
                task.cancel()
                task.add_done_callback(_retrieve_task_exception)
                raise RuntimeError(
                    "TaskGroup is not active: cannot start_soon() after the group exited"
                )
            self._children.add(handle)
        return handle

    async def start(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task, suspending until the coroutine calls ``task_status.started()``.

        :param coro_fn: Coroutine function expecting a :class:`TaskStatus` as its first parameter.
        :param args: Additional arguments to forward to `coro_fn`.
        :returns: A :class:`TaskHandle` referencing the started child task.
        :raises RuntimeError: If the task exits or crashes before calling `task_status.started()`.
        """
        current_loop = asyncio.get_running_loop()
        if self._loop is not None and current_loop is not self._loop:
            raise RuntimeError(
                "TaskGroup is physically scoped to a single event loop and cannot spawn tasks "
                "from a foreign event loop or thread. Use EventLoopThreadPool for cross-loop tasks."
            )
        task_status = TaskStatus()
        task = asyncio.create_task(coro_fn(task_status, *args))
        handle = TaskHandle(task)
        handle._start_event = task_status._started
        task.add_done_callback(lambda _t: task_status._started.set())
        with self._children_lock:
            if self._exited:
                task.cancel()
                task.add_done_callback(_retrieve_task_exception)
                raise RuntimeError("TaskGroup is not active: cannot start() after the group exited")
            self._children.add(handle)
        try:
            await task_status._started.wait()
        finally:
            if task.done():
                exc: BaseException | None = None
                if task.cancelled():
                    if not task_status._called:
                        exc = asyncio.CancelledError()
                else:
                    task_exc = task.exception()
                    if task_exc is not None:
                        exc = task_exc
                    elif not task_status._called:
                        raise RuntimeError("Child exited without calling task_status.started()")
                if exc is not None:
                    with self._children_lock:
                        self._consumed.add(task)
                        siblings = [
                            h._task
                            for h in self._children
                            if h._task is not task and not h._task.done()
                        ]
                    for sibling in siblings:
                        sibling.cancel()
                    raise exc
        return handle

    def cancel_all(self) -> None:
        """Cancel all child tasks safely across threads using ``loop.call_soon_threadsafe``."""
        with self._children_lock:
            handles = list(self._children)
        for h in handles:
            loop = h._task.get_loop()
            try:
                loop.call_soon_threadsafe(h._task.cancel)
            except RuntimeError:
                pass
