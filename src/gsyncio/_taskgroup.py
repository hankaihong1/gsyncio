"""Task groups (nurseries) for gsyncio — inspired by trio and anyio.

Provides structured concurrency: spawn child tasks inside a :class:`TaskGroup`
context manager and guarantee all children complete (or are cancelled) before
the block exits.
"""

import asyncio
import enum
from typing import Any, Self

from gsyncio._cancel import CancelScope


class _TaskStatus(enum.Enum):
    """Internal task lifecycle states."""

    PENDING = 0
    STARTED = 1
    FINISHED = 2


class TaskStatus:
    """Status tracker used with :meth:`TaskGroup.start`.

    Call :meth:`started` once the spawned coroutine has initialised so that
    :meth:`TaskGroup.start` can return the handle.
    """

    def __init__(self) -> None:
        self._started: asyncio.Event = asyncio.Event()

    def started(self) -> None:
        """Mark the task as started, unblocking :meth:`TaskGroup.start`."""
        self._started.set()


class TaskHandle:
    """A handle to a child task spawned inside a :class:`TaskGroup`.

    Awaiting the handle returns the task's result (or raises its exception).
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

        Raises:
            RuntimeError: if the task is not finished yet.
        """
        if not self._task.done():
            raise RuntimeError("task is not finished")
        if self._task.cancelled():
            raise asyncio.CancelledError()
        return self._task.result()

    @property
    def exception(self) -> BaseException | None:
        """Return the exception if the task failed, or ``None``.

        Raises:
            RuntimeError: if the task is not finished yet.
        """
        if not self._task.done():
            raise RuntimeError("task is not finished")
        if self._task.cancelled():
            return asyncio.CancelledError()
        return self._task.exception()

    def __await__(self) -> Any:
        return self._task.__await__()


class TaskGroup:
    """An async context manager that spawns and manages child tasks.

    Inspired by trio's ``Nursery`` and anyio's ``TaskGroup``, backed by
    :class:`CancelScope` for cancellation propagation.

    Usage::

        async with TaskGroup() as tg:
            h1 = tg.start_soon(worker, "a")
            h2 = tg.start_soon(worker, "b")
        # Both tasks are guaranteed finished here.
    """

    def __init__(self, name: str | None = None) -> None:
        self._name: str | None = name
        self._children: set[TaskHandle] = set()
        self._cancel_scope: CancelScope = CancelScope()

    # -- context manager -------------------------------------------------------

    async def __aenter__(self) -> Self:
        await self._cancel_scope.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        # Collect exceptions from all child tasks.
        child_exceptions = await self._wait_children()

        if not child_exceptions:
            return await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)

        # At least one child failed — cancel the scope so parent scopes
        # (and the hosting task) are aware of the failure.
        self._cancel_scope.cancel()

        # Exit the scope, suppressing the incoming CancelledError (if any)
        # because we are going to raise the children's errors instead.
        scope_exc_type = exc_type
        scope_exc_val = exc_val
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            scope_exc_type = None
            scope_exc_val = None

        await self._cancel_scope.__aexit__(scope_exc_type, scope_exc_val, exc_tb)

        # Raise collected child exceptions.
        if len(child_exceptions) == 1:
            raise child_exceptions[0]
        raise BaseExceptionGroup("taskgroup crashed", child_exceptions)

    async def _wait_children(self) -> list[BaseException]:
        """Wait for every child task to finish, collecting non-trivial exceptions.

        CancelledError raised as a direct result of our sibling-cancel call
        (i.e. after we cancel the scope and cancel remaining tasks) is
        filtered out.
        """
        exceptions: list[BaseException] = []
        tasks = [h._task for h in self._children]
        if not tasks:
            return exceptions

        pending: set[asyncio.Task[Any]] = set(tasks)
        cancelled_by_scope: set[int] = set()
        scope_cancelled = False

        async with CancelScope(shield=True):
            while pending:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    if task.cancelled():
                        # task.exception() raises CancelledError on cancelled
                        # tasks in Python 3.14. Synthesise it ourselves so we
                        # can distinguish sibling-cancel from external cancel.
                        exc: BaseException = asyncio.CancelledError()
                    else:
                        task_exc = task.exception()
                        if task_exc is None:
                            continue
                        exc = task_exc

                    # Was this CancelledError caused by *our* sibling-cancel?
                    if isinstance(exc, asyncio.CancelledError) and id(task) in cancelled_by_scope:
                        continue

                    exceptions.append(exc)

                    # On the first real failure, cancel the scope (which also
                    # cancels the nursery task) and cancel remaining siblings.
                    if not scope_cancelled:
                        # Cancel all remaining siblings directly.
                        for p in pending:
                            p.cancel()
                            cancelled_by_scope.add(id(p))

                        # Mark the scope as cancelled so parent scopes see it.
                        # CancelScope.cancel() also cancels the hosting (nursery)
                        # task — un-cancel it immediately so we can continue
                        # processing the remaining children.
                        self._cancel_scope.cancel()
                        cur = asyncio.current_task()
                        if cur is not None and cur.cancelling() > 0:
                            cur.uncancel()

                        scope_cancelled = True

        return exceptions

    # -- public API ------------------------------------------------------------

    def start_soon(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task and return its handle immediately.

        The coroutine is scheduled on the event loop; this method does not
        block.
        """
        task = asyncio.create_task(coro_fn(*args))
        handle = TaskHandle(task)
        self._children.add(handle)
        return handle

    async def start(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task, blocking until it calls ``task_status.started()``.

        The coroutine receives a :class:`TaskStatus` instance as its first
        argument.
        """
        task_status = TaskStatus()
        task = asyncio.create_task(coro_fn(task_status, *args))
        handle = TaskHandle(task)
        handle._start_event = task_status._started
        self._children.add(handle)
        try:
            await task_status._started.wait()
        finally:
            if task.done() and task.exception() is not None:
                exc = task.exception()
                for h in self._children:
                    if h._task is not task and not h._task.done():
                        h._task.cancel()
                if exc is not None:
                    raise exc
        return handle

    def cancel_all(self) -> None:
        """Cancel all child tasks using cross-loop-safe dispatch.

        Uses ``loop.call_soon_threadsafe(task.cancel)`` so this is safe to
        call from any thread, including time-out handlers running outside
        the nursery's own event loop.
        """
        for h in self._children:
            loop = h._task.get_loop()
            loop.call_soon_threadsafe(h._task.cancel)
