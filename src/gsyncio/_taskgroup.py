"""Task groups (nurseries) for gsyncio — inspired by trio and anyio.

Provides structured concurrency: spawn child tasks inside a :class:`TaskGroup`
context manager and guarantee all children complete (or are cancelled) before
the block exits.
"""

import asyncio
import enum
import threading
from typing import Any, Self

from gsyncio._cancel import CancelScope


def _retrieve_task_exception(task: asyncio.Task[Any]) -> None:
    """Consume a finished task's exception so asyncio does not log it.

    Used for orphan tasks we cancel on start_soon-after-exit: the task's
    CancelledError would otherwise surface as "exception was never
    retrieved" noise once the task finishes.
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

    Call :meth:`started` once the spawned coroutine has initialised so that
    :meth:`TaskGroup.start` can return the handle.  A child that exits
    without calling :meth:`started` makes :meth:`TaskGroup.start` raise
    :class:`RuntimeError` (trio/anyio parity).
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
        self._loop: asyncio.AbstractEventLoop | None = None
        self._children: set[TaskHandle] = set()
        # WHY: _children is mutated from the hosting task (start_soon/start/
        # _wait_children) and read from any thread (cancel_all), so every
        # access goes through this lock.  The critical sections never run
        # user code and never await — the lock only guards set operations
        # and quick snapshots.
        self._children_lock = threading.Lock()
        self._cancel_scope: CancelScope = CancelScope()
        # WHY: start_soon/start must reject new tasks once the group has
        # exited — a task spawned after __aexit__ would be an orphan nobody
        # waits for (R3 FIX-20).  Written in __aexit__'s finally, reset in
        # __aenter__, checked under _children_lock.
        self._exited = False
        # WHY: a start() child whose failure was already raised to the
        # caller (before started()) must not be re-collected by
        # _wait_children — the same exception would surface twice (R5
        # FIX-C).  Populated under _children_lock in start(), consulted by
        # _wait_children.
        self._consumed: set[asyncio.Task[Any]] = set()

    # -- context manager -------------------------------------------------------

    async def __aenter__(self) -> Self:
        # WHY: after a child failure the group's cancel scope stays cancelled;
        # re-entering such a group would raise a confusing CancelledError at
        # the first await.  Reject the reuse explicitly — and BEFORE the
        # scope is pushed onto the task-local stack, because __aexit__ is
        # never called when __aenter__ raises (a pushed scope would leak).
        if self._cancel_scope.cancel_called:
            raise RuntimeError("TaskGroup is not reusable after failure")
        self._loop = asyncio.get_running_loop()
        with self._children_lock:
            # WHY: children spawned BEFORE the first entry are part of the
            # first cycle — clearing them would silently orphan the tasks
            # (group exit neither waits nor cancels; probe R8-E).  Only
            # RE-entry starts a fresh lifecycle: the previous run's
            # children are all finished (__aexit__ waited for them), so
            # the clear is gated on having exited before (R5 revision C,
            # refined in R8).
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
            # WHY: every exit path (normal, body exception, cancellation,
            # child failure) ends the group's life — the flag must be set
            # even when _wait_children itself raises, so start_soon/start
            # can never spawn an orphan after the group ended (R3 FIX-20).
            with self._children_lock:
                self._exited = True
            self._loop = None

    async def _aexit_impl(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        # WHY: structured concurrency — when the body fails (exception or
        # cancellation) every still-running child must be cancelled before we
        # wait for them, otherwise a stuck child hangs the group forever
        # (BUG-9).  Children cancelled here are recorded so their
        # CancelledError is not reported as a group failure.
        pre_cancelled: set[asyncio.Task[Any]] = set()
        if exc_val is not None:
            with self._children_lock:
                remaining = [h._task for h in self._children if not h._task.done()]
            for task in remaining:
                task.cancel()
                pre_cancelled.add(task)
            # WHY: deliberately NOT calling self._cancel_scope.cancel() here —
            # it would inject a cancellation into the hosting task that the
            # shielded _wait_children would snapshot and later restore,
            # leaking a cancel count.  Scope cancellation is handled inside
            # _wait_children on the first real child failure; for a cancelled
            # body the external cancel already marks the scope.

        try:
            child_exceptions = await self._wait_children(pre_cancelled)
        except BaseException:
            # WHY: the group's host was cancelled WHILE waiting for the
            # children (e.g. Task.cancel() cascades into this group's host
            # through the awaited task's _fut_waiter, as in select_channel's
            # caller-cancel).  The pre-cancel branch above never ran, so
            # cancel every remaining child — then WAIT for them to finish
            # before propagating (R8 Unit 2): the structured-concurrency
            # guarantee must hold on the cancellation path too, and the
            # R5 FIX-F teardown needs the notifiers fully unwound before
            # the CE escapes (probe R8-C2: trio/anyio wait, we did not).
            with self._children_lock:
                # Reject new spawns while we drain: a task spawned from
                # another thread mid-drain would be missed by the snapshot
                # below and orphaned.  __aexit__'s finally sets the same
                # flag — idempotent.
                self._exited = True
                remaining = [h._task for h in self._children if not h._task.done()]
            for task in remaining:
                task.cancel()
            await self._drain_cancelled_children(remaining)
            raise

        # WHY (R9 guardrail 3): the wait is over — reject new spawns BEFORE
        # the next await (e.g. a done-callback could spawn during the
        # scope-exit await below and would otherwise be orphaned).  trio's
        # nursery closes lazily at the next spawn attempt; raising here is
        # the same observable.  Idempotent with __aexit__'s finally.
        with self._children_lock:
            self._exited = True

        # Cancellation wins: never merge CancelledError into the group —
        # merging would swallow the cancellation and hang outer timeouts.
        if exc_val is not None and isinstance(exc_val, asyncio.CancelledError):
            await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)
            return None  # let the body's CancelledError propagate

        if not child_exceptions:
            return await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)

        # All children failed with CancelledError while the body exited
        # normally: this is a soft group-exit signal (e.g. select_channel's
        # first-ready report), not a real failure.  Do NOT cancel the scope —
        # that would inject a cancellation into the hosting task and make a
        # well-behaved caller see its own signal as an external cancel.
        if exc_val is None and all(isinstance(e, asyncio.CancelledError) for e in child_exceptions):
            await self._cancel_scope.__aexit__(None, None, exc_tb)
            if len(child_exceptions) == 1:
                raise child_exceptions[0]
            raise BaseExceptionGroup("taskgroup soft exit", child_exceptions)

        # At least one child failed — cancel the scope so parent scopes
        # (and the hosting task) are aware of the failure.
        self._cancel_scope.cancel()

        # Exit the scope, suppressing an incoming CancelledError (if any)
        # because we are going to raise the children's errors instead.
        scope_exc_type = exc_type
        scope_exc_val = exc_val
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            scope_exc_type = None
            scope_exc_val = None

        await self._cancel_scope.__aexit__(scope_exc_type, scope_exc_val, exc_tb)

        # Collect body + child exceptions (BUG-3: the body's exception must
        # not vanish when children also fail).
        all_exceptions = list(child_exceptions)
        if exc_val is not None:
            all_exceptions.insert(0, exc_val)

        if len(all_exceptions) == 1:
            raise all_exceptions[0]
        raise BaseExceptionGroup("taskgroup crashed", all_exceptions)

    async def _wait_children(
        self, pre_cancelled: set[asyncio.Task[Any]] | None = None
    ) -> list[BaseException]:
        """Wait for every child task to finish, collecting non-trivial exceptions.

        CancelledError raised as a direct result of our sibling-cancel call
        (i.e. after we cancel the scope and cancel remaining tasks) is
        filtered out — including children cancelled by the caller before
        this method runs (``pre_cancelled``).
        """
        exceptions: list[BaseException] = []
        cancelled_by_scope: set[asyncio.Task[Any]] = set(pre_cancelled or ())
        scope_cancelled = False
        processed: set[asyncio.Task[Any]] = set()
        pending: set[asyncio.Task[Any]] = set()

        def cancel_siblings() -> None:
            # Cancel every pending sibling (absorbed spawns included); mark
            # the scope cancelled so parent scopes see the failure, and
            # consume the host injection so the host can keep collecting.
            nonlocal scope_cancelled
            for p in pending:
                p.cancel()
                cancelled_by_scope.add(p)
            self._cancel_scope.cancel()
            cur = asyncio.current_task()
            # WHY (R10 P5): consume ONLY the injection this scope actually
            # delivered.  _take_injected is False when the host was already
            # cancelling (task.cancel() is idempotent) or the binding was
            # cleared — uncancelling then would swallow a foreign count.
            if cur is not None and self._cancel_scope._take_injected():
                cur.uncancel()
            scope_cancelled = True

        def collect_one(task: asyncio.Task[Any]) -> None:
            # WHY: a start() child whose exception was already raised to the
            # caller must not be collected again — it would surface twice
            # (R5 FIX-C).
            with self._children_lock:
                if task in self._consumed:
                    return
            if task.cancelled():
                # task.exception() raises CancelledError on cancelled tasks
                # in Python 3.14.  Synthesise it ourselves so we can
                # distinguish sibling-cancel from external cancel.
                exc: BaseException = asyncio.CancelledError()
            else:
                task_exc = task.exception()
                if task_exc is None:
                    return
                exc = task_exc
            # A cancelled child is not an error (trio/anyio/stdlib
            # asyncio.TaskGroup parity; probes R8-A/D): the soft-exit branch
            # must only see children that RAISED CancelledError themselves.
            # Discriminator: injected cancel leaves cancelling() > 0, a
            # self-raised CancelledError never touched it.
            if isinstance(exc, asyncio.CancelledError) and (
                task in cancelled_by_scope or task.cancelling() > 0
            ):
                return
            exceptions.append(exc)
            if not scope_cancelled:
                cancel_siblings()

        def absorb() -> None:
            # WHY (R9): re-read the LIVE child set every iteration (anyio's
            # `while self._tasks` shape) so children spawned by a child
            # during the exit wait are awaited, not orphaned.  Three
            # guardrails, each verified by probe: (1) tasks that finished
            # before we could await them are collected NOW — dropping them
            # would swallow their exception (probe Q2); (2) spawns absorbed
            # after the first failure are cancelled immediately (anyio
            # parity: new children inherit the cancelled scope; probe Q3);
            # (3) the empty-set decision reads _children under the lock with
            # no await between the read and the exit, so a spawn cannot slip
            # past the gate.
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
        """Wait for cancelled children to finish before the group exits.

        The host is being cancelled: the first CancelledError was already
        delivered inside _wait_children (one-shot _must_cancel), so the
        next await is not re-interrupted by THAT cancel.  A FURTHER
        external cancel can land while we wait — each one is swallowed
        and the loop keeps waiting; the children finish on their own once
        cancelled (a cancellation-proof child blocks the exit — the same
        accepted semantics as trio/anyio).  The residual cancelling()
        count is left untouched: it is native asyncio semantics for a
        caught external cancel (R7), and consuming it here would risk
        swallowing a foreign count (R7-A accounting).  The caller
        re-raises the original exception after this returns.
        """
        pending = {t for t in tasks if not t.done()}
        while pending:
            try:
                done, pending = await asyncio.wait(pending)
            except asyncio.CancelledError:
                continue
            # asyncio.wait does not retrieve exceptions — consume them so
            # a child that failed (rather than being cancelled) never
            # surfaces as "Task exception was never retrieved" noise.
            for t in done:
                _retrieve_task_exception(t)

    # -- public API ------------------------------------------------------------

    def start_soon(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task and return its handle immediately.

        The coroutine is scheduled on the event loop; this method does not
        block.
        """
        current_loop = asyncio.get_running_loop()
        if self._loop is not None and current_loop is not self._loop:
            raise RuntimeError(
                "TaskGroup is physically scoped to a single event loop and cannot spawn tasks "
                "from a foreign event loop or thread. Use EventLoopThreadPool for cross-loop tasks."
            )
        # WHY: coro_fn(*args) is user code — never run it under
        # _children_lock (threading.Lock is not reentrant and user code may
        # call back into the group).
        task = asyncio.create_task(coro_fn(*args))
        handle = TaskHandle(task)
        with self._children_lock:
            if self._exited:
                # WHY: the group already ended — this task would be an
                # orphan nobody waits for.  Cancel it and consume its
                # eventual CancelledError so it cannot surface as
                # "exception was never retrieved" noise.
                task.cancel()
                task.add_done_callback(_retrieve_task_exception)
                raise RuntimeError(
                    "TaskGroup is not active: cannot start_soon() after the group exited"
                )
            self._children.add(handle)
        return handle

    async def start(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task, blocking until it calls ``task_status.started()``.

        The coroutine receives a :class:`TaskStatus` instance as its first
        argument.
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
        # WHY: a child that finishes WITHOUT calling started() (failure or
        # early return) would otherwise leave the started-event unset and
        # start() blocked forever (R5 FIX-C).  The callback resolves the
        # event so the wait below returns and the failure path can raise.
        task.add_done_callback(lambda _t: task_status._started.set())
        with self._children_lock:
            if self._exited:
                # WHY: same orphan guard as start_soon (R3 FIX-20).
                task.cancel()
                task.add_done_callback(_retrieve_task_exception)
                raise RuntimeError("TaskGroup is not active: cannot start() after the group exited")
            self._children.add(handle)
        try:
            await task_status._started.wait()
        finally:
            # WHY: the child's lifecycle is decided once done() is true
            # (state is frozen), so the checks below have no TOCTOU.
            if task.done():
                exc: BaseException | None = None
                if task.cancelled():
                    # WHY (R10 P2): on 3.14 task.exception() RAISES
                    # CancelledError for cancelled tasks — calling it here
                    # would escape the finally and skip the consume and
                    # sibling-cancel below.  trio parity: a child cancelled
                    # BEFORE started() makes start() propagate the
                    # cancellation; a child cancelled AFTER started() is a
                    # completed start protocol — the handle is returned and
                    # the group's soft-exit path reports the cancellation.
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
                        # WHY: the exception is raised here, to the caller —
                        # _wait_children must not collect it a second time
                        # and re-raise it as a group failure (R5 FIX-C).
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
        """Cancel all child tasks using cross-loop-safe dispatch.

        Uses ``loop.call_soon_threadsafe(task.cancel)`` so this is safe to
        call from any thread, including time-out handlers running outside
        the nursery's own event loop.
        """
        # WHY: snapshot under the lock — iterating the live set from another
        # thread while the hosting task adds children is a data race on
        # free-threaded builds.
        with self._children_lock:
            handles = list(self._children)
        for h in handles:
            loop = h._task.get_loop()
            try:
                loop.call_soon_threadsafe(h._task.cancel)
            except RuntimeError:
                # WHY: the child's loop was closed — it is already gone
                # (R5 FIX-E).
                pass
