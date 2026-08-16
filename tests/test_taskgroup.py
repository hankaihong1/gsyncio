"""Tests for gsyncio TaskGroup (structured concurrency)."""

import asyncio
import threading
from typing import Any

import pytest

from gsyncio._taskgroup import TaskGroup, TaskHandle, TaskStatus, _TaskStatus


async def _worker(value: Any) -> Any:
    await asyncio.sleep(0)
    return value


async def _worker_slow(value: Any, delay: float = 0.1) -> Any:
    await asyncio.sleep(delay)
    return value


async def _worker_fail(msg: str) -> None:
    await asyncio.sleep(0)
    raise ValueError(msg)


async def _worker_fail_slow(msg: str, delay: float = 0.1) -> None:
    await asyncio.sleep(delay)
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Test 1: all children succeed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_succeed() -> None:
    """Spawn 3 tasks — every one returns cleanly, TaskGroup exits without error."""
    async with TaskGroup() as tg:
        h1 = tg.start_soon(_worker, 1)
        h2 = tg.start_soon(_worker, 2)
        h3 = tg.start_soon(_worker, 3)

    assert await h1 == 1
    assert await h2 == 2
    assert await h3 == 3
    assert h1.status == _TaskStatus.FINISHED
    assert h2.status == _TaskStatus.FINISHED
    assert h3.status == _TaskStatus.FINISHED


# ---------------------------------------------------------------------------
# Test 2: one failure cancels siblings, single exception re-raised
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_fails_cancels_others() -> None:
    """Task 0 raises ValueError; tasks 1 & 2 are cancelled by the scope.

    Only the single ValueError is re-raised (not wrapped in a group).
    """
    h1: TaskHandle | None = None
    h2: TaskHandle | None = None

    with pytest.raises(ValueError, match="boom"):
        async with TaskGroup() as tg:
            h1 = tg.start_soon(_worker_fail, "boom")  # fails fast
            h2 = tg.start_soon(_worker_slow, "slow")  # gets cancelled

    # h1 has the ValueError.
    assert h1 is not None
    assert h1.status == _TaskStatus.FINISHED
    assert isinstance(h1.exception, ValueError)

    # h2 should have been cancelled by the sibling-cancel.
    assert h2 is not None
    assert h2.status == _TaskStatus.FINISHED
    assert isinstance(h2.exception, asyncio.CancelledError)


# ---------------------------------------------------------------------------
# Test 3: multiple independent failures → BaseExceptionGroup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_fail_aggregate() -> None:
    """Two tasks raise different exceptions concurrently.

    Both exceptions appear in a BaseExceptionGroup.
    """
    with pytest.raises(BaseExceptionGroup, match="taskgroup crashed") as exc_info:
        async with TaskGroup() as tg:
            tg.start_soon(_worker_fail, "e1")  # both fire ~simultaneously
            tg.start_soon(_worker_fail, "e2")

    eg = exc_info.value
    exceptions = eg.exceptions
    assert len(exceptions) == 2
    msgs = {str(e) for e in exceptions}
    assert msgs == {"e1", "e2"}


# ---------------------------------------------------------------------------
# Test 4: TaskGroup.start() blocks until task_status.started()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_with_task_status() -> None:
    """``tg.start()`` does not return before the spawned task calls
    ``task_status.started()``."""

    started_flag: bool = False

    async def child(ts: TaskStatus) -> str:
        nonlocal started_flag
        # Simulate expensive initialisation.
        await asyncio.sleep(0.05)
        ts.started()
        started_flag = True
        await asyncio.sleep(0.02)
        return "done"

    async with TaskGroup() as tg:
        handle = await tg.start(child)
        # By the time start() returns, started() has been called.
        assert started_flag is True
        assert handle.status == _TaskStatus.STARTED

    assert handle.status == _TaskStatus.FINISHED
    assert handle.result == "done"


# ---------------------------------------------------------------------------
# Test 5: CancelledError from sibling-cancel is filtered
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_children_filtered() -> None:
    """When the scope cancels siblings, their CancelledError does NOT appear
    in the exception group (only the originating error does)."""

    h_slow: TaskHandle | None = None

    with pytest.raises(ValueError, match="original"):
        async with TaskGroup() as tg:
            h_slow = tg.start_soon(_worker_slow, "ok", 0.1)  # will get cancelled
            tg.start_soon(_worker_fail, "original")  # triggers sibling-cancel

    # The slow task was cancelled by sibling-cancel — that CancelledError
    # is filtered and should NOT be in the raised exception.
    assert h_slow is not None
    assert isinstance(h_slow.exception, asyncio.CancelledError)


# ---------------------------------------------------------------------------
# Test 6: TaskHandle.result / .exception raise RuntimeError when unfinished
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_properties_raise_before_done() -> None:
    """Accessing .result or .exception before the task finishes raises
    RuntimeError."""
    handle: TaskHandle | None = None

    async with TaskGroup() as tg:
        handle = tg.start_soon(_worker_slow, 42, 0.05)
        with pytest.raises(RuntimeError, match="not finished"):
            _ = handle.result
        with pytest.raises(RuntimeError, match="not finished"):
            _ = handle.exception

    assert handle.result == 42
    assert handle.exception is None


# ---------------------------------------------------------------------------
# TaskGroup shield + start crash (Wave 2 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_taskgroup_start_child_crash_cleans_up():
    """TaskGroup.start() cancels siblings when the spawned child crashes after init.

    Post-fix: when a child started via tg.start() crashes after calling
    task_status.started(), the sibling-cancel mechanism in _wait_children
    cancels remaining children and propagates the exception.
    """
    cleanup_called = False

    async def child_crashes_after_init(ts: TaskStatus) -> None:
        ts.started()
        await asyncio.sleep(0.02)
        raise ValueError("post-init crash")

    async def innocent_child() -> int:
        nonlocal cleanup_called
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cleanup_called = True
            raise
        return 0

    with pytest.raises(ValueError, match="post-init crash"):
        async with TaskGroup() as tg:
            tg.start_soon(innocent_child)
            await tg.start(child_crashes_after_init)

    assert cleanup_called, "sibling child was not cancelled after start child crash"


# ---------------------------------------------------------------------------
# FIX-3 regression tests (BUG-3/9: structured-concurrency contract) — 2026-08-10
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_body_and_child_exceptions_both_visible() -> None:
    """BUG-3: a body exception must not be silently dropped when children fail."""

    async def child_fail() -> None:
        raise ValueError("child")

    with pytest.raises(BaseExceptionGroup) as ei:
        async with TaskGroup() as tg:
            tg.start_soon(child_fail)
            # Yield once so the child actually runs and fails before the
            # body raises — both exceptions must surface (BUG-3).
            await asyncio.sleep(0)
            raise KeyError("body")

    names = {type(e).__name__ for e in ei.value.exceptions}
    assert "KeyError" in names, f"body exception lost: {names}"
    assert "ValueError" in names, f"child exception lost: {names}"


@pytest.mark.asyncio
async def test_body_cancel_propagates_without_merge() -> None:
    """Cancellation wins: a cancelled body must propagate CancelledError, not a group."""

    async def stuck() -> None:
        await asyncio.Event().wait()

    async def run() -> None:
        async with TaskGroup() as tg:
            tg.start_soon(stuck)
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await asyncio.sleep(0.05)  # let the group body park
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_cancel_cancels_stuck_child() -> None:
    """BUG-9: cancelling a TaskGroup body must cancel stuck children so the
    group exits promptly instead of hanging forever."""

    async def stuck() -> None:
        await asyncio.Event().wait()

    async def run() -> None:
        async with TaskGroup() as tg:
            tg.start_soon(stuck)
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await asyncio.sleep(0.05)  # let the group body park
    task.cancel()
    # The external cancel must pass through (group exits promptly instead of
    # hanging). Pre-fix this times out: the group never exits.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_taskgroup_not_reusable() -> None:
    """TS-3: a crashed TaskGroup must raise RuntimeError on re-entry, not a
    confusing CancelledError."""

    async def boom() -> None:
        raise ValueError("child")

    with pytest.raises(ValueError):
        async with TaskGroup() as tg:
            tg.start_soon(boom)

    with pytest.raises(RuntimeError, match="reus"):
        async with tg:
            pass


@pytest.mark.asyncio
async def test_cancel_all_concurrent_start_soon() -> None:
    """B2: cancel_all from another thread must not race with start_soon
    (free-threaded: an unlocked iteration raises mid-loop)."""

    async def noop() -> None:
        await asyncio.sleep(0)

    tg = TaskGroup()
    stop = threading.Event()
    errors: list[BaseException] = []

    def hammer() -> None:
        try:
            for _ in range(500):
                tg.cancel_all()
        except BaseException as e:  # noqa: BLE001
            errors.append(e)
        finally:
            stop.set()

    t = threading.Thread(target=hammer)
    t.start()
    try:
        for _ in range(500):
            tg.start_soon(noop)
            await asyncio.sleep(0)
    finally:
        t.join(timeout=10)

    assert not errors, f"cancel_all raced with start_soon: {errors}"

    # Cleanup: cancel everything and let the children finish.
    tg.cancel_all()
    handles = list(tg._children)
    await asyncio.gather(*(h._task for h in handles), return_exceptions=True)


# ── U4 FIX-20: exit guard + re-entry cleanup ───────────────────────────────


@pytest.mark.asyncio
async def test_start_soon_after_exit_raises() -> None:
    """R3-FIX-20 (probe R3-C): start_soon() after the group exited must raise
    instead of silently spawning an orphan task that nobody waits for —
    and the orphan must be cancelled, not left running."""
    tg = TaskGroup()
    async with tg:
        pass
    with pytest.raises(RuntimeError, match="not active"):
        tg.start_soon(_worker, "x")
    await asyncio.sleep(0.01)
    assert not tg._children


@pytest.mark.asyncio
async def test_reenter_after_body_exception_clean() -> None:
    """R5 revision C: re-entering after a body-exception exit must start a fresh
    lifecycle — the old children (pre-cancelled by __aexit__) must not be
    re-collected, or their stale CancelledError resurfaces on the second
    exit (probe R5: the body-exception path leaves the scope uncancelled,
    so re-entry was admitted and then re-raised the stale error)."""
    tg = TaskGroup()

    async def boom() -> None:
        await asyncio.sleep(0.01)
        raise KeyError("child")

    with pytest.raises(KeyError, match="body"):
        async with tg:
            tg.start_soon(boom)
            raise KeyError("body")  # body fails first → pre-cancel path

    # Pre-fix: the second exit re-collected the cancelled child and raised
    # a stale CancelledError here.  Fixed: fresh lifecycle, clean exit.
    async with tg:
        pass


@pytest.mark.asyncio
async def test_reenter_after_normal_exit_ok() -> None:
    """FIX-20: normal exit → re-enter is a fresh lifecycle (children cleared,
    start_soon works again)."""
    tg = TaskGroup()
    async with tg:
        h = tg.start_soon(_worker, "a")
        assert await h == "a"
    async with tg:
        h = tg.start_soon(_worker, "b")
        assert await h == "b"


# ---------------------------------------------------------------------------
# FIX-C (R5 audit): start() must not hang when the child fails before started()
# ---------------------------------------------------------------------------


async def _child_fails_before_started(_ts: TaskStatus) -> None:
    raise ValueError("boom before started")


@pytest.mark.asyncio
async def test_start_child_failure_raises_not_hangs() -> None:
    """FIX-C: a child that raises before calling ``started()`` must surface
    its exception from ``start()`` — pre-fix ``start()`` blocked forever
    (the started-event is never set)."""
    with pytest.raises(ValueError, match="boom before started"):
        async with TaskGroup() as tg:
            await asyncio.wait_for(tg.start(_child_fails_before_started), timeout=2.0)


@pytest.mark.asyncio
async def test_start_child_failure_reported_once() -> None:
    """FIX-C: the same child exception must not be reported twice (start()
    raises it AND the group re-collects it into an ExceptionGroup)."""
    with pytest.raises(ValueError, match="boom before started") as excinfo:
        async with TaskGroup() as tg:
            await asyncio.wait_for(tg.start(_child_fails_before_started), timeout=2.0)
    assert not isinstance(excinfo.value, BaseExceptionGroup)


@pytest.mark.asyncio
async def test_start_child_exits_without_started_raises() -> None:
    """A child that exits normally without calling started() raises
    RuntimeError (trio/anyio parity)."""

    async def no_started(task_status: TaskStatus) -> None:
        return None

    with pytest.raises(RuntimeError, match="started"):
        async with TaskGroup() as tg:
            await tg.start(no_started)


# ---------------------------------------------------------------------------
# R8 Unit 1: cancelled children are not errors (trio/anyio parity)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_all_then_normal_exit_is_silent() -> None:
    """R8 Unit 1: cancel_all() victims must not be reported at group exit.

    Pre-fix the soft-exit branch raised the victims' CancelledErrors into
    the host task (probe R8-D): a public-API cancel_all() followed by a
    normal body exit failed the whole task with a spurious CE group.
    """

    async def child() -> None:
        await asyncio.sleep(30)

    async with TaskGroup() as tg:
        tg.start_soon(child)
        tg.start_soon(child)
        await asyncio.sleep(0.05)
        tg.cancel_all()
    # No raise: the group exits cleanly.


@pytest.mark.asyncio
async def test_start_failure_siblings_not_reported() -> None:
    """R8 Unit 1: siblings cancelled by start()'s failure path are silent.

    Pre-fix the sibling's CancelledError was collected (not in
    cancelled_by_scope) and the group exit raised it via the soft-exit
    branch while the real ValueError had already been delivered by
    start() (probe R8-B2)."""

    async def failing_child(task_status: TaskStatus) -> None:
        task_status.started()
        raise ValueError("boom after started")

    async def sibling() -> None:
        await asyncio.sleep(30)

    async with TaskGroup() as tg:
        tg.start_soon(sibling)
        with pytest.raises(ValueError, match="boom after started"):
            await tg.start(failing_child)
    # No raise at exit.


@pytest.mark.asyncio
async def test_externally_cancelled_child_is_silent() -> None:
    """R8 Unit 1: a child cancelled by task.cancel() is not an error.

    trio/anyio both absorb externally cancelled children (anyio filters
    every child CancelledError in task_done); gsyncio previously raised
    the CE out of the group and spuriously cancelled the host task
    (probe R8-A)."""

    async def child() -> None:
        await asyncio.sleep(30)

    async with TaskGroup() as tg:
        h = tg.start_soon(child)
        await asyncio.sleep(0.05)
        h._task.cancel()
    # No raise: external child cancellation is absorbed.


# ---------------------------------------------------------------------------
# R8 Unit 2: host cancellation waits for children before the group exits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_host_cancel_waits_for_children_to_finish() -> None:
    """R8 Unit 2: when the host is cancelled while the group waits, all
    children must be FINISHED before the block exits (trio/anyio parity).

    Pre-fix the host's except handler ran while the child's finally had
    not (probe R8-C2: gsyncio False vs anyio True) — the structured-
    concurrency guarantee was broken on the cancellation path."""

    child_finally_ran = asyncio.Event()

    async def slow_child() -> None:
        try:
            await asyncio.sleep(30)
        finally:
            child_finally_ran.set()

    saw_flag_inside_except: dict[str, bool] = {}

    async def host() -> None:
        try:
            async with TaskGroup() as tg:
                tg.start_soon(slow_child)
                await asyncio.sleep(0.05)  # let the child start
        except BaseException:
            saw_flag_inside_except["flag"] = child_finally_ran.is_set()
            raise

    host_task = asyncio.create_task(host())
    await asyncio.sleep(0.1)  # host is now blocked waiting on the child
    host_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await host_task
    assert saw_flag_inside_except["flag"] is True
    assert child_finally_ran.is_set()


# ---------------------------------------------------------------------------
# R8 Unit 3: children spawned before the first entry stay tracked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preexisting_children_tracked_through_first_entry() -> None:
    """R8 Unit 3: children spawned BEFORE the first entry stay tracked —
    the group waits for them at exit (probe R8-E: previously the entry
    cleared _children, silently orphaning the task)."""

    finished = asyncio.Event()

    async def child() -> None:
        await asyncio.sleep(0.1)
        finished.set()

    tg = TaskGroup()
    h = tg.start_soon(child)
    async with tg:
        await asyncio.sleep(0.02)
    assert h._task.done()
    assert finished.is_set()


# ---------------------------------------------------------------------------
# R10 P2: start() cancellation semantics (trio parity)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_child_self_cancelled_after_started_returns_handle() -> None:
    """R10 P2: a child that raises CancelledError right after started() must
    not make start() itself raise — trio parity: start() completes once
    started() is called; the failure surfaces via the group's soft exit.
    Pre-fix: task.exception() raised CancelledError from start()'s finally."""
    started_flag = asyncio.Event()

    async def worker(task_status: TaskStatus) -> None:
        task_status.started()
        started_flag.set()
        raise asyncio.CancelledError()

    handle_result: TaskHandle | None = None
    with pytest.raises(asyncio.CancelledError):
        async with TaskGroup() as tg:
            handle_result = await tg.start(worker)
            await started_flag.wait()
    # Pre-fix the start() call raised and handle_result stayed None.
    assert handle_result is not None


@pytest.mark.asyncio
async def test_start_child_cancelled_before_started_cancels_siblings() -> None:
    """R10 P2: when the child is cancelled before started(), start()
    propagates the cancellation AND cancels siblings.  Pre-fix: the
    finally's task.exception() raised before the consume/sibling-cancel
    code ran, orphaning the siblings (they were never cancelled)."""
    child_task: asyncio.Task[Any] | None = None
    sibling_cancelled = asyncio.Event()

    async def worker(task_status: TaskStatus) -> None:
        nonlocal child_task
        child_task = asyncio.current_task()
        await asyncio.sleep(10)  # never calls started()

    async def sibling() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    async with TaskGroup() as tg:
        # Fire-and-forget: awaiting the returned handle would block until
        # the 10s sibling finishes (TaskHandle.__await__ delegates to the
        # underlying task), which would defeat the cancellation check below.
        tg.start_soon(sibling)
        start_task = asyncio.create_task(tg.start(worker))
        while child_task is None:
            await asyncio.sleep(0)
        child_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await start_task
        await asyncio.wait_for(sibling_cancelled.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_child_failure_preserves_external_cross_thread_cancel() -> None:
    """R10 P5: a synchronous cross-thread task.cancel() landing during
    child-failure handling must still be delivered after the group exits.
    Pre-fix: cancel_siblings' uncancel plus the __aexit__ compensation
    consumed the count, and the host never saw its cancellation."""
    import threading
    import time

    host_task: asyncio.Task[Any] | None = None
    child_parked = asyncio.Event()
    release_child = asyncio.Event()
    cancel_done = threading.Event()

    async def failing_child() -> None:
        child_parked.set()
        await release_child.wait()
        raise ValueError("boom")

    async def run_group() -> None:
        nonlocal host_task
        host_task = asyncio.current_task()
        try:
            async with TaskGroup() as tg:
                await tg.start_soon(failing_child)
        except BaseExceptionGroup:
            pass
        await asyncio.sleep(5)  # the external cancel must fire here

    def _cancel_from_thread() -> None:
        # Task.cancel() from a foreign thread is synchronous: it can land
        # in the middle of the failure-handling sync section.
        while host_task is None:
            time.sleep(0.001)
        host_task.cancel()
        cancel_done.set()

    t = asyncio.create_task(run_group())
    threading.Thread(target=_cancel_from_thread, daemon=True).start()
    await child_parked.wait()
    await asyncio.to_thread(cancel_done.wait)
    release_child.set()
    # Either interleaving must end with the host's cancellation delivered:
    # (a) the cancel landed while the host was waiting on the child — the
    #     CancelledError path cancels and drains children, then propagates;
    # (b) the cancel landed during failure handling — the R10 P5 fix keeps
    #     the count alive so the CE fires at the next await after the
    #     group exits.  Pre-fix (b) swallowed the count and the task kept
    #     sleeping past the timeout.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(t), timeout=2.0)


@pytest.mark.asyncio
async def test_taskgroup_cross_loop_rejection() -> None:
    """TaskGroup must reject start_soon and start when called from a foreign loop or thread."""
    async with TaskGroup() as tg:
        # Create a separate thread with a new event loop
        def foreign_worker(errs: list[Exception]) -> None:
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)

            async def attempt():
                async def dummy():
                    pass

                # start_soon from foreign loop must raise RuntimeError
                try:
                    tg.start_soon(dummy)
                except RuntimeError as exc:
                    errs.append(exc)

            try:
                new_loop.run_until_complete(attempt())
            finally:
                new_loop.close()

        errors: list[Exception] = []
        t = threading.Thread(target=foreign_worker, args=(errors,))
        t.start()
        t.join(timeout=2.0)

        assert len(errors) == 1
        assert "physically scoped to a single event loop" in str(errors[0])
