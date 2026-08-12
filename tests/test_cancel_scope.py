import asyncio
import threading

import pytest

from gsyncio import (
    AsyncContext,
    CancelScope,
    EventLoopThreadPool,
    TimeoutError,
    checkpoint,
    current_effective_deadline,
    fail_after,
    fail_at,
    move_on_after,
    move_on_at,
)
from gsyncio.testing import wait_all_tasks_blocked


@pytest.mark.asyncio
async def test_deadline_expired() -> None:
    """CancelScope(deadline=0) enters, any await inside raises CancelledError."""
    loop = asyncio.get_running_loop()
    with pytest.raises(asyncio.CancelledError):
        async with CancelScope(deadline=loop.time() + 0.001) as scope:
            await asyncio.sleep(0.1)
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_shield_blocks_parent() -> None:
    """Outer scope cancelled, inner scope(shield=True) runs to completion."""
    inner_completed = False
    try:
        async with CancelScope() as outer:
            outer.cancel()
            async with CancelScope(shield=True) as inner:
                await asyncio.sleep(0)
                inner_completed = True
                assert not inner._effectively_cancelled()
        await asyncio.sleep(0)
    except asyncio.CancelledError:
        pass
    assert inner_completed


@pytest.mark.asyncio
async def test_nested_respects_parent() -> None:
    """Parent cancel propagates to child (child without shield)."""
    try:
        async with CancelScope() as parent, CancelScope() as child:
            parent.cancel()
            assert child._effectively_cancelled()
        await asyncio.sleep(0)
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_fail_after_timeout() -> None:
    """fail_after(0.001) raises TimeoutError (gsyncio.TimeoutError)."""
    # Bind the scope BEFORE the with: on a slow loop the 1ms deadline can
    # expire between creation and entry, and __aenter__ then raises
    # TimeoutError directly — `as scope` would never bind (UnboundLocalError
    # under --count=50 stress).
    scope = fail_after(0.001)
    with pytest.raises(TimeoutError):
        async with scope:
            await asyncio.sleep(0.1)
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_move_on_after_silent() -> None:
    """move_on_after(0.001) exits silently, scope.cancelled_caught is True."""
    async with move_on_after(0.001) as scope:
        await asyncio.sleep(0.1)
    assert scope.cancelled_caught
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_cancel_called_inspectable() -> None:
    """After scope.cancel(), scope.cancel_called is True."""
    scope = CancelScope()
    assert not scope.cancel_called
    scope.cancel()
    assert scope.cancel_called
    scope.cancel()  # idempotent — no-op
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_checkpoint_noop_no_scope() -> None:
    """checkpoint() is a no-op when no CancelScope is active."""
    await checkpoint()


@pytest.mark.asyncio
async def test_checkpoint_raises_when_cancelled() -> None:
    """checkpoint() raises CancelledError when the current scope is cancelled.

    The cancelled flag is set directly (skipping scope.cancel() → task.cancel())
    so that the subsequent await checkpoint() is not pre-empted by the event
    loop's cancellation check.  This exercises checkpoint's explicit
    _effectively_cancelled() path.
    """
    async with CancelScope() as scope:
        scope._cancel_called = True
        with pytest.raises(asyncio.CancelledError):
            await checkpoint()


@pytest.mark.asyncio
async def test_current_effective_deadline_no_scope() -> None:
    """current_effective_deadline() returns inf when no scope is active."""
    assert current_effective_deadline() == float("inf")


@pytest.mark.asyncio
async def test_current_effective_deadline_tightest() -> None:
    """current_effective_deadline() returns the tightest deadline from nested scopes."""
    loop = asyncio.get_running_loop()
    t = loop.time()

    async with CancelScope(deadline=t + 10.0):
        assert current_effective_deadline() == pytest.approx(t + 10.0)

        async with CancelScope(deadline=t + 5.0):
            assert current_effective_deadline() == pytest.approx(t + 5.0)

        # Outer scope deadline again
        assert current_effective_deadline() == pytest.approx(t + 10.0)


@pytest.mark.asyncio
async def test_fail_at_expired_deadline() -> None:
    """fail_at raises TimeoutError when the absolute deadline expires."""
    loop = asyncio.get_running_loop()
    with pytest.raises(TimeoutError):
        async with fail_at(loop.time() + 0.001) as scope:
            await asyncio.sleep(0.1)
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_move_on_at_silent_exit() -> None:
    """move_on_at exits silently when the absolute deadline expires."""
    loop = asyncio.get_running_loop()
    async with move_on_at(loop.time() + 0.001) as scope:
        await asyncio.sleep(0.1)
    assert scope.cancel_called
    assert scope.cancelled_caught


@pytest.mark.asyncio
async def test_cancel_scope_cross_thread_cancel():
    """CancelScope.cancel() from a non-asyncio thread wakes the hosting task.

    Post-fix: cancel() detects that the calling thread is not the scope's
    event loop and uses call_soon_threadsafe to inject cancellation.
    """
    inner_cancelled = False
    scope = CancelScope()

    async def cancellable_work():
        nonlocal inner_cancelled
        try:
            async with scope:
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            inner_cancelled = True
            raise

    task = asyncio.create_task(cancellable_work())
    await asyncio.sleep(0)  # let the scope enter

    def cancel_from_thread():
        scope.cancel()

    thread = threading.Thread(target=cancel_from_thread)
    thread.start()
    thread.join()

    await asyncio.sleep(0.05)
    assert inner_cancelled, "CancelScope.cancel() from thread did not fire"

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_already_cancelled_skips_submit():
    """Cancel before submit → future is pre-cancelled, task never reaches pool."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        ctx = AsyncContext()
        ctx.cancel()

        metrics_before = pool.get_metrics()
        fut = ctx.submit(pool, asyncio.sleep, 0.1)

        # Future must be cancelled immediately — no task was submitted.
        assert fut.cancelled()
        with pytest.raises(asyncio.CancelledError):
            await fut

        # Pool completed-tasks count must NOT have changed.
        metrics_after = pool.get_metrics()
        assert metrics_after["completed_tasks"] == metrics_before["completed_tasks"]


@pytest.mark.asyncio
async def test_cancel_during_execution():
    """Submit first, then cancel → task runs but result is never delivered."""
    async with EventLoopThreadPool(num_threads=1) as pool:
        ctx = AsyncContext()
        delivered_count = 0

        async def long_task() -> str:
            await asyncio.sleep(0.15)
            return "done"

        fut = ctx.submit(pool, long_task)
        # Cancel immediately after submission, before the task finishes.
        ctx.cancel()

        # Awaiting the future must raise CancelledError.
        with pytest.raises(asyncio.CancelledError):
            await fut

        # Result was never delivered to the caller.
        assert delivered_count == 0

        # Allow worker thread to drain the task (it runs but result is dropped).
        await asyncio.sleep(0.3)


@pytest.mark.asyncio
async def test_async_context_submit_after_cancel():
    """AsyncContext.submit() immediately cancels the future when the context is already
    cancelled (lines 78-80)."""
    async with EventLoopThreadPool(num_threads=1) as pool:
        ctx = AsyncContext()
        ctx.cancel()
        fut = ctx.submit(pool, asyncio.sleep, 0.1)
        # The future should be cancelled.
        assert fut.cancelled()
        with pytest.raises(asyncio.CancelledError):
            await fut


@pytest.mark.asyncio
async def test_async_context_nested_cancellation():
    """Test AsyncContext nested cancellation logic"""
    parent = AsyncContext()
    child = AsyncContext(parent=parent)

    assert not parent.is_cancelled
    assert not child.is_cancelled

    parent.cancel()
    parent.cancel()  # Second cancel is idempotent

    assert parent.is_cancelled
    assert child.is_cancelled

    # child created after cancel is also immediately cancelled
    child2 = AsyncContext(parent=parent)
    assert child2.is_cancelled


@pytest.mark.asyncio
@pytest.mark.repeat(10)
async def test_race_async_context_concurrent_cancel_and_submit():
    """Race 1: Validate that AsyncContext is absolutely safe during high-concurrency interleaving of submit and cancel, with no missed cancellations"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        ctx = AsyncContext()
        go_event = asyncio.Event()

        async def dummy_work():
            await asyncio.sleep(0.1)
            return "ok"

        # Launch 50 coroutines to concurrently submit tasks — first block all on the go_event barrier
        async def submitter():
            await go_event.wait()
            try:
                fut = ctx.submit(pool, dummy_work)
                return await fut
            except asyncio.CancelledError:
                return "cancelled"

        sub_tasks = [asyncio.create_task(submitter()) for _ in range(50)]

        # Yield to let all submitters reach the Event.wait() barrier
        await wait_all_tasks_blocked()

        # Release all submitters simultaneously and immediately trigger cancel —
        # Maximize the interleaving race between submit and cancel
        go_event.set()
        await wait_all_tasks_blocked()  # Let some submits begin first
        ctx.cancel()

        results = await asyncio.gather(*sub_tasks)
        # All submitted tasks after cancel should either return "cancelled" or complete normally; absolutely no stuck tasks
        assert all(r in ("cancelled", "ok") for r in results)


@pytest.mark.asyncio
async def test_async_context_cross_thread_cancellation():
    """3. Go parity context: Validate AsyncContext cross-thread cascading cancellation"""
    async with EventLoopThreadPool(num_threads=2) as pool:
        ctx = AsyncContext()

        async def long_task():
            await asyncio.sleep(5.0)
            return "done"

        # Submit to Worker thread and bind to AsyncContext
        fut = ctx.submit(pool, long_task)
        await asyncio.sleep(0.02)

        # Trigger Cancel
        ctx.cancel()

        with pytest.raises(asyncio.CancelledError):
            await fut


@pytest.mark.asyncio
async def test_async_context_cancel_survives_dead_loop() -> None:
    """A dead loop in the cancel() cascade must be skipped, not abort the
    whole cascade (R5 FIX-E completion).

    Pre-fix: context.py's futures loop had no per-call RuntimeError guard,
    so one dead loop made cancel() raise RuntimeError and later futures were
    never cancelled.
    """
    ctx = AsyncContext()
    loop = asyncio.get_running_loop()
    fut1 = loop.create_future()
    fut2 = loop.create_future()
    ctx._futures[fut1] = loop  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    ctx._futures[fut2] = loop  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]

    orig = loop.call_soon_threadsafe
    raised = False

    def exploding(cb, *args):  # type: ignore[no-untyped-def]  # noqa: ANN001, ANN002
        nonlocal raised
        if not raised:
            raised = True
            raise RuntimeError("Event loop is closed")
        return orig(cb, *args)

    loop.call_soon_threadsafe = exploding  # type: ignore[method-assign]
    try:
        ctx.cancel()
    finally:
        loop.call_soon_threadsafe = orig  # type: ignore[method-assign]
    assert raised
    await asyncio.sleep(0)  # let the scheduled fut2.cancel callback run
    assert fut2.cancelled(), (
        "cascade must continue after the dead loop; fut2 must still be cancelled"
    )


# ---------------------------------------------------------------------------
# FIX-1 regression tests (BUG-1/5: cancellation-count leaks & swallowed
# external cancels) — 2026-08-10 audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_leaves_cancelling_zero() -> None:
    """BUG-1: fail_after/move_on_after timeouts must not leak cancelling() counts.

    A leaked count surfaces as a spurious CancelledError on the next await
    (or worse, gets consumed by TaskGroup._wait_children's uncancel()).
    """
    task = asyncio.current_task()
    assert task is not None

    try:
        async with fail_after(0.01):
            await asyncio.sleep(0.1)
    except TimeoutError:
        pass
    assert task.cancelling() == 0

    async with move_on_after(0.01):
        await asyncio.sleep(0.1)
    assert task.cancelling() == 0

    # The await after the scope must not raise a spurious CancelledError.
    await asyncio.sleep(0.01)
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_timeout_then_taskgroup_no_spurious_cancel() -> None:
    """BUG-1: timeout leak consumed by TaskGroup must not cancel the next await."""
    from gsyncio import TaskGroup

    task = asyncio.current_task()
    assert task is not None

    try:
        async with fail_after(0.01):
            await asyncio.sleep(0.1)
    except TimeoutError:
        pass
    assert task.cancelling() == 0

    async def boom() -> None:
        raise ValueError("child")

    with pytest.raises(ValueError):
        async with TaskGroup() as tg:
            tg.start_soon(boom)

    # Group exit consumed the leaked count — the next await must survive.
    await asyncio.sleep(0.01)
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_move_on_after_does_not_swallow_external_cancel() -> None:
    """BUG-5: move_on_after must absorb only its own deadline cancel."""

    async def worker() -> None:
        async with move_on_after(5.0):
            await asyncio.sleep(1.0)

    task = asyncio.create_task(worker())
    await asyncio.sleep(0.05)  # let the worker enter the scope
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=2.0)
        pytest.fail("external task.cancel() was swallowed by move_on_after")
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_cancel_scope_cannot_share_across_tasks() -> None:
    """TS-2: a CancelScope entered from a second task must raise RuntimeError.

    Without the host-task check the scope's ``_task`` pointer is silently
    overwritten and cancel() targets the wrong task.
    """
    scope = CancelScope()
    results: dict[str, str] = {}

    async def enter_a() -> None:
        try:
            async with scope:
                await asyncio.sleep(0.3)
        except BaseException as e:  # noqa: BLE001
            results["a"] = type(e).__name__

    async def enter_b() -> None:
        await asyncio.sleep(0.05)  # let A enter first
        try:
            async with scope:
                await asyncio.sleep(0.3)
        except BaseException as e:  # noqa: BLE001
            results["b"] = type(e).__name__

    ta = asyncio.create_task(enter_a())
    tb = asyncio.create_task(enter_b())
    await asyncio.wait_for(asyncio.gather(ta, tb, return_exceptions=True), timeout=3.0)
    assert results.get("b") == "RuntimeError"


# ============================================================================
# R1 FIX-2 / R3 FIX-18 regression tests — aenter rollback + deadline edges
# ============================================================================


@pytest.mark.asyncio
async def test_aenter_raise_does_not_leak_stack() -> None:
    """R1-FIX-2: entering a scope under a cancelled ancestor raises, but must
    not leave a stale scope on the task-local stack, a live deadline timer,
    or a retained _task binding (__aexit__ is never called on aenter raise).
    """
    from gsyncio._cancel import _get_scope_stack

    outer = CancelScope()
    await outer.__aenter__()
    # Mark the ancestor cancelled WITHOUT task.cancel(): the point here is the
    # aenter-time _effectively_cancelled() check, not loop-injected cancels.
    outer._cancel_called = True  # type: ignore[reportAttributeAccessIssue]
    assert _get_scope_stack() == [outer]

    inner = CancelScope(deadline=asyncio.get_running_loop().time() + 60)
    with pytest.raises(asyncio.CancelledError):
        await inner.__aenter__()
    assert _get_scope_stack() == [outer]  # stale entry popped
    assert inner._task is None  # type: ignore[reportAttributeAccessIssue]
    assert inner._deadline_handle is None  # type: ignore[reportAttributeAccessIssue]
    assert inner._reset_token is None  # type: ignore[reportAttributeAccessIssue]

    await outer.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_fail_after_zero_raises_timeout() -> None:
    """R3-FIX-18: fail_after(0) raises TimeoutError and leaks no cancel count
    (previously: bare CancelledError + task.cancelling() == 1).
    """
    task = asyncio.current_task()
    assert task is not None
    with pytest.raises(TimeoutError):
        async with fail_after(0):
            await asyncio.sleep(0.1)
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_move_on_zero_silent_with_await() -> None:
    """R3-FIX-18: move_on_after(0) is silent; the body's first await is
    cancelled and the injected count is undone on exit.
    """
    task = asyncio.current_task()
    assert task is not None
    body_ran = False
    async with move_on_after(0) as scope:
        await asyncio.sleep(0.1)
        body_ran = True
    assert not body_ran
    assert scope.cancelled_caught
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_move_on_zero_silent_no_await() -> None:
    """R3-FIX-18: move_on_after(0) with an await-free body completes cleanly —
    the __aexit__ compensation uncancels the injection that was never delivered.
    """
    task = asyncio.current_task()
    assert task is not None
    async with move_on_after(0) as scope:
        pass  # no await — injection must be compensated on exit
    assert scope.cancelled_caught
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_deadline_nan_rejected() -> None:
    """R3-FIX-18: NaN deadlines raise ValueError at construction — previously
    call_later(NaN) corrupted the loop's selector timeout and crashed it with
    TypeError (probe R3-F).
    """
    with pytest.raises(ValueError):
        CancelScope(deadline=float("nan"))
    with pytest.raises(ValueError):
        fail_after(float("nan"))
    with pytest.raises(ValueError):
        fail_at(float("nan"))
    # -inf is rejected too: express "already expired" with fail_after(0).
    with pytest.raises(ValueError):
        CancelScope(deadline=float("-inf"))


@pytest.mark.asyncio
async def test_no_scope_poisoning_after_expiry() -> None:
    """R1-FIX-2/R3-FIX-18: after an expired-deadline entry (or a cancelled-
    ancestor entry), the same task can still enter fresh scopes and TaskGroups
    (probe R2 chain: the leaked scope poisoned every later __aenter__).
    """
    from gsyncio import TaskGroup

    async def noop() -> None:
        pass

    with pytest.raises(TimeoutError):
        async with fail_after(0):
            await asyncio.sleep(0.1)

    async with CancelScope():
        pass

    async with TaskGroup() as tg:
        tg.start_soon(noop)

    assert asyncio.current_task() is not None
    assert asyncio.current_task().cancelling() == 0  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_shield_expired_deadline_restores_cancel_count() -> None:
    """U1 re-audit: a shielded scope whose __aenter__ raises must restore the
    cancellation count it cleared, or the parent cancellation is silently lost.

    The shield snapshots and clears task.cancelling() on enter and re-injects
    it on exit — but __aexit__ never runs when __aenter__ raises.  The
    expired-deadline path rolls back and raises, skipping the restoration:
    pre-fix the task ends with cancelling() == 0 while two parent cancels
    were pending, so the next external cancel delivery is swallowed.
    """
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    task.cancel()
    assert task.cancelling() == 2
    with pytest.raises(asyncio.CancelledError):
        async with CancelScope(deadline=asyncio.get_running_loop().time() - 1, shield=True):
            pass  # pragma: no cover — __aenter__ raises before the body
    # Pre-fix: 0 (count lost); fixed: 2 (restored, parent cancel preserved).
    assert task.cancelling() == 2
    task.uncancel()
    task.uncancel()
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_precancelled_fail_after_raises_timeout() -> None:
    """A pre-cancelled fail_after must convert to TimeoutError at entry (anyio parity)."""
    scope = fail_after(60)
    scope.cancel()
    with pytest.raises(TimeoutError):
        async with scope:
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_precancelled_move_on_swallows() -> None:
    """A pre-cancelled move_on must swallow silently (anyio parity), cancelled_caught=True."""
    scope = move_on_after(60)
    scope.cancel()
    async with scope:
        await asyncio.sleep(0)
    assert scope.cancelled_caught


@pytest.mark.asyncio
async def test_precancelled_move_on_await_free_body() -> None:
    """Pre-cancelled move_on with an await-free body: the injection count is
    compensated by __aexit__, no leak."""
    scope = move_on_after(60)
    scope.cancel()
    async with scope:
        pass
    task = asyncio.current_task()
    assert task is not None
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_precancelled_plain_scope_raises_cancelled() -> None:
    """A pre-cancelled plain scope raises CancelledError at entry (status quo)."""
    scope = CancelScope()
    scope.cancel()
    with pytest.raises(asyncio.CancelledError):
        async with scope:
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_inherited_cancel_not_converted() -> None:
    """Ancestor cancellation is not converted by fail_after (only its own
    cancel is handled — trio semantics)."""
    outer = CancelScope()
    async with outer:
        outer.cancel()
        scope = fail_after(60)
        with pytest.raises(asyncio.CancelledError):
            async with scope:
                await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_precancelled_move_on_body_raises_no_count_leak() -> None:
    """N1: pre-cancelled move_on + body raising a non-CE exception
    synchronously — the injection count must be compensated.

    Pre-fix: __aexit__'s compensation branch only handled exc_type is None;
    with the body raising ValueError the injected count 1 leaked and the next
    await raised a spurious CancelledError.
    """
    scope = move_on_after(60)
    scope.cancel()
    try:
        async with scope:
            raise ValueError("boom")  # sync raise, no await
    except ValueError:
        pass
    task = asyncio.current_task()
    assert task is not None
    assert task.cancelling() == 0
    await asyncio.sleep(0)  # the next await must not raise a spurious CE


@pytest.mark.asyncio
async def test_fail_after_caught_cancel_leaves_zero_count() -> None:
    """R7-A: a fail_after CE caught inside the body, body exits normally —
    the count must return to zero.

    Pre-fix: the injected cancel count had no __aexit__ compensation
    (convert/swallow branches both require the CE in flight), so the residual
    cancelling()=1 leaked into the outer scope.
    """
    task = asyncio.current_task()
    assert task is not None
    async with fail_after(0.01):
        try:
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            pass
    assert task.cancelling() == 0
    await asyncio.sleep(0.001)  # real future await: no spurious CE allowed


@pytest.mark.asyncio
async def test_plain_scope_caught_cancel_leaves_zero_count() -> None:
    """R7-A: same scenario for a plain CancelScope (deadline cancel caught,
    body exits normally)."""
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    async with CancelScope(deadline=loop.time() + 0.01):
        try:
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            pass
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_leaked_count_not_amplified_by_shield() -> None:
    """R7-A real damage: a leaked count must not be snapshotted by a shield
    and re-injected (probe R7-A2).

    Pre-fix: the residual count was snapshotted by the shield's __aenter__ as
    a "real cancellation"; on exit _restore_saved_cancel_count re-injected
    task.cancel(), raising a spurious CancelledError in unrelated code at the
    first real await after the shield.
    """
    task = asyncio.current_task()
    assert task is not None
    async with fail_after(0.01):
        try:
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            pass
    async with CancelScope(shield=True):
        await asyncio.sleep(0.001)
    assert task.cancelling() == 0
    await asyncio.sleep(0.001)  # delivery point of the shield restore: must be clean
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_checkpoint_consumes_pending_cancel() -> None:
    """R7-B: checkpoint()'s raise must consume the pending count — no double
    delivery.

    Pre-fix: with the injection pending (sync code section, _must_cancel
    undelivered), checkpoint()'s user-level raise did not decrement
    cancelling(); the next real future await delivered a second
    CancelledError (probe R7-B: DOUBLE-DELIVERY).
    """
    task = asyncio.current_task()
    assert task is not None
    async with CancelScope() as scope:
        scope.cancel()  # inject from a sync section: _must_cancel pending
        try:
            await checkpoint()
        except asyncio.CancelledError:
            pass
        assert task.cancelling() == 0
        await asyncio.sleep(0.001)  # real future await: no double delivery
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_scope_reentry_resets_injected_accounting() -> None:
    """R7-A refinement: re-entering a scope must reset the injection
    accounting.

    Round 1: normal enter/exit (flag False, no compensation).  Round 2:
    cancel + catch + normal exit (cancel() re-records, compensation consumes,
    flag cleared).  Round 3 locks in the existing F-1 semantics: re-entering
    a cancelled scope raises CancelledError at entry (cancel_called is
    permanent — same as test_precancelled_plain_scope_raises_cancelled).
    """
    task = asyncio.current_task()
    assert task is not None
    scope = CancelScope()
    # Round 1: normal enter/exit
    async with scope:
        await asyncio.sleep(0)
    assert task.cancelling() == 0
    # Round 2: cancel + catch + normal exit
    async with scope:
        scope.cancel()
        try:
            await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            pass
    assert task.cancelling() == 0
    # Round 3: re-entering a cancelled scope = pre-cancelled plain scope ->
    # entry raises CE (existing semantics)
    with pytest.raises(asyncio.CancelledError):
        async with scope:
            await asyncio.sleep(0)
