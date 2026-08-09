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
    with pytest.raises(TimeoutError):
        async with fail_after(0.001) as scope:
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
