"""Tests for gsyncio.Condition and gsyncio.Barrier primitives."""

import asyncio
import threading

import pytest

from gsyncio import Barrier, Condition, Lock
from gsyncio.testing import wait_all_tasks_blocked

# ---------------------------------------------------------------------------
# Condition tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_condition_wait_notify():
    """A waiter wakes after notify and holds the lock on resume."""
    cond = Condition()
    shared: list[int] = []

    async def consumer() -> None:
        async with cond:
            await cond.wait()
            shared.append(1)

    task = asyncio.create_task(consumer())
    await wait_all_tasks_blocked()

    # The consumer should be waiting; lock is free.
    cond.notify()
    await asyncio.wait_for(task, timeout=5)
    assert shared == [1]


@pytest.mark.asyncio
async def test_condition_wait_predicate_loop():
    """Classic wait-while-not-ready pattern with predicate loop."""
    cond = Condition()
    ready = False
    shared_value = 0

    async def consumer() -> int:
        async with cond:
            while not ready:
                await cond.wait()
            return shared_value

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)

    async with cond:
        ready = True
        shared_value = 42
        cond.notify()

    result = await asyncio.wait_for(task, timeout=5)
    assert result == 42


@pytest.mark.asyncio
async def test_condition_notify_all():
    """3 waiters all resume after notify_all."""
    cond = Condition()
    results: list[int] = []

    async def waiter(i: int) -> None:
        async with cond:
            await cond.wait()
            results.append(i)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.05)

    cond.notify_all()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
    assert len(results) == 3
    assert sorted(results) == [0, 1, 2]


@pytest.mark.asyncio
async def test_condition_cancel_while_waiting():
    """Cancelled waiter is removed from queue; lock is re-acquired."""
    cond = Condition()
    order: list[str] = []

    async def waiter() -> None:
        async with cond:
            await cond.wait()
            order.append("resumed")

    async def holder() -> None:
        async with cond:
            await asyncio.sleep(0.1)
            cond.notify()
            order.append("notified")

    # First, have one task enter wait
    async with cond:
        t1 = asyncio.create_task(waiter())
        await asyncio.sleep(0.05)

        # Cancel the first waiter while it's waiting
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1

        # Now the lock should be held again (re-acquired after cancel)
        # Hold and release, then a new waiter should work fine
        t2 = asyncio.create_task(waiter())

    # Release lock — t2 should acquire and wait
    await asyncio.sleep(0.05)

    # Notify — only t2 should resume
    cond.notify()
    await asyncio.wait_for(t2, timeout=5)
    assert "resumed" in order


@pytest.mark.asyncio
async def test_condition_custom_lock():
    """Condition accepts an external Lock."""
    lock = Lock()
    cond = Condition(lock=lock)
    assert cond._lock is lock

    acquired = False

    async def worker() -> None:
        nonlocal acquired
        await cond.acquire()
        acquired = True
        cond.release()

    await worker()
    assert acquired


@pytest.mark.asyncio
async def test_condition_context_manager():
    """Condition.__aenter__ / __aexit__ work correctly."""
    cond = Condition()
    async with cond:
        assert cond._lock.locked
    assert not cond._lock.locked


# ---------------------------------------------------------------------------
# Barrier cross-loop wake (Wave 1 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_barrier_cross_loop_wake():
    """Barrier wakes waiters when the last party arrives from a different event loop.

    Post-fix: Barrier.wait() uses call_soon_threadsafe to set the waiter
    events, which works correctly when the last waiter is in a different
    event loop than the waiting parties.
    """
    barrier = Barrier(parties=2)
    results: list[bool] = []

    def waiter_in_thread():
        loop = asyncio.new_event_loop()

        async def _wait():
            r = await barrier.wait()
            results.append(r.fulfilled)

        loop.run_until_complete(_wait())
        loop.close()

    thread = threading.Thread(target=waiter_in_thread)
    thread.start()

    # Allow the cross-loop waiter to register
    await asyncio.sleep(0.1)
    assert barrier.n_waiting == 1

    # Last party arrives from our loop — triggers cross-loop wake
    await barrier.wait()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert results == [True]
    assert barrier.n_waiting == 0


# ---------------------------------------------------------------------------
# Barrier tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_barrier_wait_two_parties():
    """Two parties wait and both unblock."""
    barrier = Barrier(parties=2)
    results: list[int] = []

    async def party(i: int) -> None:
        r = await barrier.wait()
        assert r.fulfilled
        assert r.parties == 2
        results.append(i)

    await asyncio.wait_for(asyncio.gather(party(1), party(2)), timeout=5)
    assert sorted(results) == [1, 2]


@pytest.mark.asyncio
async def test_barrier_multiple_rounds():
    """Barrier can be reused across multiple rounds."""
    barrier = Barrier(parties=2)
    round_results: list[list[int]] = []

    async def party(i: int) -> None:
        for round_num in range(3):
            r = await barrier.wait()
            assert r.fulfilled
            assert r.parties == 2
            round_results.append(round_num)

    await asyncio.wait_for(asyncio.gather(party(1), party(2)), timeout=5)
    assert len(round_results) == 6


@pytest.mark.asyncio
async def test_barrier_n_waiting():
    """n_waiting reflects current waiters."""
    barrier = Barrier(parties=3)
    assert barrier.n_waiting == 0
    assert barrier.parties == 3

    done = asyncio.Event()

    async def waiter() -> None:
        await barrier.wait()
        done.set()

    t1 = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert barrier.n_waiting == 1

    t2 = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert barrier.n_waiting == 2

    # Third party triggers release
    t3 = asyncio.create_task(waiter())
    await asyncio.wait_for(done.wait(), timeout=5)

    await asyncio.gather(t1, t2, t3)
    assert barrier.n_waiting == 0


@pytest.mark.asyncio
async def test_barrier_abort():
    """Abort raises RuntimeError in waiting tasks."""
    barrier = Barrier(parties=3)

    errors: list[type[Exception]] = []

    async def waiter() -> None:
        try:
            await barrier.wait()
        except BaseException as exc:
            errors.append(type(exc))

    t1 = asyncio.create_task(waiter())
    t2 = asyncio.create_task(waiter())
    await wait_all_tasks_blocked()

    barrier.abort()
    await asyncio.wait_for(asyncio.gather(t1, t2), timeout=5)
    assert len(errors) == 2
    assert all(e is RuntimeError for e in errors)

    # Subsequent wait() calls also raise
    with pytest.raises(RuntimeError, match="aborted"):
        await barrier.wait()


@pytest.mark.asyncio
async def test_barrier_invalid_parties():
    """parties <= 0 raises ValueError."""
    with pytest.raises(ValueError):
        Barrier(parties=0)
    with pytest.raises(ValueError):
        Barrier(parties=-1)


@pytest.mark.asyncio
async def test_condition_barrier_combined():
    """Condition + Barrier used together: producer/consumer with barrier sync."""
    barrier = Barrier(parties=2)
    cond = Condition()
    consumer_ready = asyncio.Event()
    shared: list[int] = []

    async def producer() -> None:
        # Wait for consumer to be ready before proceeding.
        await consumer_ready.wait()
        async with cond:
            shared.append(42)
            cond.notify()
        await barrier.wait()

    async def consumer() -> None:
        async with cond:
            consumer_ready.set()
            await cond.wait()
            val = shared[-1]
        assert val == 42
        await barrier.wait()

    t1 = asyncio.create_task(producer())
    t2 = asyncio.create_task(consumer())
    await asyncio.wait_for(asyncio.gather(t1, t2), timeout=5)
    assert shared == [42]
