"""Tests for gsyncio.Lock and gsyncio.Event cross-event-loop-safe primitives."""

import asyncio
import threading

import pytest

from gsyncio import Event, Lock
from gsyncio.testing import wait_all_tasks_blocked

# ---------------------------------------------------------------------------
# Lock tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_acquire_release():
    """Basic acquire/release cycle — lock becomes free after release."""
    lock = Lock()
    assert not lock.locked
    assert lock.owner is None

    await lock.acquire()
    assert lock.locked
    assert lock.owner is asyncio.current_task()

    lock.release()
    assert not lock.locked
    assert lock.owner is None


@pytest.mark.asyncio
async def test_lock_async_context_manager():
    """Lock works as an async context manager."""
    lock = Lock()
    assert not lock.locked

    async with lock:
        assert lock.locked
        assert lock.owner is asyncio.current_task()

    assert not lock.locked


@pytest.mark.asyncio
async def test_lock_release_unowned():
    """Releasing a lock from a non-owning task raises RuntimeError."""
    lock = Lock()

    async def holder():
        async with lock:
            pass

    await holder()

    async def thief():
        with pytest.raises(RuntimeError, match="does not own"):
            lock.release()

    await thief()


@pytest.mark.asyncio
async def test_lock_fair_fifo():
    """Three tasks acquire in FIFO order."""
    lock = Lock()
    order: list[int] = []

    async def worker(n: int) -> None:
        await lock.acquire()
        order.append(n)
        lock.release()

    # Hold the lock so all workers queue up.
    await lock.acquire()
    t1 = asyncio.create_task(worker(1))
    t2 = asyncio.create_task(worker(2))
    t3 = asyncio.create_task(worker(3))

    # Let them all reach the acquire point.
    await wait_all_tasks_blocked()

    lock.release()  # hand off to first waiter

    await asyncio.gather(t1, t2, t3)

    assert order == [1, 2, 3]


@pytest.mark.asyncio
async def test_lock_cancel_while_waiting():
    """Cancelling a waiting task removes it from the waiter queue."""
    lock = Lock()

    await lock.acquire()

    async def waiter():
        await lock.acquire()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)  # let waiter queue up
    assert lock.locked

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Release — should not hand off to the cancelled task.
    lock.release()
    assert not lock.locked


@pytest.mark.asyncio
async def test_lock_owner_death_breaker():
    """If owner task crashes, next acquirer seizes the lock."""
    lock = Lock()

    async def crash_owner():
        await lock.acquire()
        # Simulate a crash: the coroutine raises, task becomes done.
        raise RuntimeError("boom")

    task = asyncio.create_task(crash_owner())
    with pytest.raises(RuntimeError, match="boom"):
        await task

    # The lock's _owner still points to the crashed (done) task.
    # Next acquire should trigger the owner-death breaker.
    await lock.acquire()
    assert lock.locked
    assert lock.owner is asyncio.current_task()
    lock.release()


@pytest.mark.asyncio
async def test_lock_cross_loop():
    """Release from main loop wakes a waiter running in a different event loop."""
    lock = Lock()
    got_lock: list[str] = []

    await lock.acquire()
    assert lock.locked

    def waiter_in_other_loop():
        loop = asyncio.new_event_loop()

        async def waiter():
            await lock.acquire()
            got_lock.append("acquired")
            lock.release()

        loop.run_until_complete(waiter())
        loop.close()

    thread = threading.Thread(target=waiter_in_other_loop)
    thread.start()

    # Let the waiter queue up.
    await asyncio.sleep(0.1)

    # Release from main loop — must wake the waiter in the other thread.
    lock.release()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert got_lock == ["acquired"]
    assert not lock.locked


# ---------------------------------------------------------------------------
# Event tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_event_set_before_wait():
    """set() before wait() — wait() returns immediately."""
    event = Event()
    assert not event.is_set()

    event.set()
    assert event.is_set()

    # Should return without blocking.
    await event.wait()


@pytest.mark.asyncio
async def test_event_wait_then_set():
    """A waiter wakes up after set() is called."""
    event = Event()

    async def waiter():
        await event.wait()
        return "woken"

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)

    event.set()
    result = await task
    assert result == "woken"
    assert event.is_set()


@pytest.mark.asyncio
async def test_event_multiple_waiters():
    """All waiters wake when set() is called."""
    event = Event()
    results: list[str] = []

    async def waiter(i: int):
        await event.wait()
        results.append(f"w{i}")

    tasks = [asyncio.create_task(waiter(i)) for i in range(5)]
    await asyncio.sleep(0.02)

    event.set()
    await asyncio.gather(*tasks)

    assert len(results) == 5
    assert event.is_set()


@pytest.mark.asyncio
async def test_event_idempotent_set():
    """Calling set() multiple times is harmless."""
    event = Event()
    event.set()
    event.set()
    event.set()
    assert event.is_set()
    await event.wait()


@pytest.mark.asyncio
async def test_event_cross_thread():
    """set() from a non-asyncio thread wakes the waiter."""
    event = Event()

    async def waiter():
        await event.wait()
        return "done"

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)

    def set_from_thread():
        event.set()

    thread = threading.Thread(target=set_from_thread)
    thread.start()
    thread.join()

    result = await asyncio.wait_for(task, timeout=5)
    assert result == "done"


# ---------------------------------------------------------------------------
# Event.wait cancel cleanup (Wave 0 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_event_wait_cancel_cleanup():
    """Cancelling an Event.wait() caller removes its entry from _waiters.

    Post-fix: the internal _waiters list must be cleaned up so subsequent
    set() does not try to wake a dead waiter loop/event pair.
    """
    event = Event()

    async def waiter():
        await event.wait()

    task = asyncio.create_task(waiter())
    await wait_all_tasks_blocked()
    assert len(event._waiters) == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Waiter must be removed after cancellation.
    assert len(event._waiters) == 0

    # Subsequent set() and wait() must still work correctly.
    event.set()
    await event.wait()


# ---------------------------------------------------------------------------
# Combined / stress test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_event_combined():
    """Use Lock + Event together in a producer/consumer pattern."""
    lock = Lock()
    ready = Event()
    shared: list[int] = []

    async def producer():
        async with lock:
            shared.append(42)
        ready.set()

    async def consumer():
        await ready.wait()
        async with lock:
            val = shared[0]
        return val

    prod = asyncio.create_task(producer())
    cons = asyncio.create_task(consumer())

    results = await asyncio.gather(prod, cons)
    assert results[-1] == 42
