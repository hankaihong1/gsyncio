"""Tests for multiloop.Lock and multiloop.Event cross-event-loop-safe primitives."""

import asyncio
import threading
from typing import Any

import pytest

from multiloop import Condition, Event, Lock
from multiloop.testing import wait_all_tasks_blocked

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


# ---------------------------------------------------------------------------
# Regression tests
# forward) — 2026-08-10 audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_handoff_to_cancelled_waiter_forwards() -> None:
    """releasing the lock onto a waiter that is then cancelled before
    it wakes must forward the lock to the next FIFO waiter instead of
    stranding ownership on a dead task."""
    lock = Lock()
    go = asyncio.Event()
    w3_got = asyncio.Event()
    results: list[str] = []

    async def w1() -> None:
        async with lock:
            await go.wait()

    async def w2() -> None:
        try:
            async with lock:
                results.append("w2")
        except asyncio.CancelledError:
            pass

    async def w3() -> None:
        async with lock:
            results.append("w3")
            w3_got.set()

    t1 = asyncio.create_task(w1())
    await asyncio.sleep(0.05)  # W1 holds the lock
    t2 = asyncio.create_task(w2())
    await asyncio.sleep(0.05)  # W2 queued
    t3 = asyncio.create_task(w3())
    await asyncio.sleep(0.05)  # W3 queued behind W2

    go.set()  # W1 releases: ownership handed to W2
    t2.cancel()  # ...but W2 is cancelled before its wakeup runs

    # Pre-fix this times out: W3 waits forever on a lock owned by a dead task.
    await asyncio.wait_for(w3_got.wait(), timeout=1.0)
    await asyncio.gather(t1, t2, t3, return_exceptions=True)
    assert results == ["w3"]


@pytest.mark.asyncio
async def test_condition_notify_consumed_by_cancelled_waiter_forwards() -> None:
    """W11: a notify consumed by a cancelled waiter must wake the next waiter."""
    cond = Condition()
    w3_got = asyncio.Event()
    results: list[str] = []

    async def waiter(name: str, got: asyncio.Event | None = None) -> None:
        async with cond:
            await cond.wait()
            results.append(name)
            if got is not None:
                got.set()

    t2 = asyncio.create_task(waiter("w2"))
    await asyncio.sleep(0.05)
    t3 = asyncio.create_task(waiter("w3", w3_got))
    await asyncio.sleep(0.05)

    cond.notify()  # pops W2's entry and wakes it
    t2.cancel()  # ...but W2 is cancelled before it re-acquires the lock

    # Pre-fix this times out: the notification is gone and W3 never wakes.
    await asyncio.wait_for(w3_got.wait(), timeout=1.0)
    await asyncio.gather(t2, t3, return_exceptions=True)
    assert results == ["w3"]


@pytest.mark.asyncio
async def test_wake_race_no_callback_noise() -> None:
    """W21: cancelling a waiter after it was popped must not spam the loop
    exception handler with InvalidStateError."""
    loop = asyncio.get_running_loop()
    errors: list[BaseException | None] = []
    old_handler = loop.get_exception_handler()

    def handler(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        errors.append(context.get("exception"))

    loop.set_exception_handler(handler)
    try:

        async def grab(lock: Lock, ev: asyncio.Event) -> None:
            try:
                async with lock:
                    ev.set()
            except asyncio.CancelledError:
                pass

        for _ in range(200):
            lock = Lock()
            held = asyncio.Event()
            t1 = asyncio.create_task(grab(lock, held))
            await asyncio.wait_for(held.wait(), timeout=1.0)  # t1 holds
            t2 = asyncio.create_task(grab(lock, asyncio.Event()))
            await asyncio.sleep(0)  # t2 queued
            rel = asyncio.create_task(_release_after(lock))
            await asyncio.sleep(0)  # release pops t2 and wakes it
            t2.cancel()  # race: woken entry cancelled mid-delivery
            await asyncio.gather(t1, t2, rel, return_exceptions=True)
    finally:
        loop.set_exception_handler(old_handler)

    assert not errors, f"wake race produced {len(errors)} callback error(s)"


async def _release_after(lock: Lock) -> None:
    await asyncio.sleep(0)
    lock.release()


# ---------------------------------------------------------------------------
# Regression tests
# — 2026-08-10 audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_writer_cancel_wakes_blocked_readers() -> None:
    """cancelling a pending writer must wake readers blocked on it.

    A cancelled writer decrements pending_writers without notifying _read_ok —
    blocked readers then wait forever on a condition that is already false.
    """
    from multiloop import AsyncRWMutex

    rw = AsyncRWMutex()
    reader_done = asyncio.Event()

    async def reader(done: asyncio.Event) -> None:
        async with rw.reader():
            done.set()

    async def writer_stuck() -> None:
        try:
            async with rw.writer():
                await asyncio.Event().wait()  # never exits on its own
        except asyncio.CancelledError:
            pass

    t1 = asyncio.create_task(reader(asyncio.Event()))
    await asyncio.sleep(0.05)  # R1 holds the read lock
    t2 = asyncio.create_task(writer_stuck())
    await asyncio.sleep(0.05)  # W1 pending (blocks new readers)
    t3 = asyncio.create_task(reader(reader_done))
    await asyncio.sleep(0.05)  # R2 blocked on _read_ok

    t2.cancel()  # W1 cancelled: pending_writers -> 0

    # Pre-fix this times out: R2 waits forever.
    await asyncio.wait_for(reader_done.wait(), timeout=1.0)
    await asyncio.gather(t1, t2, t3, return_exceptions=True)


# ---------------------------------------------------------------------------
# Regression test: wakeup loops must tolerate waiters whose loop was closed
# ---------------------------------------------------------------------------


def _register_on_abandoned_loop(coro_factory: Any) -> None:
    """Register a waiter on a fresh loop, then stop and close that loop
    WITHOUT cancelling the waiter — simulating an abandoned loop whose
    waiter entry is still registered."""
    loop = asyncio.new_event_loop()

    def _run() -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    asyncio.run_coroutine_threadsafe(coro_factory(), loop)
    # WHY: block until the coroutine's first step ran on loop_a (waiter
    # registered) — a bare call_soon sync point can race the loop's
    # iteration boundary and close the loop before the waiter registers,
    # silently turning the test into a no-op.
    asyncio.run_coroutine_threadsafe(asyncio.sleep(0.1), loop).result()
    loop.call_soon_threadsafe(loop.stop)
    t.join(2.0)
    loop.close()


@pytest.mark.asyncio
async def test_event_set_tolerates_closed_loop_waiter() -> None:
    """``Event.set()`` must wake live-loop waiters even when a stale
    waiter's loop was closed — pre-fix the first ``call_soon_threadsafe``
    raised RuntimeError and aborted the wakeup loop."""
    ev = Event()
    _register_on_abandoned_loop(lambda: ev.wait())

    t2 = asyncio.create_task(ev.wait())
    await asyncio.sleep(0)
    ev.set()  # must not raise
    await asyncio.wait_for(t2, timeout=1.0)


@pytest.mark.asyncio
async def test_lock_release_tolerates_closed_loop_waiter() -> None:
    """releasing a lock whose FIFO head lives on a closed loop must
    hand ownership to the next live waiter — pre-fix ``release()`` raised
    RuntimeError after transferring ownership to the dead task and the lock
    was permanently broken (every later acquire hung)."""
    lock = Lock()
    await lock.acquire()
    _register_on_abandoned_loop(lock.acquire)

    t2 = asyncio.create_task(lock.acquire())
    await asyncio.sleep(0)
    lock.release()  # must not raise; ownership must reach t2
    await asyncio.wait_for(t2, timeout=1.0)


@pytest.mark.asyncio
async def test_lock_base_exception_forwards_ownership() -> None:
    """Lock.acquire must forward ownership when a waiter is interrupted by BaseException."""
    lock = Lock()
    await lock.acquire()

    class CustomBaseException(BaseException):
        pass

    interrupted_task_started = asyncio.Event()

    async def interrupted_waiter() -> None:
        interrupted_task_started.set()
        await lock.acquire()

    t2 = asyncio.create_task(interrupted_waiter())

    t3_held = asyncio.Event()

    async def waiter3() -> None:
        await lock.acquire()
        t3_held.set()
        await asyncio.sleep(0.01)
        lock.release()

    t3 = asyncio.create_task(waiter3())
    await interrupted_task_started.wait()
    await wait_all_tasks_blocked()

    # Release lock: ownership is handed to t2 (popped from _waiters)
    lock.release()

    # Cancel t2 before it can finish acquiring
    t2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t2

    # Ownership must have been forwarded to t3
    await asyncio.wait_for(t3_held.wait(), timeout=1.0)
    await t3
    assert not lock.locked


@pytest.mark.asyncio
async def test_condition_wait_unacquired_owner_check() -> None:
    """Condition.wait called without acquiring the lock must raise RuntimeError immediately."""
    cond = Condition()
    with pytest.raises(RuntimeError, match="cannot wait on un-acquired lock"):
        await cond.wait()


# ---------------------------------------------------------------------------
# Concurrency audit & semantic refactoring tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_dead_owner_fifo_recovery() -> None:
    """Lock dead-owner recovery wakes FIFO waiters in order instead of barging."""
    lock = Lock()
    order: list[str] = []

    # 1. Owner acquires lock then dies without release
    async def doomed_owner() -> None:
        await lock.acquire()

    asyncio.create_task(doomed_owner())
    await asyncio.sleep(0.01)
    assert lock.locked

    # 2. Waiter 1 queues while owner is still alive
    async def waiter_1() -> None:
        await lock.acquire()
        order.append("waiter_1")
        lock.release()

    t_waiter = asyncio.create_task(waiter_1())
    await asyncio.sleep(0.01)

    # 3. New caller tries to acquire lock after owner dies
    async def new_caller() -> None:
        await lock.acquire()
        order.append("new_caller")
        lock.release()

    t_caller = asyncio.create_task(new_caller())
    await asyncio.gather(t_waiter, t_caller)

    assert order == ["waiter_1", "new_caller"], f"Expected FIFO order, got {order}"
