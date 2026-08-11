"""Tests for gsyncio.Semaphore and gsyncio.CapacityLimiter."""

import asyncio
import threading

import pytest

from gsyncio import CapacityLimiter, Semaphore
from gsyncio.testing import wait_all_tasks_blocked

# ---------------------------------------------------------------------------
# Semaphore tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_semaphore_acquire_release():
    """Acquire 2 of max 2, release 1, verify 3rd can acquire."""
    sem = Semaphore(2)
    assert sem.value == 2
    assert sem.max_value == 2

    await sem.acquire()
    assert sem.value == 1
    await sem.acquire()
    assert sem.value == 0

    sem.release()
    assert sem.value == 1

    await sem.acquire()
    assert sem.value == 0


@pytest.mark.asyncio
async def test_semaphore_cancel_acquire():
    """Cancelling a waiting acquire does not leak the token."""
    sem = Semaphore(1)
    await sem.acquire()
    assert sem.value == 0

    cancelled = False

    async def waiter():
        nonlocal cancelled
        try:
            await sem.acquire()
        except asyncio.CancelledError:
            cancelled = True
            raise

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)  # let waiter register
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled
    # Token was not stolen — semaphore still has 0 held tokens
    assert sem.value == 0

    sem.release()
    assert sem.value == 1


@pytest.mark.asyncio
async def test_semaphore_block():
    """3 concurrent acquires on max=2 — 3rd blocks until release."""
    sem = Semaphore(2)
    acquired = []

    async def worker(i: int):
        await sem.acquire()
        acquired.append(i)

    # Start 3 workers
    tasks = [asyncio.create_task(worker(i)) for i in range(3)]
    await asyncio.sleep(0)
    # First two should have acquired immediately
    assert len(acquired) == 2

    # Third is still blocked
    sem.release()
    await wait_all_tasks_blocked()
    assert len(acquired) == 3

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_semaphore_context_manager():
    """async with sem: works correctly."""
    sem = Semaphore(2)
    assert sem.value == 2

    async with sem:
        assert sem.value == 1

    assert sem.value == 2


@pytest.mark.asyncio
async def test_semaphore_fifo_order():
    """Waiters are served in FIFO order."""
    sem = Semaphore(1)
    await sem.acquire()  # exhaust

    order = []

    async def waiter(i: int):
        await sem.acquire()
        order.append(i)
        # Do NOT release — main test controls releases

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0)

    sem.release()  # first waiter (0) gets it
    await asyncio.sleep(0.05)
    assert order == [0]

    sem.release()  # second waiter (1)
    await asyncio.sleep(0.05)
    assert order == [0, 1]

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# CapacityLimiter tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capacity_limiter_acquire_release():
    """Basic acquire/release on CapacityLimiter."""
    limiter = CapacityLimiter(2.0)
    assert limiter.total_tokens == 2.0
    assert limiter.available_tokens == 2.0
    assert limiter.borrowed_tokens == 0.0

    await limiter.acquire()
    assert limiter.available_tokens == 1.0
    assert limiter.borrowed_tokens == 1.0

    await limiter.acquire()
    assert limiter.available_tokens == 0.0
    assert limiter.borrowed_tokens == 2.0

    limiter.release()
    assert limiter.available_tokens == 1.0


@pytest.mark.asyncio
async def test_capacity_limiter_dynamic_total():
    """Changing total_tokens allows new acquires."""
    limiter = CapacityLimiter(1.0)
    await limiter.acquire()
    assert limiter.available_tokens == 0.0

    # Increase budget — now another acquire should succeed
    limiter.total_tokens = 3.0
    assert limiter.total_tokens == 3.0
    assert limiter.available_tokens == 2.0  # 3 - 1 borrowed

    await limiter.acquire()
    assert limiter.available_tokens == 1.0
    assert limiter.borrowed_tokens == 2.0


@pytest.mark.asyncio
async def test_capacity_limiter_context_manager():
    """async with limiter: works."""
    limiter = CapacityLimiter(1.0)
    async with limiter:
        assert limiter.available_tokens == 0.0
    assert limiter.available_tokens == 1.0


# ---------------------------------------------------------------------------
# CapacityLimiter lock under concurrency (Wave 1 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capacity_limiter_concurrent_total_tokens():
    """Concurrent total_tokens mutations do not corrupt state.

    Post-fix: the _total_lock protects the setter so that simultaneous
    reads/writes from multiple threads produce consistent values.
    """
    limiter = CapacityLimiter(10.0)
    errors: list[Exception] = []

    def mutate_total() -> None:
        try:
            for i in range(100):
                limiter.total_tokens = float(5 + (i % 10))
                # Read back immediately — must be consistent.
                _ = limiter.total_tokens
                _ = limiter.available_tokens
                _ = limiter.borrowed_tokens
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=mutate_total) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Concurrent mutations caused: {errors}"


# ---------------------------------------------------------------------------
# R1 FIX-3 / R2 FIX-11 regression tests — release bound + limiter accounting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_semaphore_release_beyond_max_raises():
    """R1-FIX-3: over-release raises ValueError (asyncio.Semaphore parity);
    pre-fix the value silently exceeded max_value."""
    sem = Semaphore(2)  # starts full — releasing without acquiring is over-release
    with pytest.raises(ValueError):
        sem.release()
    await sem.acquire()  # value 1
    sem.release()  # back to max — legal
    with pytest.raises(ValueError):
        sem.release()  # over-release


def test_semaphore_zero_ok():
    """R2-FIX-11: Semaphore(0) is legal (asyncio parity) — a closed gate."""
    sem = Semaphore(0)
    assert sem.value == 0
    with pytest.raises(ValueError):
        sem.release()


@pytest.mark.asyncio
async def test_semaphore_zero_acquire_blocks():
    """R2-FIX-11: acquiring a zero-capacity semaphore blocks."""
    sem = Semaphore(0)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sem.acquire(), timeout=0.2)


@pytest.mark.asyncio
async def test_limiter_shrink_below_borrowed_accounting():
    """R1-FIX-3 (probe R1-B): shrinking below the borrowed count must report
    the REAL borrowed count, not a value derived from the semaphore value."""
    limiter = CapacityLimiter(5)
    for _ in range(3):
        await limiter.acquire()
    limiter.total_tokens = 1
    total, avail, borrowed = limiter.snapshot()
    assert borrowed == 3
    assert avail == total - borrowed
    for _ in range(3):
        limiter.release()
    total, avail, borrowed = limiter.snapshot()
    assert borrowed == 0
    assert avail == total


@pytest.mark.asyncio
async def test_limiter_regrow_after_shrink():
    """R5 修订 D: regrowing after a shrink must not trip the bounded release —
    the setter must update the semaphore max_value BEFORE releasing tokens."""
    limiter = CapacityLimiter(5)
    for _ in range(3):
        await limiter.acquire()
    limiter.total_tokens = 1
    limiter.total_tokens = 5  # regrow — bounded release() must not raise
    total, avail, borrowed = limiter.snapshot()
    assert borrowed == 3
    assert avail == total - borrowed
    for _ in range(3):
        limiter.release()
    assert limiter.available_tokens == limiter.total_tokens


def test_limiter_overrelease_keeps_borrowed():
    """R5 修订 D: a failing (over-)release must not half-update the borrowed
    count (release first, then bookkeeping, in one _total_lock region)."""
    limiter = CapacityLimiter(1)
    with pytest.raises(ValueError):
        limiter.release()  # nothing borrowed — over-release
    assert limiter.borrowed_tokens == 0
    assert limiter.available_tokens == limiter.total_tokens


@pytest.mark.asyncio
async def test_limiter_fractional_lt_one():
    """R2-FIX-11 (probe R2-C): total_tokens in (0,1) is a capacity-0 gate —
    pre-fix it crashed with ValueError from Semaphore(int(0.5)) == Semaphore(0)."""
    limiter = CapacityLimiter(0.5)
    assert limiter.total_tokens == 0.5
    assert limiter.available_tokens == 0.5
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(limiter.acquire(), timeout=0.2)
    limiter.total_tokens = 1.5
    await asyncio.wait_for(limiter.acquire(), timeout=1.0)
    limiter.release()


def test_semaphore_max_value_consistent_after_resize():
    """U2 re-audit: max_value is read under the lock, so a concurrent
    CapacityLimiter resize (which rewrites _max_value from another thread)
    can never be observed as a torn value."""
    s = Semaphore(2)
    assert s.max_value == 2
    limiter = CapacityLimiter(1.0)
    limiter.total_tokens = 5.0
    assert limiter._semaphore.max_value == 5  # type: ignore[attr-defined]
    limiter.total_tokens = 2.5
    assert limiter._semaphore.max_value == 2  # type: ignore[attr-defined]
