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
async def test_capacity_limiter_on_behalf_of():
    """acquire_on_behalf_of / release_on_behalf_of delegate to semaphore."""
    limiter = CapacityLimiter(2.0)
    borrower = object()

    await limiter.acquire_on_behalf_of(borrower)
    assert limiter.available_tokens == 1.0
    limiter.release_on_behalf_of(borrower)
    assert limiter.available_tokens == 2.0


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
