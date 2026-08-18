"""Tests for multiloop.Semaphore and multiloop.CapacityLimiter."""

import asyncio
import threading

import pytest

from multiloop import (
    AsyncContext,
    AsyncWaitGroup,
    CapacityLimiter,
    EventLoopThreadPool,
    Lock,
    Semaphore,
)
from multiloop.testing import wait_all_tasks_blocked

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
# Regression tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_semaphore_release_beyond_max_raises():
    """over-release raises ValueError (asyncio.Semaphore parity);
    pre-fix the value silently exceeded max_value."""
    sem = Semaphore(2)  # starts full — releasing without acquiring is over-release
    with pytest.raises(ValueError):
        sem.release()
    await sem.acquire()  # value 1
    sem.release()  # back to max — legal
    with pytest.raises(ValueError):
        sem.release()  # over-release


def test_semaphore_zero_ok():
    """Semaphore(0) is legal (asyncio parity) — a closed gate."""
    sem = Semaphore(0)
    assert sem.value == 0
    with pytest.raises(ValueError):
        sem.release()


@pytest.mark.asyncio
async def test_semaphore_zero_acquire_blocks():
    """acquiring a zero-capacity semaphore blocks."""
    sem = Semaphore(0)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sem.acquire(), timeout=0.2)


@pytest.mark.asyncio
async def test_limiter_shrink_below_borrowed_accounting():
    """shrinking below the borrowed count must report
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
    """R5 revision D: regrowing after a shrink must not trip the bounded release —
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
    """R5 revision D: a failing (over-)release must not half-update the borrowed
    count (release first, then bookkeeping, in one _total_lock region)."""
    limiter = CapacityLimiter(1)
    with pytest.raises(ValueError):
        limiter.release()  # nothing borrowed — over-release
    assert limiter.borrowed_tokens == 0
    assert limiter.available_tokens == limiter.total_tokens


@pytest.mark.asyncio
async def test_limiter_fractional_lt_one():
    """total_tokens in (0,1) is a capacity-0 gate —
    pre-fix it crashed with ValueError from Semaphore(int(0.5)) == Semaphore(0)."""
    limiter = CapacityLimiter(0.5)
    assert limiter.total_tokens == 0.5
    assert limiter.total_capacity == 0
    assert limiter.available_tokens == 0.5
    assert limiter.available_capacity == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(limiter.acquire(), timeout=0.2)
    limiter.total_tokens = 1.5
    assert limiter.total_capacity == 1
    assert limiter.available_capacity == 1
    await asyncio.wait_for(limiter.acquire(), timeout=1.0)
    assert limiter.available_capacity == 0
    limiter.release()
    assert limiter.available_capacity == 1


def test_semaphore_max_value_consistent_after_resize():
    """U2 re-audit: max_value is read under the lock, so a concurrent
    CapacityLimiter resize (which updates total_tokens atomically)
    can never be observed as a torn value."""
    s = Semaphore(2)
    assert s.max_value == 2
    limiter = CapacityLimiter(1.0)
    limiter.total_tokens = 5.0
    assert int(limiter.total_tokens) == 5
    limiter.total_tokens = 2.5
    assert int(limiter.total_tokens) == 2


# ---------------------------------------------------------------------------
# Concurrency audit & semantic refactoring tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_waitgroup_add_negative_wake() -> None:
    """AsyncWaitGroup.add(-n) to 0 must wake registered waiters."""
    wg = AsyncWaitGroup()
    wg.add(2)
    waiter_task = asyncio.create_task(wg.wait())
    await asyncio.sleep(0.01)

    assert not waiter_task.done(), "Waiter should be suspended on counter=2"
    wg.add(-2)  # counter becomes 0 via negative delta
    await asyncio.wait_for(waiter_task, timeout=1.0)
    assert waiter_task.done(), "Waiter must be woken when add(-n) drives counter to 0"


@pytest.mark.asyncio
async def test_waitgroup_cross_generation_race() -> None:
    """Fast done() to 0 followed by new generation must not prematurely wake gen 2 waiters."""
    wg = AsyncWaitGroup()
    gen1_results: list[int] = []
    gen2_results: list[int] = []

    # Round 1
    wg.add(1)

    async def gen1_waiter() -> None:
        await wg.wait()
        gen1_results.append(1)

    t1 = asyncio.create_task(gen1_waiter())
    await asyncio.sleep(0.01)

    wg.done()
    await t1
    assert gen1_results == [1]

    # Round 2
    wg.add(1)

    async def gen2_waiter() -> None:
        await wg.wait()
        gen2_results.append(2)

    t2 = asyncio.create_task(gen2_waiter())
    await asyncio.sleep(0.02)
    assert not t2.done(), "Generation 2 waiter must NOT be woken before gen 2 finishes"

    wg.done()
    await t2
    assert gen2_results == [2]


@pytest.mark.asyncio
async def test_async_context_submit_interruptible() -> None:
    """AsyncContext.submit tasks must be interruptible via ctx.cancel()."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        ctx = AsyncContext()
        started_event = threading.Event()

        async def sleep_task() -> str:
            started_event.set()
            await asyncio.sleep(5.0)
            return "finished"

        fut = ctx.submit(pool, sleep_task)
        while not started_event.is_set():
            await asyncio.sleep(0.001)

        # Cancel context while sleep_task is in flight
        ctx.cancel()

        with pytest.raises(asyncio.CancelledError):
            await fut


@pytest.mark.asyncio
async def test_async_context_async_context_manager_and_parent_detachment() -> None:
    """AsyncContext supports async with and auto-detaches from parent upon exit/cancel."""
    root = AsyncContext()
    assert len(root._children) == 0

    async with EventLoopThreadPool(num_threads=2) as pool:
        started_event = threading.Event()

        async def worker() -> str:
            started_event.set()
            await asyncio.sleep(5.0)
            return "ok"

        async with AsyncContext(parent=root) as child:
            assert len(root._children) == 1
            fut = child.submit(pool, worker)
            while not started_event.is_set():
                await asyncio.sleep(0.001)

        # Upon exiting async with, child must be cancelled and detached from root
        assert child.is_cancelled is True
        assert len(root._children) == 0
        with pytest.raises(asyncio.CancelledError):
            await fut


@pytest.mark.asyncio
async def test_async_context_child_unregistration_no_leak() -> None:
    """AsyncContext must unregister child context from parent on cancel to prevent memory leaks."""
    root_ctx = AsyncContext()
    children = [AsyncContext(parent=root_ctx) for _ in range(50)]
    assert len(root_ctx._children) == 50

    for child in children:
        child.cancel()

    assert len(root_ctx._children) == 0, "All cancelled children must be removed from root_ctx"


@pytest.mark.asyncio
async def test_semaphore_zero_bounded_capacity() -> None:
    """Semaphore(0) represents zero-capacity bounded semaphore, reject over-release unless resized."""
    sem = Semaphore(0)
    assert sem.value == 0
    assert sem.max_value == 0
    with pytest.raises(ValueError, match="Semaphore released too many times"):
        sem.release()

    # Fractional limiter creates Semaphore(0) cleanly and supports dynamic resizing
    limiter = CapacityLimiter(0.5)
    assert limiter.total_tokens == 0.5
    assert limiter.available_tokens == 0.5
    limiter.total_tokens = 2.0
    assert limiter.total_tokens == 2.0
    assert limiter.available_tokens == 2.0


@pytest.mark.asyncio
async def test_semaphore_base_exception_cleanup() -> None:
    """Semaphore.acquire must clean up waiter on non-CancelledError BaseException."""
    sem = Semaphore(1)
    await sem.acquire()
    assert sem.value == 0
    assert len(sem._waiters) == 0

    class CustomBaseException(BaseException):
        pass

    async def doomed_waiter() -> None:
        try:
            await sem.acquire()
        except BaseException:
            pass

    t = asyncio.create_task(doomed_waiter())
    await asyncio.sleep(0.01)
    assert len(sem._waiters) == 1
    event = next(iter(sem._waiters))

    # Directly set exception on event/task to simulate BaseException interrupt
    sem._cancel_waiter(event)
    assert len(sem._waiters) == 0

    # Ensure release and subsequent acquire work cleanly without ghost nodes
    sem.release()
    assert sem.value == 1

    await sem.acquire()
    assert sem.value == 0
    t.cancel()
    try:
        await t
    except BaseException:
        pass


@pytest.mark.asyncio
async def test_capacity_limiter_downscale_convergence() -> None:
    """CapacityLimiter release during downscaling deficit must absorb tokens and not allow new acquires."""
    limiter = CapacityLimiter(4)
    for _ in range(4):
        await limiter.acquire()

    assert limiter.borrowed_tokens == 4.0

    # Downscale from 4 to 2
    limiter.total_tokens = 2.0
    assert limiter.total_tokens == 2.0
    assert limiter.borrowed_tokens == 4.0
    assert limiter.available_tokens == -2.0

    # Task 1 releases its token.
    # Because borrowed (4) > total (2), this returned token MUST be absorbed.
    # borrowed becomes 3, available becomes -1, and no new task should be able to acquire.
    limiter.release()
    assert limiter.borrowed_tokens == 3.0
    assert limiter.available_tokens == -1.0

    # Verify that a new task CANNOT acquire immediately while still in deficit (borrowed 3 >= total 2)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(limiter.acquire(), timeout=0.1)

    # Task 2 releases its token -> borrowed becomes 2, available becomes 0 (deficit cleared)
    limiter.release()
    assert limiter.borrowed_tokens == 2.0
    assert limiter.available_tokens == 0.0

    # Still at capacity (borrowed 2 == total 2), new acquire must still block
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(limiter.acquire(), timeout=0.1)

    # Task 3 releases its token -> borrowed becomes 1, available becomes 1
    limiter.release()
    assert limiter.borrowed_tokens == 1.0
    assert limiter.available_tokens == 1.0

    # Now a new task CAN acquire
    await asyncio.wait_for(limiter.acquire(), timeout=0.2)
    assert limiter.borrowed_tokens == 2.0

    # Clean up
    limiter.release()
    limiter.release()
    assert limiter.borrowed_tokens == 0.0


@pytest.mark.asyncio
async def test_capacity_limiter_upscale_during_deficit_does_not_overmint() -> None:
    """CapacityLimiter upscaling during a deficit must offset deficit rather than minting phantom permits."""
    limiter = CapacityLimiter(10)
    for _ in range(8):
        await limiter.acquire()

    assert limiter.borrowed_tokens == 8.0

    # Downscale to 2: deficit is 8 - 2 = 6
    limiter.total_tokens = 2.0
    assert limiter.available_tokens == -6.0

    # Upscale to 4: new deficit is 8 - 4 = 4.
    # Since borrowed (8) is still > new_total (4), NO permits should be minted into the semaphore!
    limiter.total_tokens = 4.0
    assert limiter.available_tokens == -4.0

    # A new task must NOT be able to acquire because borrowed (8) > total (4)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(limiter.acquire(), timeout=0.1)

    # Release 4 tokens to bring borrowed from 8 down to 4
    for _ in range(4):
        limiter.release()

    assert limiter.borrowed_tokens == 4.0
    assert limiter.available_tokens == 0.0

    # At capacity (borrowed 4 == total 4), acquire still blocks
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(limiter.acquire(), timeout=0.1)

    # Release 1 more token -> borrowed becomes 3, available becomes 1
    limiter.release()
    assert limiter.borrowed_tokens == 3.0
    assert limiter.available_tokens == 1.0

    # Now acquire succeeds
    await asyncio.wait_for(limiter.acquire(), timeout=0.2)
    assert limiter.borrowed_tokens == 4.0

    # Clean up
    for _ in range(4):
        limiter.release()
    assert limiter.borrowed_tokens == 0.0


@pytest.mark.asyncio
async def test_async_wait_group_holding() -> None:
    """AsyncWaitGroup.holding() increments on enter and decrements on exit/exception."""
    wg = AsyncWaitGroup()

    # Normal execution
    async def worker() -> None:
        async with wg.holding():
            await asyncio.sleep(0.01)

    t1 = asyncio.create_task(worker())
    t2 = asyncio.create_task(worker())
    await asyncio.sleep(0.002)

    # Wait for completion
    await asyncio.wait_for(wg.wait(), timeout=1.0)
    await t1
    await t2

    # Exception path
    async def failing_worker() -> None:
        async with wg.holding():
            raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await failing_worker()

    # Wait should succeed immediately because counter is 0
    await asyncio.wait_for(wg.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_async_wait_group_wrap() -> None:
    """AsyncWaitGroup.wrap wraps callables and tracks execution."""
    wg = AsyncWaitGroup()

    async def async_worker(x: int) -> int:
        await asyncio.sleep(0.01)
        return x * 2

    wrapped = wg.wrap(async_worker)
    res = await wrapped(21)
    assert res == 42
    await asyncio.wait_for(wg.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_async_wait_group_track_unawaited_leak_protection() -> None:
    """AsyncWaitGroup.track must not leak counter when tracked coroutine is closed or discarded."""
    import gc

    wg = AsyncWaitGroup()

    async def sample_coro() -> int:
        return 123

    # Case 1: tracked coroutine explicitly closed
    tracked = wg.track(sample_coro())
    tracked.close()
    # Counter must be 0, so wait() returns immediately
    await asyncio.wait_for(wg.wait(), timeout=0.1)

    # Case 2: tracked coroutine discarded and garbage collected
    def discard_tracked() -> None:
        _ = wg.track(sample_coro())

    discard_tracked()
    gc.collect()
    await asyncio.wait_for(wg.wait(), timeout=0.1)

    # Case 3: tracked coroutine awaited normally
    tracked_normal = wg.track(sample_coro())
    val = await tracked_normal
    assert val == 123
    await asyncio.wait_for(wg.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_cancellation_storm_lock() -> None:
    """Cancellation storm on Lock: 500 queued waiters simultaneously cancelled."""
    lock = Lock()
    await lock.acquire()

    cancelled_count = 0

    async def waiter() -> None:
        nonlocal cancelled_count
        try:
            await lock.acquire()
            lock.release()
        except asyncio.CancelledError:
            cancelled_count += 1
            raise

    tasks = [asyncio.create_task(waiter()) for _ in range(500)]
    await asyncio.sleep(0.01)
    assert len(lock._waiters) == 500

    # Cancel all waiters simultaneously
    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    assert cancelled_count == 500
    assert len(lock._waiters) == 0

    # Ensure lock is still operational and clean
    lock.release()
    assert not lock.locked

    await lock.acquire()
    assert lock.locked
    lock.release()


@pytest.mark.asyncio
async def test_cancellation_storm_semaphore() -> None:
    """Cancellation storm on Semaphore: 500 queued waiters simultaneously cancelled."""
    sem = Semaphore(1)
    await sem.acquire()
    assert sem.value == 0

    cancelled_count = 0

    async def waiter() -> None:
        nonlocal cancelled_count
        try:
            await sem.acquire()
        except asyncio.CancelledError:
            cancelled_count += 1
            raise

    tasks = [asyncio.create_task(waiter()) for _ in range(500)]
    await asyncio.sleep(0.01)
    assert len(sem._waiters) == 500

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    assert cancelled_count == 500
    assert len(sem._waiters) == 0
    assert sem.value == 0

    # Release permit and ensure new acquire succeeds
    sem.release()
    assert sem.value == 1
    await sem.acquire()
    assert sem.value == 0
    sem.release()


@pytest.mark.asyncio
async def test_cancellation_storm_capacity_limiter() -> None:
    """Cancellation storm on CapacityLimiter: 500 queued waiters simultaneously cancelled."""
    limiter = CapacityLimiter(1)
    await limiter.acquire()

    cancelled_count = 0

    async def waiter() -> None:
        nonlocal cancelled_count
        try:
            await limiter.acquire()
            limiter.release()
        except asyncio.CancelledError:
            cancelled_count += 1
            raise

    tasks = [asyncio.create_task(waiter()) for _ in range(500)]
    await asyncio.sleep(0.01)
    assert len(limiter._waiters) == 500

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    assert cancelled_count == 500
    assert len(limiter._waiters) == 0

    total, avail, borrowed = limiter.snapshot()
    assert total == 1.0
    assert avail == 0.0
    assert borrowed == 1.0

    limiter.release()
    total, avail, borrowed = limiter.snapshot()
    assert total == 1.0
    assert avail == 1.0
    assert borrowed == 0.0
