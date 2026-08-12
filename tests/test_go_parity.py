import asyncio
import threading

import pytest

from gsyncio import (
    AsyncOnce,
    AsyncRWMutex,
    AsyncWaitGroup,
    EventLoopThreadPool,
)


@pytest.mark.asyncio
async def test_async_rw_mutex_concurrent_read_exclusive_write():
    """4. Go parity sync.RWMutex: Validate AsyncRWMutex read sharing and write mutual exclusion"""
    rwlock = AsyncRWMutex()
    read_count = 0
    max_concurrent_readers = 0
    writing = False

    async def reader():
        nonlocal read_count, max_concurrent_readers
        async with rwlock.reader():
            assert not writing  # No writes during reads
            read_count += 1
            max_concurrent_readers = max(max_concurrent_readers, read_count)
            await asyncio.sleep(0.02)
            read_count -= 1

    async def writer():
        nonlocal writing
        async with rwlock.writer():
            writing = True
            await asyncio.sleep(0.03)
            writing = False

    # Start 5 reader coroutines and 1 writer coroutine
    readers = [asyncio.create_task(reader()) for _ in range(5)]
    await asyncio.sleep(0.005)
    writer_task = asyncio.create_task(writer())

    await asyncio.gather(*readers, writer_task)
    assert (
        max_concurrent_readers > 1
    )  # Validate shared read lock (multiple readers enter concurrently)


@pytest.mark.asyncio
async def test_async_wait_group_basic():
    """Test AsyncWaitGroup cooperative waiting"""
    wg = AsyncWaitGroup()
    wg.add(3)

    results = []

    async def worker(i):
        await asyncio.sleep(0.01)
        results.append(i)
        wg.done()

    for i in range(3):
        asyncio.create_task(worker(i))

    await wg.wait()
    assert len(results) == 3


@pytest.mark.asyncio
async def test_async_once_single_execution():
    """Test AsyncOnce ensures logic executes exactly once"""
    once = AsyncOnce()
    counter = 0

    async def init_func():
        nonlocal counter
        counter += 1
        await asyncio.sleep(0.01)
        return "init_result"

    async def runner():
        return await once.do(init_func)

    # 10 concurrent tasks call once.do simultaneously
    results = await asyncio.gather(*[runner() for _ in range(10)])

    assert counter == 1
    assert all(r == "init_result" for r in results)


@pytest.mark.asyncio
async def test_async_wait_group_wait_zero():
    """AsyncWaitGroup.wait() returns immediately when counter is already zero (line 273)."""
    wg = AsyncWaitGroup()
    # Counter starts at 0 — wait() must return without blocking.
    await wg.wait()


@pytest.mark.asyncio
async def test_async_once_with_coroutine():
    """AsyncOnce.do() with a coroutine function — covers primitives.py
    branch 342->344 (iscoroutine/isfuture check)."""
    once = AsyncOnce()

    async def async_fn():
        return 42

    result = await once.do(async_fn)
    assert result == 42

    # Second call returns cached result
    result2 = await once.do(async_fn)
    assert result2 == 42


@pytest.mark.asyncio
async def test_async_once_exception_rethrow():
    """Test AsyncOnce exception capture and re-throw"""
    once = AsyncOnce()

    def fail_fn():
        raise ValueError("Once failed")

    with pytest.raises(ValueError, match="Once failed"):
        await once.do(fail_fn)

    # Second do still raises the same exception
    with pytest.raises(ValueError, match="Once failed"):
        await once.do(fail_fn)


@pytest.mark.asyncio
@pytest.mark.repeat(10)
async def test_race_async_once_100_threads_blitz():
    """Race 3: Validate that 100 coroutines racing AsyncOnce under multi-threaded loops execute exactly once"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        once = AsyncOnce()
        counter = 0

        async def init_func():
            nonlocal counter
            counter += 1
            await asyncio.sleep(0.01)
            return counter

        futs = [pool.submit(once.do, init_func) for _ in range(100)]
        results = await asyncio.gather(*futs)

        assert counter == 1
        assert all(r == 1 for r in results)


@pytest.mark.asyncio
async def test_race_wait_group_high_concurrency():
    """Race 4: Validate that AsyncWaitGroup add/done/wait under high-concurrency multi-threading has no race-condition missed wakeups"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        wg = AsyncWaitGroup()
        total_tasks = 100
        wg.add(total_tasks)
        completed = 0
        lock = threading.Lock()

        async def worker():
            nonlocal completed
            await asyncio.sleep(0.005)
            with lock:
                completed += 1
            wg.done()

        for _ in range(total_tasks):
            pool.submit(worker)

        await wg.wait()
        assert completed == total_tasks


# ---------------------------------------------------------------------------
# FIX-D (R5 audit): cancelled AsyncWaitGroup.wait() must unregister
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_waitgroup_cancelled_wait_unregisters() -> None:
    """FIX-D: a cancelled ``wait()`` must remove its entry from the Rust
    waiter list — pre-fix every cancelled wait left a stale entry that only
    a later done()-to-zero drained (unbounded growth on long-lived groups)."""
    wg = AsyncWaitGroup()
    wg.add(1)
    tasks = [asyncio.create_task(wg.wait()) for _ in range(5)]
    await asyncio.sleep(0)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    waiters = wg._inner.done()  # returns the handed-over waiter list
    n = len(waiters) if waiters else 0
    assert n == 0, f"{n} stale waiter entries left after cancellation"


# ---------------------------------------------------------------------------
# R7-D: cancelled AsyncOnce leader must not poison later callers with CE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_once_cancelled_leader_does_not_poison_later_callers() -> None:
    """R7-D: after the leader is cancelled, a later caller's do() gets a
    RuntimeError instead of a CancelledError.

    Pre-fix: _exc stored the CancelledError, so an unrelated later caller's
    do() raised it — a user-level CE marks its task as cancelled (probe R7-BD).
    """
    once = AsyncOnce()
    started = asyncio.Event()

    async def fn() -> None:
        started.set()
        await asyncio.sleep(60)

    lt = asyncio.create_task(once.do(fn))
    await started.wait()
    await asyncio.sleep(0.01)
    lt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lt  # the leader itself re-raises the original CE (correct)

    ct = asyncio.create_task(once.do(fn))
    with pytest.raises(RuntimeError, match="cancelled"):
        await ct
    assert not ct.cancelled()  # pre-fix: True (CE poisoning)


@pytest.mark.asyncio
async def test_once_cancelled_leader_follower_gets_runtime_error() -> None:
    """R7-D: a follower waiting alongside the leader gets a RuntimeError
    instead of a CancelledError."""
    once = AsyncOnce()
    started = asyncio.Event()

    async def fn() -> None:
        started.set()
        await asyncio.sleep(60)

    lt = asyncio.create_task(once.do(fn))
    await started.wait()
    ft = asyncio.create_task(once.do(fn))
    await asyncio.sleep(0.01)
    lt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lt  # the leader itself re-raises the original CE (correct)
    with pytest.raises(RuntimeError, match="cancelled"):
        await ft
    assert not ft.cancelled()  # pre-fix: True (CE poisoning)
