"""Stress tests for gsyncio running on Python 3.14t (free-threaded, no GIL).

These tests exercise cross-thread safety paths that are unique to the
free-threaded build where multiple OS threads can truly execute Python
bytecode concurrently.

All tests are skipped on non-t builds via ``@pytest.mark.free_threading``
which gate-keeps on ``sys._is_gil_enabled() is False``.
"""

import asyncio
import sys
import threading

import pytest

# ── gate: only run on free-threaded builds ──────────────────────────────────

free_threaded = hasattr(sys, "_is_gil_enabled") and sys._is_gil_enabled() is False

# 注意：pyproject.toml 的 markers 定义了 free_threading 标记，这里把它和
# skipif 一起挂到 pytestmark 上——这样 -m "not free_threading" 可以整体
# deselection 本文件（GIL 版解释器下本文件本来就因 skipif 全跳，deselect
# 只对需要区分 free_threading 场景的 CI 矩阵有意义）。
pytestmark = [
    pytest.mark.skipif(not free_threaded, reason="requires Python 3.14t (free-threaded) build"),
    pytest.mark.free_threading,
]

pytest.importorskip("gsyncio")

from gsyncio import Barrier, CancelScope, CapacityLimiter, EventLoopThreadPool  # noqa: E402
from gsyncio.testing import wait_all_tasks_blocked  # noqa: E402

# ---------------------------------------------------------------------------
# CancelScope — cross-thread cancel + read under pressure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_scope_cross_thread_stress():
    """CancelScope.cancel() from multiple threads while reading cancel_called.

    On free-threaded builds, cancel() may interleave with cancel_called
    reads — the internal _cancel_lock must keep the flag consistent.
    """
    scope = CancelScope()
    read_values: list[bool] = []
    mutex = threading.Lock()

    async def worker():
        # WHY: 不能在这里捕获 CancelledError。scope.cancel() 注入的取消必须
        # 穿透到 task 本身，否则 worker 正常完成、await task 永不 raise，
        # 与下方 pytest.raises(CancelledError) 断言自相矛盾。
        async with scope:
            await asyncio.sleep(5)

    task = asyncio.create_task(worker())
    await asyncio.sleep(0)

    def hammer_cancel():
        for _ in range(100):
            scope.cancel()

    def hammer_read():
        for _ in range(100):
            with mutex:
                read_values.append(scope.cancel_called)

    threads = [threading.Thread(target=hammer_cancel) for _ in range(3)] + [
        threading.Thread(target=hammer_read) for _ in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    await asyncio.sleep(0.05)

    # All reads must be consistent (no torn reads of the boolean).
    # After hammering, cancel_called must be True.
    assert scope.cancel_called is True
    # Every read should return a valid bool.
    assert all(isinstance(v, bool) for v in read_values)

    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# CapacityLimiter — concurrent mutations under free-threading
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capacity_limiter_concurrent_mutations():
    """CapacityLimiter.total_tokens reads/writes are consistent under
    concurrent access from multiple threads.

    On free-threaded builds, the _total_lock must serialise mutations.
    """
    limiter = CapacityLimiter(100.0)
    errors: list[Exception] = []
    snapshots: list[tuple[float, float, float]] = []

    def mutate_and_snapshot(seed: int):
        try:
            for i in range(200):
                new_total = float(50 + ((i + seed) % 50))
                limiter.total_tokens = new_total
                # snapshot() 在单次 _total_lock 内读齐三个值——分开读三个
                # property 的话，另一线程在两次读之间改 total_tokens 会把
                # 基于不同 total 的值混在一起，不变量必然被"破坏"（这是
                # 跨锁区快照不一致，不是数据竞态）。
                total, avail, borrowed = limiter.snapshot()
                # Invariant: available + borrowed ≈ total
                if not (abs((avail + borrowed) - total) < 1.0):
                    errors.append(
                        ValueError(
                            f"invariant broken: total={total} avail={avail} borrowed={borrowed}"
                        )
                    )
                snapshots.append((total, avail, borrowed))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=mutate_and_snapshot, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"errors: {errors}"
    # Final state must be a valid total_tokens.
    assert limiter.total_tokens > 0


# ---------------------------------------------------------------------------
# Barrier — cross-loop abort under concurrency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_barrier_cross_loop_abort_stress():
    """Barrier.abort() from a non-asyncio thread while parties are waiting.

    On free-threaded builds, abort() must use call_soon_threadsafe
    to wake waiters, and the _mutex must prevent torn state.
    """
    barrier = Barrier(parties=4)
    errors_in_waiters: list[type[Exception]] = []

    async def waiter():
        try:
            await barrier.wait()
        except RuntimeError:
            errors_in_waiters.append(RuntimeError)
        except BaseException as exc:
            errors_in_waiters.append(type(exc))

    tasks = [asyncio.create_task(waiter()) for _ in range(3)]
    await wait_all_tasks_blocked()
    assert barrier.n_waiting == 3

    def abort_from_thread():
        barrier.abort()

    thread = threading.Thread(target=abort_from_thread)
    thread.start()
    thread.join()

    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
    assert len(errors_in_waiters) == 3
    assert all(e is RuntimeError for e in errors_in_waiters)

    # Subsequent wait must also raise
    with pytest.raises(RuntimeError, match="aborted"):
        await barrier.wait()


# ---------------------------------------------------------------------------
# EventLoopThreadPool — concurrent submit + close under free-threading
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_concurrent_submit_close():
    """Pool.submit() concurrent with close() must not crash.

    On free-threaded builds the internal locks (_resolve_target_worker,
    _groups_lock) serialise access across threads.
    """
    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()

    errors: list[Exception] = []

    def hammer_submit():
        for _ in range(20):
            try:
                pool.submit(asyncio.sleep, 0.001)
            except Exception as exc:
                errors.append(exc)

    threads = [threading.Thread(target=hammer_submit) for _ in range(4)]
    for t in threads:
        t.start()

    # Close while threads are still submitting — exercises lock safety.
    await pool.close()

    for t in threads:
        t.join()

    # Some errors (ThreadPoolClosedError) are expected from submit
    # after close. None of them should be segmentation faults or
    # internal Python exceptions (AttributeError, TypeError, etc.).
    for exc in errors:
        assert isinstance(exc, RuntimeError), f"Unexpected error type: {type(exc).__name__}: {exc}"
