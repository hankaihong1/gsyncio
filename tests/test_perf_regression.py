"""Performance regression tests.

Marked @pytest.mark.slow — skipped by default in CI.
Run with: pytest --run-slow tests/test_perf_regression.py
"""

import asyncio
import time

import pytest

from multiloop.pool import EventLoopThreadPool


@pytest.mark.slow
@pytest.mark.asyncio
async def test_pool_throughput_baseline() -> None:
    """Single-thread pool submits N coroutines and verifies QPS > loose threshold.

    Uses a simple sleep-based task to simulate I/O-bound work.  The threshold
    (10 tasks / sec) is deliberately loose so the test only catches severe
    performance regressions, not normal CI variance.
    """
    n = 100

    async def quick_task() -> int:
        await asyncio.sleep(0.001)
        return 42

    async with EventLoopThreadPool(num_threads=1) as pool:
        start = time.perf_counter()
        futs = [pool.submit(quick_task) for _ in range(n)]
        results = await asyncio.gather(*futs)
        elapsed = time.perf_counter() - start

    qps = n / elapsed
    assert all(r == 42 for r in results)
    assert qps > 10, (
        f"Single-thread QPS {qps:.2f} below loose threshold of 10 tasks/sec — "
        f"possible regression in task dispatch path"
    )


@pytest.mark.slow
@pytest.mark.asyncio
async def test_pool_worker_local_acceleration() -> None:
    """Verify multi-thread worker-local dispatch achieves >1.5x single-thread throughput.

    Worker-local means each worker processes a batch of tasks entirely within
    its own event loop, avoiding cross-thread dispatch overhead.  This is the
    same pattern used by benchmarks/bench_multithread_loops.py.
    """
    n_tasks = 100

    async def quick_task() -> int:
        await asyncio.sleep(0.001)
        return 42

    # ── Single-thread baseline (per-task dispatch) ──────────────────────
    async with EventLoopThreadPool(num_threads=1) as pool:
        start = time.perf_counter()
        futs = [pool.submit(quick_task) for _ in range(n_tasks)]
        results = await asyncio.gather(*futs)
        t_single = time.perf_counter() - start

    assert all(r == 42 for r in results)

    # ── Multi-thread worker-local (batch dispatch, zero cross-thread IPC) ─
    async def worker_batch(n: int) -> None:
        """Run a batch of tasks locally inside a single worker loop."""
        tasks = [quick_task() for _ in range(n)]
        results_local = await asyncio.gather(*tasks)
        assert all(r == 42 for r in results_local)

    async with EventLoopThreadPool(num_threads=2) as pool:
        start = time.perf_counter()
        per_worker = n_tasks // 2
        futs = [pool.submit(worker_batch, per_worker) for _ in range(2)]
        await asyncio.gather(*futs)
        t_multi = time.perf_counter() - start

    speedup = t_single / t_multi
    assert speedup > 1.5, (
        f"Multi-thread worker-local speedup {speedup:.2f}x is below 1.5x threshold — "
        f"possible regression in multi-thread parallelism"
    )
