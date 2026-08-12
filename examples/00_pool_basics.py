"""Example 0: thread-pool basics — submitting tasks, pinning workers, health metrics.

Run: uv run python examples/00_pool_basics.py
"""

import asyncio

import gsyncio


async def heavy_task(x: int) -> int:
    """Simulate a slow task: sleep 10ms, then return the doubled value."""
    await asyncio.sleep(0.01)
    return x * 2


async def main() -> None:
    # The pool is an async context manager: entering starts the worker
    # threads, exiting shuts them down gracefully.
    async with gsyncio.EventLoopThreadPool(num_threads=4) as pool:
        # 1. Submit a single task (global queue + work stealing)
        fut1 = pool.submit(heavy_task, 21)
        print("single task:", await fut1)  # 42

        # 2. Pin to a specific worker (pin_to=0) for deterministic routing
        fut2 = pool.submit(heavy_task, 21, pin_to=0)
        print("pinned to worker 0:", await fut2)  # 42

        # 3. Pool health metrics (per-worker active/completed counters)
        metrics = pool.get_metrics()
        print("metrics keys:", sorted(metrics.keys()))


if __name__ == "__main__":
    asyncio.run(main())
