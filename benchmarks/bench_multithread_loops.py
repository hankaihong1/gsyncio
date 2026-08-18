import asyncio
import os
import time

from multiloop.pool import EventLoopThreadPool


def cpu_intensive_work(iterations: int = 150000):
    """Pure Python CPU-bound workload (Math & Loop) to test 3.14t Free-threading."""
    acc = 0
    for i in range(iterations):
        acc = (acc + i * i) % 9999991
    return acc


async def mixed_workload_task(task_id: int):
    """Simulate mixed workload: I/O wait + CPU calculation."""
    # I/O simulate
    await asyncio.sleep(0.002)
    # Heavy CPU workload
    res = cpu_intensive_work(iterations=40000)
    return task_id + res


async def run_single_thread_benchmark(total_tasks: int):
    """Run workload on a single Event Loop."""
    start_time = time.perf_counter()
    tasks = [mixed_workload_task(i) for i in range(total_tasks)]
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_time
    return elapsed


async def run_multithread_pool_benchmark(total_tasks: int, num_threads: int):
    """Run workload distributed across multithreaded Event Loop pool."""
    start_time = time.perf_counter()
    async with EventLoopThreadPool(num_threads=num_threads) as pool:
        futs = [pool.submit(mixed_workload_task, i) for i in range(total_tasks)]
        await asyncio.gather(*futs)
    elapsed = time.perf_counter() - start_time
    return elapsed


async def worker_local_batch(tasks_per_worker: int):
    """Run a batch of tasks directly inside a worker loop (Zero dispatch overhead)."""
    tasks = [mixed_workload_task(i) for i in range(tasks_per_worker)]
    await asyncio.gather(*tasks)


async def run_worker_local_benchmark(total_tasks: int, num_threads: int):
    """Run tasks partitioned natively inside worker loops."""
    start_time = time.perf_counter()
    tasks_per_worker = total_tasks // num_threads
    async with EventLoopThreadPool(num_threads=num_threads) as pool:
        futs = [pool.submit(worker_local_batch, tasks_per_worker) for _ in range(num_threads)]
        await asyncio.gather(*futs)
    elapsed = time.perf_counter() - start_time
    return elapsed


async def main():
    cpu_count = os.cpu_count() or 4
    total_tasks = 400

    print("=" * 70)
    print("🚀 Python 3.14t (Free-threaded) MVP Multi-Loop Benchmark")
    print(f"CPU Cores: {cpu_count} | Total Tasks: {total_tasks}")
    print("=" * 70)

    # Warmup
    await run_single_thread_benchmark(20)

    # 1. Single Thread Event Loop Benchmark
    print("\n[1/3] Benchmarking Single Thread Event Loop...")
    t_single = await run_single_thread_benchmark(total_tasks)
    qps_single = total_tasks / t_single
    print(f"  ➜ Single Thread Elapsed: {t_single:.4f} s | QPS: {qps_single:.2f} req/s")

    # 2. Per-Task Dispatch Multi-Thread Event Loop Pool Benchmark
    num_threads = min(cpu_count, 8)
    print("\n[2/3] Benchmarking Multi-Thread Pool (Per-Task Dispatch Overhead)...")
    t_dispatch = await run_multithread_pool_benchmark(total_tasks, num_threads=num_threads)
    qps_dispatch = total_tasks / t_dispatch
    print(f"  ➜ Per-Task Dispatch Elapsed: {t_dispatch:.4f} s | QPS: {qps_dispatch:.2f} req/s")

    # 3. Worker-Local Event Loop Benchmark (Zero-Dispatch Overhead)
    print("\n[3/3] Benchmarking Multi-Thread Pool (Worker-Local Execution)...")
    t_local = await run_worker_local_benchmark(total_tasks, num_threads=num_threads)
    qps_local = total_tasks / t_local
    print(f"  ➜ Worker-Local Elapsed: {t_local:.4f} s | QPS: {qps_local:.2f} req/s")

    # Summary
    print("\n" + "=" * 70)
    print("📊 MVP Performance & Overhead Summary:")
    print(f"  1. Single Thread Loop QPS          : {qps_single:8.2f} req/s (Baseline)")
    print(
        f"  2. Per-Task Cross-Thread Dispatch  : {qps_dispatch:8.2f} req/s ({qps_dispatch / qps_single:.2f}x - Bottlenecked by System Calls)"
    )
    print(
        f"  3. Worker-Local Execution (No IPC) : {qps_local:8.2f} req/s ({qps_local / qps_single:.2f}x - True Multi-core Acceleration)"
    )
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
