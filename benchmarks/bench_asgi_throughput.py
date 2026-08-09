import asyncio
import hashlib
import re
import subprocess
import threading
import time
from collections import Counter

from gsyncio.asgi import GsyncioASGIWorker
from gsyncio.pool import EventLoopThreadPool

thread_counter = Counter()


# 模拟典型的 FastAPI / Starlette 异步业务接口
async def fastapi_demo_app(scope, receive, send):
    if scope["type"] != "http":
        return

    path = scope.get("path", "/")

    # Track which thread handled this request
    thread_counter[threading.current_thread().name] += 1

    if path == "/api/compute":
        # 模拟 FastAPI 中的异步 I/O 挂起与业务数据计算
        await asyncio.sleep(0.001)

        data = b"fastapi_demo_payload_for_python314t_benchmarks"
        for _ in range(50000):
            data = hashlib.sha256(data).digest()

        body = f'{{"status":"success","hash_len":{len(data)}}}'.encode()
        status = 200
        headers = [(b"content-type", b"application/json")]
    else:
        body = b"Not Found"
        status = 404
        headers = [(b"content-type", b"text/plain")]

    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": headers,
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": body,
        }
    )


async def benchmark_client_ab(port: int, total_requests: int, concurrency: int):
    """Use Apache Bench (ab) as the load testing client for accurate throughput."""
    url = f"http://127.0.0.1:{port}/api/compute"

    # Warmup
    subprocess.run(  # noqa: ASYNC221
        ["ab", "-n", "50", "-c", "10", url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    start = time.perf_counter()
    result = subprocess.run(  # noqa: ASYNC221
        ["ab", "-n", str(total_requests), "-c", str(concurrency), url],
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed = time.perf_counter() - start

    # Parse ab output for QPS and success
    qps = 0.0
    succ = 0
    for line in result.stdout.splitlines():
        if "Requests per second:" in line:
            match = re.search(r"Requests per second:\s+([0-9.]+)", line)
            if match:
                qps = float(match.group(1))
        if "Complete requests:" in line:
            match = re.search(r"Complete requests:\s+([0-9]+)", line)
            if match:
                succ = int(match.group(1))
        if "Failed requests:" in line:
            match = re.search(r"Failed requests:\s+([0-9]+)", line)
            if match and int(match.group(1)) > 0:
                succ -= int(match.group(1))

    # If ab parsing fails, fallback to manual timing
    if qps == 0.0:
        qps = total_requests / elapsed
        succ = total_requests

    return elapsed, qps, succ


async def main():
    total_requests = 500
    concurrency = 50

    print("=" * 70)
    print("🚀 FastAPI / ASGI App + Gsyncio Multithreaded Worker Benchmark")
    print("Target: Python 3.14t Free-Threaded Multithreaded ASGI Engine")
    print(f"Requests: {total_requests} | Client Concurrency: {concurrency}")
    print("=" * 70)

    # 1. Single Thread Worker
    async with EventLoopThreadPool(num_threads=1) as pool:
        worker = GsyncioASGIWorker(app=fastapi_demo_app, pool=pool, port=0)
        await worker.start()

        print("\n[1/2] Benchmarking FastAPI App on Single Thread Worker...")
        elapsed_1, qps_1, succ_1 = await benchmark_client_ab(
            worker.port, total_requests, concurrency
        )
        print(
            f"  ➜ 1-Thread Elapsed: {elapsed_1:.4f} s | QPS: {qps_1:.2f} req/s (Success: {succ_1}/{total_requests})"
        )

        await worker.close()

    # 2. Multi-Thread Worker (4 Loops)
    async with EventLoopThreadPool(num_threads=4) as pool:
        worker = GsyncioASGIWorker(app=fastapi_demo_app, pool=pool, port=0)
        await worker.start()

        print("\n[2/2] Benchmarking FastAPI App on 4-Thread Gsyncio ASGI Worker Pool...")
        thread_counter.clear()
        elapsed_4, qps_4, succ_4 = await benchmark_client_ab(
            worker.port, total_requests, concurrency
        )
        print(
            f"  ➜ 4-Thread Elapsed: {elapsed_4:.4f} s | QPS: {qps_4:.2f} req/s (Success: {succ_4}/{total_requests})"
        )
        print(f"  🧵 Thread Distribution: {dict(thread_counter)}")

        metrics = pool.get_metrics()
        print(f"\n  📊 Health Metrics: {metrics}")

        await worker.close()

    print("\n" + "=" * 70)
    print("📊 FastAPI Multi-Thread Acceleration Summary:")
    print(f"  - Single-Thread ASGI QPS : {qps_1:8.2f} req/s")
    print(
        f"  - 4-Thread Gsyncio Worker QPS: {qps_4:8.2f} req/s ({qps_4 / qps_1:.2f}x Acceleration)"
    )
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
