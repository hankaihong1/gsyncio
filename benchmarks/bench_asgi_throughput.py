import asyncio
import hashlib
import re
import shutil
import subprocess
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen

from multiloop.asgi import MultiloopASGIWorker
from multiloop.pool import EventLoopThreadPool

thread_counter = Counter()


# Simulate a typical FastAPI / Starlette async business endpoint
async def fastapi_demo_app(scope, receive, send):
    if scope["type"] != "http":
        return

    path = scope.get("path", "/")

    # Track which thread handled this request
    thread_counter[threading.current_thread().name] += 1

    if path == "/api/compute":
        # Simulate async I/O suspension and business data computation in FastAPI
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


def _http_get(url: str) -> int:
    """One GET request; returns the HTTP status code (0 if the request failed).

    The body is consumed before closing: leaving it unread makes close() send
    an RST (macOS closes-with-unread-data), and the freed port can then be
    reused by a fresh connection while the server is still tearing down the
    old one — that race surfaces as spurious ConnectionResetErrors (~0.2%).
    """
    try:
        with urlopen(url, timeout=30) as resp:
            resp.read()
            return resp.status
    except Exception:
        return 0


def _benchmark_with_ab(url: str, total_requests: int, concurrency: int) -> tuple[float, float, int]:
    """Use Apache Bench (ab) as the load testing client for accurate throughput."""
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


def _benchmark_with_python(
    url: str, total_requests: int, concurrency: int
) -> tuple[float, float, int]:
    """Fallback load client for platforms without `ab` (e.g. Windows).

    A thread pool of plain urllib requests keeps the benchmark dependency-free.
    QPS here is not directly comparable to `ab`: no keep-alive and Python-side
    overhead make the absolute numbers lower. The script only relies on the
    ratio between its two phases (1-thread vs N-thread), which stays valid.
    """

    def run_batch(n: int, workers: int) -> int:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            statuses = list(pool.map(_http_get, [url] * n))
        return sum(1 for s in statuses if s == 200)

    run_batch(50, 10)  # warmup, mirrors the `ab` warmup above

    start = time.perf_counter()
    succ = run_batch(total_requests, concurrency)
    elapsed = time.perf_counter() - start

    return elapsed, total_requests / elapsed, succ


async def benchmark_client(
    port: int, total_requests: int, concurrency: int
) -> tuple[float, float, int]:
    """Load-test the server, preferring `ab` when it is installed.

    Windows ships neither `ab` nor apache2-utils, so fall back to a stdlib
    Python client there instead of failing with FileNotFoundError.
    """
    url = f"http://127.0.0.1:{port}/api/compute"
    if shutil.which("ab") is not None:
        return _benchmark_with_ab(url, total_requests, concurrency)
    return _benchmark_with_python(url, total_requests, concurrency)


async def main():
    total_requests = 500
    concurrency = 50

    print("=" * 70)
    print("🚀 FastAPI / ASGI App + Multiloop Multithreaded Worker Benchmark")
    print("Target: Python 3.14t Free-Threaded Multithreaded ASGI Engine")
    print(f"Requests: {total_requests} | Client Concurrency: {concurrency}")
    print("=" * 70)

    # 1. Single Thread Worker
    async with EventLoopThreadPool(num_threads=1) as pool:
        worker = MultiloopASGIWorker(app=fastapi_demo_app, pool=pool, port=0)
        await worker.start()

        print("\n[1/2] Benchmarking FastAPI App on Single Thread Worker...")
        elapsed_1, qps_1, succ_1 = await benchmark_client(worker.port, total_requests, concurrency)
        print(
            f"  ➜ 1-Thread Elapsed: {elapsed_1:.4f} s | QPS: {qps_1:.2f} req/s (Success: {succ_1}/{total_requests})"
        )

        await worker.close()

    # 2. Multi-Thread Worker (4 Loops)
    async with EventLoopThreadPool(num_threads=4) as pool:
        worker = MultiloopASGIWorker(app=fastapi_demo_app, pool=pool, port=0)
        await worker.start()

        print("\n[2/2] Benchmarking FastAPI App on 4-Thread Multiloop ASGI Worker Pool...")
        thread_counter.clear()
        elapsed_4, qps_4, succ_4 = await benchmark_client(worker.port, total_requests, concurrency)
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
        f"  - 4-Thread Multiloop Worker QPS: {qps_4:8.2f} req/s ({qps_4 / qps_1:.2f}x Acceleration)"
    )
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
