"""High-performance cross-platform FastAPI / ASGI multi-threaded benchmark suite for multiloop.

Tests multiloop's ASGI 3.0 server engine against representative FastAPI workloads:
1. Lightweight JSON API (/api/ping) - HTTP parsing and connection concurrency
2. CPU + Async I/O Hybrid Compute (/api/compute) - Python 3.14t No-GIL multi-core speedup
3. POST Request Body Processing (/api/items) - Payload reading and JSON serialization

Optional Dependencies & Installation:
--------------------------------------
- To benchmark with real FastAPI & Pydantic, install them via:
    uv pip install fastapi pydantic
  or:
    pip install fastapi pydantic
- If FastAPI is not installed, this benchmark automatically falls back to an
  in-tree specification-compliant ASGI 3.0 router with identical route endpoints.

Cross-Platform Pure-Python Design:
- Built-in multi-threaded HTTP/1.1 Keep-Alive socket load generator.
- 100% native on Windows, macOS, and Linux without any external CLI tool dependencies.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import socket
import sys
import threading
import time
from collections import Counter
from typing import Any

from multiloop.asgi import MultiloopASGIWorker
from multiloop.pool import EventLoopThreadPool

thread_counter: Counter[str] = Counter()


def create_benchmark_app() -> Any:
    """Create a FastAPI application or a specification-compliant ASGI 3.0 fallback."""
    try:
        from fastapi import FastAPI
        from pydantic import BaseModel

        class ItemModel(BaseModel):
            name: str
            price: float

        app = FastAPI(title="Multiloop FastAPI Benchmark")

        @app.get("/api/ping")
        async def ping() -> dict[str, str]:
            t_name = threading.current_thread().name
            thread_counter[t_name] += 1
            return {"status": "ok", "thread": t_name}

        @app.get("/api/compute")
        async def compute() -> dict[str, Any]:
            t_name = threading.current_thread().name
            thread_counter[t_name] += 1
            await asyncio.sleep(0.001)
            data = b"fastapi_multiloop_payload_for_python314t_benchmark"
            for _ in range(25000):
                data = hashlib.sha256(data).digest()
            return {"status": "success", "hash_len": len(data), "thread": t_name}

        @app.post("/api/items")
        async def create_item(item: ItemModel) -> dict[str, Any]:
            t_name = threading.current_thread().name
            thread_counter[t_name] += 1
            return {"received": item.model_dump(), "thread": t_name}

        print("   App Framework: Native FastAPI (fastapi + pydantic)")
        return app

    except ImportError:
        print(
            "   App Framework: Built-in ASGI 3.0 Fallback (fastapi not installed; "
            "run `uv pip install fastapi pydantic` to test with native FastAPI)"
        )

        # High-performance, spec-compliant ASGI 3.0 application fallback
        async def asgi_fallback_app(
            scope: dict[str, Any],
            receive: Any,
            send: Any,
        ) -> None:
            scope_type = scope.get("type")
            if scope_type == "lifespan":
                while True:
                    msg = await receive()
                    if msg["type"] == "lifespan.startup":
                        await send({"type": "lifespan.startup.complete"})
                    elif msg["type"] == "lifespan.shutdown":
                        await send({"type": "lifespan.shutdown.complete"})
                        return

            if scope_type != "http":
                return

            path = scope.get("path", "/")
            method = scope.get("method", "GET").upper()
            t_name = threading.current_thread().name
            thread_counter[t_name] += 1

            if path == "/api/ping" and method == "GET":
                body = json.dumps({"status": "ok", "thread": t_name}).encode()
                status = 200
                headers = [(b"content-type", b"application/json")]
            elif path == "/api/compute" and method == "GET":
                await asyncio.sleep(0.001)
                data = b"fastapi_multiloop_payload_for_python314t_benchmark"
                for _ in range(25000):
                    data = hashlib.sha256(data).digest()
                body = json.dumps(
                    {"status": "success", "hash_len": len(data), "thread": t_name}
                ).encode()
                status = 200
                headers = [(b"content-type", b"application/json")]
            elif path == "/api/items" and method == "POST":
                req_body = bytearray()
                while True:
                    chunk = await receive()
                    req_body.extend(chunk.get("body", b""))
                    if not chunk.get("more_body", False):
                        break
                try:
                    payload = json.loads(req_body.decode("utf-8") if req_body else "{}")
                except Exception:
                    payload = {}
                body = json.dumps({"received": payload, "thread": t_name}).encode()
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

        return asgi_fallback_app


def _socket_worker(
    host: str,
    port: int,
    num_requests: int,
    req_bytes: bytes,
    result_list: list[int],
    timeout: float = 10.0,
) -> None:
    """Worker thread executing persistent HTTP/1.1 Keep-Alive requests over raw socket.

    Cross-platform implementation compatible with Windows, macOS, and Linux.
    """
    succ = 0
    sock: socket.socket | None = None

    def _connect() -> socket.socket:
        s = socket.create_connection((host, port), timeout=timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return s

    buf = bytearray(65536)
    i = 0
    while i < num_requests:
        if sock is None:
            try:
                sock = _connect()
            except Exception:
                time.sleep(0.002)
                continue

        try:
            sock.sendall(req_bytes)
            n = sock.recv_into(buf)
            if n <= 0:
                sock.close()
                sock = None
                continue

            # Verify 200 OK status in response header prefix
            if b"200 OK" in buf[: min(n, 64)] or b"200" in buf[: min(n, 64)]:
                succ += 1
            i += 1
        except (TimeoutError, OSError):
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None

    if sock:
        try:
            sock.close()
        except OSError:
            pass

    result_list.append(succ)


def _benchmark_with_native_socket(
    host: str,
    port: int,
    endpoint: str,
    total_requests: int,
    concurrency: int,
    method: str = "GET",
    data: bytes | None = None,
) -> tuple[float, float, int]:
    """Execute high-concurrency load test using pure-Python multi-threaded Keep-Alive sockets.

    Achieves tens of thousands of QPS across all platforms without external dependencies.
    """
    # Build persistent HTTP/1.1 wire payload
    if method.upper() == "POST" and data:
        req_wire = (
            f"POST {endpoint} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(data)}\r\n"
            f"Connection: keep-alive\r\n\r\n"
        ).encode("latin1") + data
    else:
        req_wire = (
            f"GET {endpoint} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: keep-alive\r\n\r\n"
        ).encode("latin1")

    # Warmup connections
    warmup_threads_count = min(4, concurrency)
    warmup_res: list[int] = []
    w_threads = [
        threading.Thread(target=_socket_worker, args=(host, port, 10, req_wire, warmup_res, 3.0))
        for _ in range(warmup_threads_count)
    ]
    for t in w_threads:
        t.start()
    for t in w_threads:
        t.join()

    # Partition requests across worker threads
    req_per_thread = total_requests // concurrency
    remainder = total_requests % concurrency

    results: list[int] = []
    threads: list[threading.Thread] = []

    start = time.perf_counter()
    for idx in range(concurrency):
        count = req_per_thread + (1 if idx < remainder else 0)
        if count <= 0:
            continue
        t = threading.Thread(
            target=_socket_worker,
            args=(host, port, count, req_wire, results, 15.0),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    elapsed = time.perf_counter() - start
    succ = sum(results)
    qps = total_requests / elapsed if elapsed > 0 else 0.0

    return elapsed, qps, succ


async def run_benchmark_client(
    port: int,
    endpoint: str,
    total_requests: int,
    concurrency: int,
    method: str = "GET",
    data: bytes | None = None,
) -> tuple[float, float, int]:
    """Run load test against specified endpoint using native socket engine."""
    return _benchmark_with_native_socket(
        "127.0.0.1", port, endpoint, total_requests, concurrency, method=method, data=data
    )


async def benchmark_scenario(
    name: str,
    endpoint: str,
    total_requests: int,
    concurrency: int,
    method: str = "GET",
    data: bytes | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Run single-worker vs 4-worker vs 8-worker comparison for a given scenario."""
    app = create_benchmark_app()
    app_type = "FastAPI App" if "FastAPI" in type(app).__name__ else "Native ASGI 3.0 Handler"

    print(f"\n{'=' * 75}")
    print(f"📊 Scenario: {name} ({endpoint}) [{app_type}]")
    print(f"   Requests: {total_requests} | Concurrency: {concurrency} | Method: {method}")
    print(f"{'-' * 75}")

    # Phase 1: 1-Worker
    thread_counter.clear()
    async with EventLoopThreadPool(num_threads=1) as pool_1:
        worker_1 = MultiloopASGIWorker(app=app, pool=pool_1, port=0)
        await worker_1.start()
        elapsed_1, qps_1, succ_1 = await run_benchmark_client(
            worker_1.port,
            endpoint,
            total_requests,
            concurrency,
            method=method,
            data=data,
        )
        await worker_1.close()

    print(
        f"  ➜ [1-Worker] Elapsed: {elapsed_1:.4f}s | QPS: {qps_1:8.2f} req/s "
        f"| Success: {succ_1}/{total_requests}"
    )

    # Phase 2: 4-Workers (Multi-core Loop Pool)
    thread_counter.clear()
    async with EventLoopThreadPool(num_threads=4) as pool_4:
        worker_4 = MultiloopASGIWorker(app=app, pool=pool_4, port=0)
        await worker_4.start()
        elapsed_4, qps_4, succ_4 = await run_benchmark_client(
            worker_4.port,
            endpoint,
            total_requests,
            concurrency,
            method=method,
            data=data,
        )
        threads_used = dict(thread_counter)
        metrics_4 = pool_4.get_metrics()
        await worker_4.close()

    speedup_4 = (qps_4 / qps_1) if qps_1 > 0 else 1.0
    print(
        f"  ➜ [4-Worker] Elapsed: {elapsed_4:.4f}s | QPS: {qps_4:8.2f} req/s "
        f"| Success: {succ_4}/{total_requests} | Speedup: {speedup_4:.2f}x"
    )
    print(f"  🧵 4-Worker Thread Load Distribution: {threads_used}")

    # Phase 3: 8-Workers (Full-core Loop Pool)
    thread_counter.clear()
    async with EventLoopThreadPool(num_threads=8) as pool_8:
        worker_8 = MultiloopASGIWorker(app=app, pool=pool_8, port=0)
        await worker_8.start()
        elapsed_8, qps_8, succ_8 = await run_benchmark_client(
            worker_8.port,
            endpoint,
            total_requests,
            concurrency,
            method=method,
            data=data,
        )
        threads_used_8 = dict(thread_counter)
        metrics_8 = pool_8.get_metrics()
        await worker_8.close()

    speedup_8 = (qps_8 / qps_1) if qps_1 > 0 else 1.0
    print(
        f"  ➜ [8-Worker] Elapsed: {elapsed_8:.4f}s | QPS: {qps_8:8.2f} req/s "
        f"| Success: {succ_8}/{total_requests} | Speedup: {speedup_8:.2f}x"
    )
    print(f"  🧵 8-Worker Thread Load Distribution: {threads_used_8}")

    res_1 = {
        "workers": 1,
        "elapsed": elapsed_1,
        "qps": qps_1,
        "succ": succ_1,
        "total": total_requests,
    }
    res_4 = {
        "workers": 4,
        "elapsed": elapsed_4,
        "qps": qps_4,
        "succ": succ_4,
        "total": total_requests,
        "speedup": speedup_4,
        "threads": threads_used,
        "metrics": metrics_4,
    }
    res_8 = {
        "workers": 8,
        "elapsed": elapsed_8,
        "qps": qps_8,
        "succ": succ_8,
        "total": total_requests,
        "speedup": speedup_8,
        "threads": threads_used_8,
        "metrics": metrics_8,
    }
    return res_1, res_4, res_8


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cross-platform pure-Python multiloop ASGI & FastAPI Benchmark Suite."
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=10000,
        help="Total requests for ping scenario (default: 10000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Concurrency level (default: 50)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 75)
    print("🚀 multiloop ASGI & FastAPI Multi-Scenario Benchmark Suite")
    print(f"   Python Version: {sys.version.split()[0]} (Free-Threaded / Multi-Loop Engine)")
    print(f"   CPU Cores Available: {os.cpu_count() or 4}")
    print("   Load Engine: Pure-Python Multi-Threaded Keep-Alive Sockets (100% Cross-Platform)")
    print("=" * 75)

    results: list[tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]] = []

    # Scenario 1: Lightweight JSON API (High QPS & FastHttpParser parsing)
    res1_1, res1_4, res1_8 = await benchmark_scenario(
        name="Lightweight JSON Ping",
        endpoint="/api/ping",
        total_requests=args.requests,
        concurrency=args.concurrency,
        method="GET",
    )
    results.append(("JSON Ping (/api/ping)", res1_1, res1_4, res1_8))

    # Scenario 2: Hybrid CPU Compute + Async I/O (Python 3.14t No-GIL Acceleration)
    res2_1, res2_4, res2_8 = await benchmark_scenario(
        name="CPU Compute + Async I/O",
        endpoint="/api/compute",
        total_requests=400,
        concurrency=40,
        method="GET",
    )
    results.append(("CPU Compute (/api/compute)", res2_1, res2_4, res2_8))

    # Scenario 3: POST JSON Body Processing (Streaming Payload & Deserialization)
    post_payload = json.dumps({"name": "benchmark_item", "price": 99.9}).encode("utf-8")
    res3_1, res3_4, res3_8 = await benchmark_scenario(
        name="POST Request Body Processing",
        endpoint="/api/items",
        total_requests=max(args.requests // 2, 2000),
        concurrency=args.concurrency,
        method="POST",
        data=post_payload,
    )
    results.append(("POST JSON Payload (/api/items)", res3_1, res3_4, res3_8))

    # Print Final Summary Table
    print("\n" + "=" * 90)
    print("🏆 FINAL BENCHMARK SUMMARY (Python 3.14t Multi-Core Free-Threaded Physical Scaling)")
    print("=" * 90)
    header = f"{'Scenario':<30} | {'1-Worker QPS':<13} | {'4-Worker QPS':<13} | {'8-Worker QPS':<13} | {'Max Speedup':<11} | {'Success':<8}"
    print(header)
    print("-" * len(header))
    for name, r1, r4, r8 in results:
        succ_rate = f"{r8['succ']}/{r8['total']}"
        print(
            f"{name:<30} | {r1['qps']:10.2f} r/s | {r4['qps']:10.2f} r/s | {r8['qps']:10.2f} r/s | {r8['speedup']:9.2f}x | {succ_rate:<8}"
        )
    print("=" * 90 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
