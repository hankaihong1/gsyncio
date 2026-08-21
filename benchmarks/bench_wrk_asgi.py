"""Production-Grade wrk-based Benchmark Suite for multiloop ASGI Server.

=============================================================================
⚠️  PREREQUISITE NOTICE / 依赖前置说明:
    This benchmark requires the 'wrk' HTTP benchmarking tool installed on your system.
    本基准测试依赖系统中安装的 'wrk' 高性能压测工具（C语言编写，极低CPU占用，避免同机客户端CPU争抢）。

    Installation Instructions / 安装方式:
    - macOS (Homebrew):
        $ brew install wrk

    - Debian / Ubuntu:
        $ sudo apt-get install wrk

    - Arch Linux:
        $ sudo pacman -S wrk

    - Build from Source (Any Linux / Unix):
        $ git clone https://github.com/wg/wrk.git
        $ cd wrk && make
        $ sudo cp wrk /usr/local/bin/
=============================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
from typing import Any

from multiloop.asgi import MultiloopASGIWorker
from multiloop.pool import EventLoopThreadPool


def create_asgi_benchmark_app() -> Any:
    """Create specification-compliant high-performance ASGI 3.0 benchmark application."""

    async def asgi_app(
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

        if path == "/api/plaintext" or path == "/":
            body = b"Hello, World!"
            status = 200
            headers = [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", b"13"),
            ]
        elif path == "/api/ping":
            body = json.dumps({"status": "ok"}).encode("utf-8")
            status = 200
            headers = [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("latin1")),
            ]
        elif path == "/api/compute":
            # Simulate micro-I/O yield + small payload compute
            await asyncio.sleep(0.0001)
            body = b'{"status":"computed"}'
            status = 200
            headers = [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("latin1")),
            ]
        else:
            body = b"Not Found"
            status = 404
            headers = [
                (b"content-type", b"text/plain"),
                (b"content-length", b"9"),
            ]

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

    return asgi_app


def run_wrk_command(
    url: str,
    duration: int = 5,
    concurrency: int = 100,
    threads: int = 4,
) -> tuple[float, float, str]:
    """Execute `wrk` subprocess and extract QPS, average latency, and raw output.

    :param url: Target HTTP URL.
    :param duration: Benchmark duration in seconds.
    :param concurrency: Number of concurrent HTTP connections.
    :param threads: Number of worker threads for wrk.
    :returns: Tuple of (requests_per_sec, avg_latency_ms, raw_output).
    """
    cmd = [
        "wrk",
        "-t",
        str(threads),
        "-c",
        str(concurrency),
        "-d",
        f"{duration}s",
        "--latency",
        url,
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=duration + 10,
        )
        output = proc.stdout
    except subprocess.CalledProcessError as e:
        output = e.stdout + "\n" + e.stderr
        return 0.0, 0.0, output
    except Exception as e:
        return 0.0, 0.0, str(e)

    # Parse Requests/sec: 123456.78
    qps = 0.0
    qps_match = re.search(r"Requests/sec:\s+([\d\.]+)", output)
    if qps_match:
        qps = float(qps_match.group(1))

    # Parse Latency avg (e.g. 1.23ms, 450.00us, 1.05s)
    avg_latency_ms = 0.0
    lat_match = re.search(r"Latency\s+([\d\.]+)(us|ms|s)", output)
    if lat_match:
        val = float(lat_match.group(1))
        unit = lat_match.group(2)
        if unit == "us":
            avg_latency_ms = val / 1000.0
        elif unit == "ms":
            avg_latency_ms = val
        elif unit == "s":
            avg_latency_ms = val * 1000.0

    return qps, avg_latency_ms, output


async def run_scenario_benchmark(
    endpoint: str,
    workers_list: list[int],
    duration: int,
    concurrency: int,
    threads: int,
) -> dict[int, tuple[float, float]]:
    """Run benchmark against specific endpoint across various worker counts."""
    app = create_asgi_benchmark_app()
    results: dict[int, tuple[float, float]] = {}

    for num_workers in workers_list:
        print(f"\n  ➜ Launching multiloop ASGI Server with {num_workers} Worker Loop(s)...")
        async with (
            EventLoopThreadPool(num_threads=num_workers) as pool,
            MultiloopASGIWorker(app=app, pool=pool, host="127.0.0.1", port=0) as server,
        ):
            port = server.port
            url = f"http://127.0.0.1:{port}{endpoint}"

            # Brief warmup
            print(f"    [Warmup] 1s initial traffic on {url}...")
            run_wrk_command(url, duration=1, concurrency=min(10, concurrency), threads=2)
            await asyncio.sleep(0.2)

            print(
                f"    [Benchmarking] Running wrk (-t{threads} -c{concurrency} -d{duration}s) on {endpoint}..."
            )
            qps, lat, _raw_out = run_wrk_command(
                url, duration=duration, concurrency=concurrency, threads=threads
            )
            results[num_workers] = (qps, lat)
            print(
                f"    [Result] {num_workers}-Worker: {qps:10.2f} req/s | Avg Latency: {lat:6.2f} ms"
            )

    return results


async def async_main() -> None:
    """Entry point for wrk benchmark suite."""
    parser = argparse.ArgumentParser(
        description="Production-Grade wrk-based multiloop ASGI Benchmark Suite.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Requirements:
  wrk must be installed in PATH (e.g. 'brew install wrk' on macOS or 'apt install wrk' on Linux).
""",
    )
    parser.add_argument(
        "--duration",
        "-d",
        type=int,
        default=5,
        help="Benchmark duration in seconds per test (default: 5s)",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=100,
        help="Number of open HTTP connections in wrk (default: 100)",
    )
    parser.add_argument(
        "--threads",
        "-t",
        type=int,
        default=4,
        help="Number of load generator threads in wrk (default: 4)",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=str,
        default="1,4,8",
        help="Comma-separated list of worker counts to benchmark (default: '1,4,8')",
    )
    args = parser.parse_args()

    # Check for wrk binary presence
    wrk_path = shutil.which("wrk")
    if not wrk_path:
        print("\n" + "=" * 76, file=sys.stderr)
        print("❌ ERROR: 'wrk' executable not found in your PATH!", file=sys.stderr)
        print("=" * 76, file=sys.stderr)
        print(
            "This benchmark requires 'wrk' for accurate zero-client-overhead load testing.\n",
            file=sys.stderr,
        )
        print("Please install wrk using one of the following methods:", file=sys.stderr)
        print("  • macOS (Homebrew) : brew install wrk", file=sys.stderr)
        print("  • Ubuntu / Debian  : sudo apt-get install wrk", file=sys.stderr)
        print("  • Arch Linux       : sudo pacman -S wrk", file=sys.stderr)
        print(
            "  • Build from source: git clone https://github.com/wg/wrk.git && cd wrk && make\n",
            file=sys.stderr,
        )
        sys.exit(1)

    workers_list = [int(w.strip()) for w in args.workers.split(",") if w.strip().isdigit()]
    if not workers_list:
        workers_list = [1, 4, 8]

    cpu_cores = os.cpu_count() or 4

    print("=" * 76)
    print("🚀 multiloop ASGI Server wrk Multi-Core Benchmark Suite")
    print(f"   Python Version : {sys.version.split()[0]} (Free-Threaded No-GIL)")
    print(f"   CPU Cores      : {cpu_cores}")
    print(f"   wrk Path       : {wrk_path}")
    print(
        f"   Settings       : Duration={args.duration}s | Concurrency={args.concurrency} | wrk Threads={args.threads}"
    )
    print(f"   Worker Matrix  : {workers_list}")
    print("=" * 76)

    endpoints = [
        ("/api/plaintext", "Plaintext 13B (/api/plaintext)"),
        ("/api/ping", "JSON Ping (/api/ping)"),
    ]

    all_results: dict[str, dict[int, tuple[float, float]]] = {}

    for path, title in endpoints:
        print("\n============================================================================")
        print(f"📊 Scenario: {title}")
        print("----------------------------------------------------------------------------")
        res = await run_scenario_benchmark(
            endpoint=path,
            workers_list=workers_list,
            duration=args.duration,
            concurrency=args.concurrency,
            threads=args.threads,
        )
        all_results[title] = res

    # Print Final Summary Table
    print("\n" + "=" * 76)
    print("🏆 FINAL wrk BENCHMARK SUMMARY (Physical Multi-Core QPS & Latency)")
    print("=" * 76)

    # Table header
    header_cols = ["Scenario"] + [f"{w}-Worker QPS" for w in workers_list] + ["Max Speedup"]
    print(f"{header_cols[0]:<30} | " + " | ".join(f"{col:>13}" for col in header_cols[1:]))
    print("-" * 76)

    for scenario, res_map in all_results.items():
        base_qps = res_map.get(workers_list[0], (0.0, 0.0))[0]
        row = [f"{scenario:<30}"]
        max_speedup = 1.0
        for w in workers_list:
            qps, _lat = res_map.get(w, (0.0, 0.0))
            if base_qps > 0:
                speedup = qps / base_qps
                if speedup > max_speedup:
                    max_speedup = speedup
            row.append(f"{qps:10.2f} r/s")
        row.append(f"{max_speedup:10.2f}x")
        print(" | ".join(row))

    print("=" * 76 + "\n")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
