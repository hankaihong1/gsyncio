# gsyncio: Multi-Event-Loop Engine & Concurrency Toolkit for Python 3.14t

[![CI](https://img.shields.io/github/actions/workflow/status/hankaihong1/gsyncio/ci.yml)](https://github.com/hankaihong1/gsyncio/actions/workflows/ci.yml)
[![Python 3.14](https://img.shields.io/badge/Python-3.14-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**[中文版 (Chinese)](README_ZH.md)**

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Architecture](#architecture)
- [Core Features](#core-features)
- [Examples](#examples)
- [Live Demo](#live-demo)
- [API Reference](#api-reference)
- [Quality & Tests](#quality--tests)
- [Community Docs](#community-docs)
- [License](#license)

---

## Introduction

`gsyncio` is a high-performance multi-event-loop thread pool and Go-level
concurrency primitive toolkit designed exclusively for **Python 3.14t
(Free-Threaded / no-GIL)**. It features an `asyncssh`-style top-level API
facade, supporting both zero-config top-level function access and explicit
thread pool management.

---

## Installation

### Prerequisites

- **Python 3.14+**: Free-Threaded (no-GIL) build, e.g. `3.14t`.
- **Rust stable toolchain**: to compile the `_gsyncio_core` C extension from source.

### Install with pip

```bash
pip install gsyncio
```

### Install with uv

```bash
uv add gsyncio
```

### Build from source

```bash
# After cloning, build and install into the current environment with maturin
maturin develop --release
```

> **Note**: install `maturin` (`pip install maturin` or `uv tool install maturin`)
> and the Rust stable toolchain (via [rustup](https://rustup.rs)) first.

---

## Architecture

```mermaid
graph TD
    UserApp[User Application / FastAPI / ASGI 3.0] -->|pool.submit| Scheduler[Round-Robin Scheduler]
    
    subgraph gsyncio Core Engine
        Scheduler -->|Round-Robin Notify| W1[Worker Loop Thread 1]
        Scheduler -->|Round-Robin Notify| W2[Worker Loop Thread 2]
        Scheduler -->|Round-Robin Notify| W3[Worker Loop Thread 3]
        
        W1 <-->|Atomic Metrics| RustCore[Rust C-Extension _gsyncio_core]
        W2 <-->|Atomic Metrics| RustCore
        W3 <-->|Atomic Metrics| RustCore
    end
    
    subgraph Golang Concurrency Toolkit
        RustCore <--> FastChan[FastChannel / flume]
        FastChan <--> Select[gsyncio.select_channel]
        FastChan <--> Context[AsyncContext]
        RustCore <--> WaitGroup[AsyncWaitGroup]
    end
```

---

## Core Features

- ⚡ **True Multithreaded Parallelism**: breaks the GIL entirely, achieving up
  to **3.48x+ physical multi-core speedup** in Python 3.14t environments.
- 🎯 **Round-Robin Worker Distribution**: tasks are pushed into a shared
  lock-free queue; worker threads pull and execute via a work-stealing model,
  with wake-up notifications distributed round-robin across worker loops.
- 🦀 **Rust Engine (`_gsyncio_core`)**: lock-free primitives and the C
  extension are written in Rust (PyO3 + `flume` + `parking_lot`), delivering
  zero busy-wait (0% CPU idle) and extreme channel throughput.
- 🚀 **`asyncssh`-Style Top-Level API Facade**: minimal API surface such as
  `gsyncio.select_channel(...)` and `gsyncio.EventLoopThreadPool`, with pool
  lifecycle managed via `async with`.
- 🦫 **Golang-Style Concurrency Primitives**:
  - `FastChannel` & `AsyncChannel` (elegant iteration via `async for item in ch:`)
  - `gsyncio.select_channel(*channels)` (multi-channel select/multiplex)
  - `AsyncContext` (cross-thread cascading task cancellation and timeout broadcast)
  - `AsyncWaitGroup` & `AsyncOnce` & `AsyncRWMutex` (read-write lock separation)

---

## Examples

### 1. Top-Level Zero-Config Usage (`asyncssh`-style)

```python
import asyncio
import gsyncio


async def heavy_task(x: int):
    await asyncio.sleep(0.01)
    return x * 2


async def main():
    # async with manages the thread pool lifecycle automatically
    async with gsyncio.EventLoopThreadPool() as pool:
        # Submit an async coroutine (shared queue + work-stealing scheduler)
        fut1 = pool.submit(heavy_task, 21)

        # Explicitly target a worker loop (stateful connection affinity)
        fut2 = pool.submit(heavy_task, 21, loop=0)  # Output: 42

        # Inspect pool health metrics
        print("Metrics:", pool.get_metrics())

    # Leaving the async with block shuts down gracefully


if __name__ == "__main__":
    asyncio.run(main())
```

### 2. Go-Style Channel Iteration and `gsyncio.select_channel`

```python
import asyncio
import gsyncio


async def main():
    ch1 = gsyncio.FastChannel()
    ch2 = gsyncio.FastChannel()

    async def producer():
        await ch1.send("Data from Channel 1")
        ch1.close()

    asyncio.create_task(producer())

    # select_channel waits for the first channel that becomes ready
    selected_ch, val = await gsyncio.select_channel(ch1, ch2)
    print(f"Received: {val}")


if __name__ == "__main__":
    asyncio.run(main())
```

### 3. Concurrent Task Synchronization (Golang `sync.WaitGroup` style)

```python
import asyncio
import gsyncio


async def worker(name: str, wg: gsyncio.AsyncWaitGroup):
    try:
        await asyncio.sleep(0.02)  # simulated work
        print(f"worker {name} done")
    finally:
        wg.done()  # decrement the counter on both success and failure


async def main():
    wg = gsyncio.AsyncWaitGroup()

    # Dispatch 5 tasks across a 4-thread pool
    async with gsyncio.EventLoopThreadPool(num_threads=4) as pool:
        for i in range(5):
            wg.add()  # increment the counter
            pool.submit(worker, f"task-{i}", wg)

        # Block until all tasks finish (counter reaches zero)
        await wg.wait()
        print("all workers finished")


if __name__ == "__main__":
    asyncio.run(main())
```

### 4. Structured Concurrency with Timeout Control

```python
import asyncio
import gsyncio


async def fetch(name: str, delay: float):
    await asyncio.sleep(delay)  # simulated network latency
    return f"{name}: ok"


async def main():
    try:
        # fail_after sets an overall timeout (0.1 s) for the whole block
        async with gsyncio.fail_after(0.1):
            async with gsyncio.TaskGroup() as tg:
                # start_soon spawns a child task immediately, returning a TaskHandle
                h1 = tg.start_soon(fetch, "fast", 0.01)
                h2 = tg.start_soon(fetch, "slow", 0.5)

            # All child tasks are guaranteed finished when leaving the block
            print(await h1, "|", await h2)
    except TimeoutError:
        print("timed out: children did not finish within 0.1 s")


if __name__ == "__main__":
    asyncio.run(main())
```

More runnable examples: [`examples/`](examples/README.md)

---

## Live Demo

Want to see it running without writing code?
[gsyncio-fastapi-demo](https://github.com/hankaihong1/gsyncio-fastapi-demo) is
a real FastAPI application served directly by `GsyncioASGIWorker` — no
uvicorn involved. Its page doubles as a live dashboard: every 2 seconds it
pulls real metrics from `EventLoopThreadPool.get_metrics()` and shows which
of the 4 event-loop threads handled each request.

```bash
git clone https://github.com/hankaihong1/gsyncio-fastapi-demo
cd gsyncio-fastapi-demo
uv sync
uv run python app.py        # then open http://127.0.0.1:8000
```

---

## API Reference

Complete API documentation: [docs/API.md](docs/API.md).

Not sure which primitive to use? See the decision table in
[docs/CHOOSING.md](docs/CHOOSING.md).

---

## Quality & Tests

```bash
# 1. Lint & static checks (0 errors)
uv run ruff check .

# 2. Full automated test suite
uv run pytest
```

---

## Community Docs

- [CONTRIBUTING.md](CONTRIBUTING.md) — Contribution Guide
- [CHANGELOG.md](CHANGELOG.md) — Changelog
- [SECURITY.md](SECURITY.md) — Security Policy
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — Code of Conduct
- [AGENTS.md](AGENTS.md) — AI Development Guide

---

## License

MIT License. See [LICENSE](LICENSE) for details.
