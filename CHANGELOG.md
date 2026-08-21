# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-08-21

### Added & Optimized
- **wrk High-Concurrency Multi-Core Benchmark Suite (`benchmarks/bench_wrk_asgi.py`)**:
  - Production-grade wrk benchmark suite for physical multi-core scaling under Python 3.14t No-GIL.
  - Achieves **151,366+ QPS** (4-Worker) on `/api/plaintext` and **145,836+ QPS** on `/api/ping` with sub-millisecond latency (0.29ms avg).
- **Rust SIMD HTTP Sliced Streaming & Zero-Allocation Fast-Path (`src/http.rs`, `src/multiloop/_http11.py`)**:
  - `FastHttpConnection` streaming body sliced event dispatch (`ReceivingContentLength`) eliminating cursor rewind and $O(N^2)$ header re-parsing on split TCP packets.
  - $O(1)$ buffer compaction via `compact_buffer` clearing on cursor drain without memory copies.
  - Fast-path for zero-body GET/HEAD requests with module-level singleton `_EMPTY_BODY_MSG`, completely eliminating `asyncio.Queue` heap allocation and event loop dispatch overhead.
- **ASGI Transport Backpressure & Pipelining Concurrency Protection (`src/multiloop/_http11.py`, `src/multiloop/_websocket.py`)**:
  - Physical write backpressure flow control (`_drain_event`) in `Http11Protocol.send()` eliminating server OOM under slow clients.
  - Serial HTTP Pipelining request queueing (`_pending_pipeline_events`) guaranteeing strictly ordered, non-interleaved TCP responses.
  - RFC 6455 WebSocket thread-safe send frame lock synchronization (`_send_lock`), eliminating `RuntimeError: concurrent drain calls` on high-concurrency data/control frame writes.
- **WSGI True Streaming Channel & Client Disconnect Decoupling (`src/multiloop/wsgi.py`)**:
  - `SyncStreamReader` backed by lock-free cross-thread `Channel(maxsize=16)`, eliminating the 10MB pre-buffering requirement and enabling true streaming request body consumption.
  - Automatic channel closure and feeder cancellation upon client disconnection, preventing orphaned synchronous WSGI worker tasks from occupying the thread pool.
- **No-GIL Task Collection Thread Safety & Adaptive CLI Watcher (`src/multiloop/server.py`, `src/multiloop/cli.py`)**:
  - `ConnectionPinningServer` multi-worker task collection mutex protection (`_tasks_lock`), eliminating `RuntimeError: Set changed size during iteration` during `server.close()` under Python 3.14t physical multi-core execution.
  - CLI file watcher with extended ignored directories and adaptive smooth polling backoff (0.3s~0.8s), reducing idle CPU usage while maintaining fast hot-reload responsiveness.

## [0.1.1] - 2026-08-18

### Added & Optimized
- **Pure-Python Cross-Platform ASGI Benchmark Engine (`benchmarks/bench_asgi_throughput.py`)**:
  - Implemented high-performance multi-threaded HTTP/1.1 Keep-Alive raw socket load generator.
  - 100% native on Windows, macOS, and Linux with zero external command dependencies (eliminated `ab` requirement).
  - Achieves 70,000+ QPS with multi-core physical scaling across worker event loops.
- **Rust SIMD FastHttpParser & Zero-Allocation Assembly (`src/http.rs`)**:
  - Direct ASGI `PyList[(PyBytes, PyBytes)]` tuple assembly in Rust C-layer, eliminating 20+ intermediate small-object allocations per HTTP request.
  - Global `OnceLock<InternedMethods>` static interning for common HTTP verbs (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`) and protocol versions (`1.0`, `1.1`, `2.0`), enabling zero-allocation method resolution across worker threads.
- **Rust SIMD RFC 6455 WebSocket Unmasking (`src/websocket.rs`, `src/multiloop/_websocket.py`)**:
  - 64-bit auto-vectorized chunk XOR unmasking with `PyBytes::new_with` single-pass memory safety under Python 3.14t No-GIL.
  - Zero-copy WebSocket frame header parser (`FastWebSocketParser::parse_frame_header`).
- **Rust `RawAsyncRWMutex` 64-Bit Atomic Core (`src/rwlock.rs`, `src/multiloop/rwlock.py`)**:
  - 64-bit atomic state machine with writer-preference fairness.
  - Cancellation token conservation and automatic waiter cleanup (`remove_waiter`), preventing cross-language GC reference leaks and writer-cancellation reader starvation.
- **Platform-Aware Multi-Core Connection Balancing (`src/multiloop/server.py`)**:
  - Linux: Symmetric kernel-level `SO_REUSEPORT` 4-tuple BPF hash balancing across worker event loops.
  - macOS / BSD: Multi-worker shared-socket racing accept (`loop.sock_accept(shared_sock)`) achieving perfectly balanced 25% traffic distribution per worker and 91,650+ QPS.
- **ASGI & FastAPI High-Throughput Benchmarks (`benchmarks/bench_asgi_throughput.py`)**:
  - Real-world POST payload throughput increased from 34.1k to **83,168 req/s (+143% speedup)**.
  - Ultra-lightweight JSON Ping stabilized at **91,125 ~ 91,653 req/s**.
  - Multi-core CPU task dispatch achieving **3.65x physical speedup** on 4 workers (91.3% of theoretical limit).

## [0.1.0] - 2026-08-18

### Added
- **Multi-Event-Loop Engine (`EventLoopThreadPool`)**:
  - Independent `asyncio` event loop running per worker OS thread under Python 3.14t free-threading (no GIL).
  - Native Rust work-stealing shared queue architecture (`NativeWorkerPool`) with lock-free global flume queue and per-worker local channels.
  - Soft poller gate (`num_polling < max(num_workers / 2, 1)`) and batch task pulling (`min(global_len / num_workers + 1, 128)`) to eliminate thundering herd contention.
  - ContextVars preservation across thread hops (`contextvars.copy_context()`).
- **Go-Style Concurrency Primitives**:
  - **`Channel`**: Lock-free channel backed by Rust core (`RawAsyncChannel`), supporting bounded/unbounded buffering, non-blocking `try_send`/`try_recv`, synchronous worker `send_sync`, and double-checked waiter lists.
  - **`select_channel`**: Non-blocking randomized probe and single-future multi-registration arbiter for $O(1)$ first-ready channel selection.
  - **`AsyncWaitGroup`**: Go-style atomic counter with generation tracking and leak-safe `track()`, `wrap()`, and `holding()` RAII context managers.
  - **`AsyncOnce`**: Thread-safe single initialization runner across multi-loop environments.
  - **`AsyncContext`**: Hierarchical cancellation context modeled after Go's `context.Context`.
- **Structured Concurrency**:
  - **`TaskGroup`**: Nursery-style structured concurrency manager with automatic sibling cancellation on failure and non-blocking `start_soon` / synchronized `start`.
  - **`CancelScope`**: Task-local cancellation scope with monotonic deadlines, shield snapshot/restore mechanics, and `fail_after`/`move_on_after` helpers.
- **Cross-Loop Synchronization Toolkit**:
  - **`Lock`**: Fair FIFO async mutex bound to owning tasks.
  - **`Semaphore`**: Fair FIFO async semaphore with token-conservative cancellation forwarding.
  - **`CapacityLimiter`**: Dynamically resizable token capacity limiter with single-lock atomic state transitions.
  - **`Event`**: One-shot cross-thread event with permanent set state.
  - **`Condition`**: Async condition variable atop `Lock` with lock-free `notify()` to prevent notifier-sleeper deadlocks.
  - **`Barrier`**: Synchronized N-party barrier with automatic round advancement and generation tracking.
  - **`AsyncRWMutex`**: Asynchronous read-write lock with writer-preference fairness.
- **High-Performance Web Server Adapters & CLI**:
  - **`ConnectionPinningServer`**: TCP server with Linux kernel-level `SO_REUSEPORT` 4-tuple hashing load balancing and connection affinity.
  - **`MultiloopASGIWorker`**: ASGI 3.0 server adapter with Lifespan protocol, HTTP/1.1 chunked streaming, RFC 6455 WebSockets, and HTTP/2 Tokio runtime bridge (`_multiloop_core`).
  - **`MultiloopWSGIWorker`**: WSGI 1.0.1 (PEP 3333) synchronous worker adapter for Flask and Django.
  - **CLI Runner**: `multiloop run <module:app>` command with auto-reload and configurable worker counts.
- **Telemetry**:
  - **`AtomicMetrics`**: 64-byte padded atomic counters preventing CPU cache-line false sharing.
