# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
