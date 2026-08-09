# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Collaboration tooling**: `docs/CHOOSING.md` (primitive selection decision
  table), `docs/CONCURRENCY.md` (concurrency correctness guide with a
  pre-change checklist), `examples/` runnable examples directory (5 scripts
  + EN/ZH READMEs), and `tests/test_docs.py` doc-drift guard (public API
  coverage + code-block syntax checks).
- **Documentation split**: `README.md` is now English-only, with a new
  `README_ZH.md` (Chinese-only); `examples/README.md` likewise gained
  `examples/README_ZH.md`; every `docs/` document now has a Chinese mirror
  (`API_ZH.md` / `CHOOSING_ZH.md` / `CONCURRENCY_ZH.md`) with EN/ZH
  cross-links.
- **AGENTS.md**: root-directory quick-reference table and 8 mandatory rules
  before touching concurrency code (reproduction test required for bugs,
  no GitHub Actions debugging, request-before-code / green-before-merge).
- **README Live Demo section**: points to
  [gsyncio-fastapi-demo](https://github.com/hankaihong1/gsyncio-fastapi-demo),
  a real FastAPI app served directly by `GsyncioASGIWorker` (no uvicorn)
  whose page doubles as a live dashboard of `EventLoopThreadPool` metrics.
  Added to both the English and Chinese README mirrors.

### Removed
- **Root-directory cleanup**: dropped the unreferenced `test_build.sh` (a
  one-liner duplicating `make develop`).
- **winuvloop support**: removed `benchmarks/benchmark_winuvloop.py`, the
  `winuvloop` dev dependency, the two winuvloop tests in
  `test_loop_factory.py`, and all doc references — winuvloop (uvloop/winloop
  shim) has not committed to thread-safety for free-threaded Python, so
  recommending or testing it as a pool `loop_factory` was misleading.
- **`get_best_loop_factory()`**: removed — it only returned
  `asyncio.new_event_loop` while the name suggested probing for third-party
  loops, which gsyncio deliberately never did. `loop_factory` now defaults to
  `asyncio.new_event_loop` directly.
- **Soft-deprecated aliases**: removed `ConnectionPinningServer.stop()` and
  `GsyncioASGIWorker.stop()` (both were `DeprecationWarning`-emitting aliases
  of `close()`); also dropped the stale `shutdown()` API doc entry that the
  code had already lost.

### Fixed
- **free-threading stress tests**: `test_cancel_scope_cross_thread_stress` no
  longer swallows `CancelledError` inside the worker (the assertion expected
  the cancellation to propagate); `test_capacity_limiter_concurrent_mutations`
  now reads all three counters via the new atomic `snapshot()` instead of
  three separate lock acquisitions that could mix values across `total_tokens`
  resizes.
- **`CapacityLimiter.snapshot()`**: new atomic `(total, available, borrowed)`
  read under a single `_total_lock` acquisition — the invariant
  `available + borrowed == total` holds by construction under any concurrent
  interleaving (previously only when read one property at a time).
- **mypy strict**: removed a stale `type: ignore[import-not-found]` on the
  `winuvloop` import in `pool.py` — `winuvloop` is an installable shim on all
  platforms (it internally selects `uvloop`/`winloop`), so the ignore was
  dead code that tripped `unused-ignore`.
- **Windows server deadlock** (`ConnectionPinningServer`): `ConnectionPinningServer`
  now starts a **single acceptor** on Windows instead of the multi-acceptor
  thundering herd. Windows Proactor associates a socket with exactly one IOCP
  (`CreateIoCompletionPort` is a no-op for an already-associated handle), so
  the second loop's `AcceptEx` completion was delivered to the first loop's
  IOCP and the cross-thread future completion raced — connections got accepted
  but their handlers never resumed, hanging clients (e.g. `httpx` GET) forever.
  macOS/Linux keep the multi-acceptor herd (Selector loops support shared-fd
  read registration).
- **Windows benchmark compatibility** (`benchmarks/bench_asgi_throughput.py`):
  the ASGI throughput benchmark now prefers `ab` when installed and falls
  back to a stdlib-only Python client (urllib + `ThreadPoolExecutor`) when
  it is not — `ab` ships with neither Windows nor apache2-utils equivalents,
  so the script previously crashed with `FileNotFoundError` on Windows. The
  fallback client also consumes the response body before closing; leaving it
  unread made `close()` send an RST (macOS closes-with-unread-data), and the
  freed port could then be reused while the server was still tearing down the
  old socket — a race surfacing as ~0.2% spurious `ConnectionResetError`s in
  the first wave of requests after warmup.
- **ASGI responses now carry a correct Content-Length** (`asgi.py`): the
  worker buffers the response until the final body message and emits
  `Content-Length`, instead of close-delimited responses that rule out
  keep-alive and force clients to read-to-EOF. Reason phrases now come from
  `http.HTTPStatus` rather than a hardcoded `"OK"` (`HTTP/1.1 404 OK` is
  now `HTTP/1.1 404 Not Found`).
- **Server shutdown no longer orphans in-flight connections** (`server.py`):
  `ConnectionPinningServer.close()` now cancels tracked connection-handler
  tasks, round-tripping each worker loop so the cancellations land before
  the pool stops — the `"Task was destroyed but it is pending!"` warnings
  at shutdown are gone.

### Changed
- **`get_best_loop_factory()` no longer probes for third-party loops**:
  it returns `asyncio.new_event_loop` unconditionally, matching asyncio's
  policy philosophy (the framework never goes looking for user-installed
  loops). Users who want uvloop/winloop/winuvloop pass it explicitly via
  `EventLoopThreadPool(loop_factory=...)`. This also removes the unstable
  free-threaded loop builds from the 3.14t default path — the root cause of
  a `Fatal Python error: Aborted` crash in worker threads on Linux 3.14.7
  (uvloop #720 `call_soon_threadsafe` / #756 segfault, both unfixed in
  uvloop 0.22.1). Removed the dead `testing.uvloop_available()` helper.
- **CI**: `astral-sh/setup-uv@v3` → `@v5` everywhere — v3 resolved `"3.14t"`
  to the GIL build on Linux, silently skipping all free-threading tests in CI.
  With v5 the `3.14t` matrix job now runs the real free-threaded interpreter.
  Also fixed `cargo test` runtime libpython loading (Linux LD_LIBRARY_PATH,
  macOS DYLD_FALLBACK_LIBRARY_PATH, Windows base_prefix via cygpath) and the
  `3.15-dev` Python request (uv rejects it; `3.15` resolves to the 3.15 beta).
- **`.python-version`**: `3.14` → `3.14t` — the file previously pinned the GIL
  build, so a bare `uv sync` on a fresh clone created a GIL venv and every
  benchmark silently ran single-threaded. The `t` suffix makes uv default to
  the free-threaded interpreter locally, matching what CI already builds and
  tests.
- **Publish workflow**: wheel smoke tests re-run the full suite including
  `free_threading` tests (removed the `not free_threading` deselection that
  was hiding the two bugs fixed above).
- **CI hang protection**: added `pytest-timeout` (180s, thread method) so a
  deadlocked test fails with a thread-stack dump instead of spinning a CI job
  until the runner timeout.

## [0.1.0] - 2026-08-03

### Added
- **Core Engine**: Multi-Event-Loop Thread Pool (`EventLoopThreadPool`) with microsecond Event Loop Lag adaptive probe scheduling for Python 3.14t Free-Threaded (No-GIL).
- **Rust Extension (`_gsyncio_core`)**: High-performance lock-free primitives written in Rust (PyO3 0.29 + `flume` + `parking_lot`).
- **Golang Parity Primitives**:
  - `FastChannel` & `AsyncChannel` with `async for item in ch:` iteration support.
  - `select_channel(*channels)` for multi-channel multiplexing.
  - `AsyncContext` for cross-thread cascading task cancellation.
  - `AsyncWaitGroup`, `AsyncOnce`, and `AsyncRWMutex`.
- **Framework Integrations**:
  - `ConnectionPinningServer` (Socket connection pinning).
  - `GsyncioASGIWorker` (ASGI 3.0 & FastAPI compatibility).
- **Production Hardening**:
  - Auto-Healing for worker thread crashes.
  - Health metrics API (`get_metrics()`).
  - PEP 561 type stubs (`py.typed` and `__init__.pyi`).
