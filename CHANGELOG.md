# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2026-08-15

### Fixed
- **`AsyncWaitGroup`**: unified atomic state machine `(counter, generation, waiters)` under a single mutex, ensuring `add(-n)` waking zero correctly transitions generation and hands over waiters without cross-generation premature wakeup races.
- **`select_channel`**: re-architected into a 2-phase deterministic single-future multi-registration arbiter with immediate non-blocking fast-probe (`timeout=0` no longer falsely times out with ready data), eliminating speculative cancellation overhead and recursion.
- **`CancelScope`**: symmetric single-ledger `_injected` tracking and immutable `tuple` contextvar stack, ensuring multi-layer nested and cross-task cancellations accurately preserve outer pending cancellation counts.
- **`EventLoopThreadPool`**: full 3-phase shutdown state machine (`RUNNING` -> `DRAINING` -> `TERMINATED`) with unlimited worker poller capability during drain phase to prevent task dropping.
- **`ConnectionPinningServer`**: added explicit `sock_guard` RAII ownership protection during cross-thread socket handoffs to prevent file descriptor leaks on connection handshake failures.
- **`CapacityLimiter`**: token absorption during downscale deficit and offset accounting during upscale to prevent phantom permit minting.
- **`Condition` & `Barrier`**: robust cleanup on non-`CancelledError` `BaseException` interruptions and cross-round generation guards during `abort()`.

## [0.1.4] - 2026-08-13

### Fixed
- **`FastChannel`** (R10 P1): a cancelled `send()`/`recv()` whose waiter
  entry was already popped by the other side now **forwards the wakeup** to
  the next putter/getter — previously the freed slot (or buffered item)
  idled while later waiters slept forever (lost-wakeup; same chain
  forwarding as `Lock`/`Semaphore`/`Condition`).
- **`TaskGroup.start()`** (R10 P2): on 3.14 `task.exception()` raises
  `CancelledError` for cancelled tasks — the `start()` cleanup no longer
  calls it on a cancelled child. A child cancelled **before** `started()`
  makes `start()` propagate the cancellation **and** cancel siblings
  (previously the siblings were never cancelled); a child cancelled
  **after** `started()` is a completed start protocol and the handle is
  returned (trio parity).
- **`EventLoopThreadPool.wait_closed()`** (R10 P4): returns immediately on a
  never-started pool (docstring promise) and now polls instead of
  `asyncio.to_thread(threading.Event.wait)` — the blocked thread was not
  cancellable and hung `asyncio.run`'s executor shutdown.
- **`CancelScope`** (R10 P5): the injection ledger (`_injected`) is now
  recorded from the actual `task.cancel()` outcome, and the `TaskGroup`
  failure path consumes its own injection via `_take_injected()` — a
  cross-thread cancellation landing during child-failure handling is no
  longer swallowed by the `__aexit__` compensation.
- **`NativeWorkerPool.push_local`**: a closed pool now reports
  `ThreadPoolClosedError` instead of "Worker index out of range" (the
  sender list was cleared before the advisory flag was set).

### Changed
- **`ConnectionPinningServer`**: the acceptor list is now guarded by
  `_running_lock` (start's reset/append and close's snapshot/clear are one
  critical section) — a race on free-threaded builds could leave an
  acceptor uncancelled at shutdown.
- Removed the dead `_reset_token` field from `CancelScope`; corrected the
  `AtomicMetrics`/`FastChannel` protocol stubs to match the real Rust
  signatures.

### Docs
- `README.md` (+ ZH mirror): new "Current Status & Known Limits" section —
  verified performance numbers (3.5x CPU-bound / 2.5x mixed / 1.25x
  pure-I/O on 4 workers), the known-limits table with escape hatches, and
  the experimental (3.14t, 0.x API) positioning.

## [Unreleased]

### Fixed
- **`TaskGroup`**: cancelled children are no longer reported at group exit —
  an injected cancellation (`cancel_all()`, the `start()` failure path, or an
  external `task.cancel()`) previously left a `CancelledError` in the
  soft-exit branch that was raised into the host task, spuriously cancelling
  it (trio/anyio/stdlib parity; the discriminator is the dead task's
  `cancelling()` count — 0 for a self-raised `CancelledError`, > 0 for an
  injected one — so `select_channel`'s notifier readiness signal is
  unaffected).
- **`TaskGroup`**: when the host is cancelled while the group waits for its
  children, the remaining children are now cancelled **and awaited** before
  the block exits — previously the group returned while a child's `finally`
  had not yet run (structured-concurrency guarantee restored; anyio parity).
- **`TaskGroup`**: children spawned before the first entry are now tracked by
  that entry instead of being silently orphaned when `__aenter__` cleared the
  child set.
- **`EventLoopThreadPool`**: removed the dead `_notify_all_workers()` calls
  from `close()`/`abort()` (they ran after `_running` was cleared and always
  returned immediately; workers wake via their poll timeout and `loop.stop`).

## [0.1.3] - 2026-08-12

### Fixed
- **`CancelScope`**: the scope's own injected cancellation is now consumed on
  exit even when the body caught the `CancelledError` (or the injection was
  never delivered) — a leaked `cancelling()` count was snapshotted by an
  enclosing shield as a real cancellation and re-injected, raising a spurious
  `CancelledError` in unrelated code (anyio `_pending_uncancellations`
  parity). Injection accounting (`_injected`, under `_cancel_lock`) ensures
  the exit compensation only consumes injections that actually happened, and
  is reset on re-entry.
- **`checkpoint()`**: the raise now consumes the task's pending cancellation
  count, making it the single delivery — previously the pending `_must_cancel`
  fired a second `CancelledError` at the next real await (double delivery).
- **`AsyncOnce`**: a cancelled leader re-raises `CancelledError` itself, but
  followers and later callers now receive `RuntimeError("AsyncOnce execution
  was cancelled")` (chained via `__cause__`) instead of the stored
  `CancelledError` — raising a user-level `CancelledError` in an unrelated
  task silently marked that task as cancelled.

### Changed
- Code comments across `src/`, `tests/`, `examples/`, and `benchmarks/` are
  now English-only (no mixed-language comments).

### Docs
- `docs/API.md` (+ ZH mirror): `checkpoint()` single-delivery semantics,
  `CancelScope` exit consumption of its own injection, `AsyncOnce`
  cancelled-leader failure semantics.

## [0.1.2] - 2026-08-11

### Fixed
- **Concurrency audit (U1–U8)**: `CancelScope` aenter rollback, expired-
  deadline triage (`fail_after(0)` → `TimeoutError`, `move_on_after(0)`
  silent), NaN deadline rejection, and shield cancellation-count
  restoration on aenter-raise paths.
- **`Lock`/`Semaphore`/`CapacityLimiter`**: same-task re-entrant acquire
  raises `RuntimeError` (asyncio parity); `Semaphore.release()` beyond
  `max_value` raises `ValueError`; `Semaphore(0)` is legal; the limiter
  tracks borrows independently (`available + borrowed == total` survives
  resizes); `max_value` reads are locked.
- **`AsyncRWMutex`**: release paths run under a cancellation shield;
  reader→writer / writer→reader / writer→writer nesting raises
  `RuntimeError` (depth-counted reader registration).
- **`TaskGroup`**: `start_soon()`/`start()` after exit raise `RuntimeError`
  (orphan task is cancelled); re-entry clears stale children so a
  body-exception exit cannot resurface old `CancelledError`s.
- **`select_channel`**: raises `ChannelClosedError` when **all** channels
  are closed and empty (was: hang forever); closed channels with buffered
  data still report ready; partially closed selects keep waiting.
- **`EventLoopThreadPool.abort()`**: completes every outstanding future
  (was: up to thousands hung); delivery unified through a race-safe
  wrapper; caller `contextvars` now propagate to worker tasks.
- **`AsyncContext`**: finished submissions are dropped from the tracking
  set (done-callback).
- **`ConnectionPinningServer`**: `start()` is idempotent; handler
  exceptions are consumed (no "exception was never retrieved" noise).
- **Docs**: EN/ZH synced for closed-channel semantics, barrier
  cancellation caveat, O(n²) cancellation-storm limitation, diagnostic
  property semantics, `AsyncOnce` recursion deadlock, and
  `AsyncWaitGroup` cancellation entries.

## [0.1.1] - 2026-08-10

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
- **API slim-down (audit FIX-8)**:
  - `open_channel()` — top-level facade removed; construct `FastChannel()`
    directly (it was a trivial async wrapper).
  - `EventLoopThreadPool.submit_daemon()` — pure alias of `submit()` removed.
  - `CapacityLimiter.acquire_on_behalf_of()` / `release_on_behalf_of()` —
    no-op borrower slots removed; `acquire()` / `release()` are the API.
  - `testing.assert_checkpoints()` and its `checkpoints` fixture — never
    used by the test suite, removed.
  - `BarrierWaitResult.fulfilled` — removed; a normal `Barrier.wait()`
    return always means the round completed (aborts raise `RuntimeError`
    instead), so the flag carried no information.
  - `AtomicMetrics.get_metrics()` — dead Rust method removed; pool health
    metrics come exclusively from the Python `MetricsCollector`.

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
- **Concurrency audit fixes (BUG-1..BUG-10)**: fixed cancel-count leaks and
  swallowed external cancels in `CancelScope` (BUG-1/2), `None` payloads on
  `FastChannel` (send(None) then recv — BUG-5), structured-concurrency
  contract violations in `TaskGroup` and `select_channel` (child futures
  always settled before scope exit), handoff tokens now forwarded to live
  waiters, readers woken when a writer cancels, and pool lifecycle contract
  violations (submit/close races). Submission to a closed pool reliably
  raises `ThreadPoolClosedError` — native `Disconnected` errors are
  translated to the public exception type instead of leaking.

### Changed
- **CI: docs-only changes skip the heavy matrix**: a `changes` job
  (`dorny/paths-filter`, `some-with-excludes` with exact-complement
  filters) classifies every push/PR. Changes touching only `*.md` /
  `docs/**` skip lint and the 6 build/test matrix — their jobs report
  "skipped", which GitHub counts as success for required checks — and run
  only the new `Docs Drift Check` job (`tests/test_docs.py`, no Rust build:
  `uv sync --no-install-project` + `PYTHONPATH=src`). Code and mixed
  changes run the full suite; a failed classifier fails closed (everything
  runs). Branch protection now requires `Docs Drift Check` in addition to
  the existing checks.
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
- **Breaking: `submit(loop=...)` renamed to `submit(pin_to=...)`**
  (`EventLoopThreadPool`) — the parameter name now states what it does (pin
  the task to a worker event loop or index). All call sites, tests,
  examples, and docs updated.
- **`AsyncContext.parent` is now read-only** — fixed at construction;
  previously a publicly writable attribute.

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
