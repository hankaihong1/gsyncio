# AGENTS.md — AI Agent Onboarding Guide for multiloop

> **Repository**: [hankaihong1/multiloop](https://github.com/hankaihong1/multiloop)
> **License**: MIT

---

## 1. Project Identity

**multiloop** is a multi-event-loop engine and Go-style concurrency toolkit for
Python 3.14t (free-threaded / no-GIL). It wraps a Rust core (`_multiloop_core`)
built with PyO3, flume, and parking_lot, and exposes `EventLoopThreadPool`,
`Channel`, `select_channel`, `TaskGroup`, `CancelScope`, and async
synchronization primitives (`AsyncWaitGroup`, `AsyncOnce`, `AsyncRWMutex`,
`Lock`, `Semaphore`, `Condition`, `Barrier`, `CapacityLimiter`). The public API
is intentionally minimal, modeled after `asyncssh`'s top-level-facade style.

**Crucial Paradigm Distinction: API Lending vs. Single-Threaded Semantics Rejection**:
`multiloop` borrows the clean, familiar API surface and ergonomics of AnyIO and
Trio (`TaskGroup`, `CancelScope`, `CapacityLimiter`, `Event`, etc.), but
**fundamentally rejects their single-threaded, zero-contention semantic
assumptions**. AnyIO and Trio were designed for single-threaded, single-event-loop
cooperative multitasking where OS locks are unnecessary and primitives serve
merely as cooperative coroutine throttles. In contrast, `multiloop` operates under
Python 3.14t true multi-core physical parallelism across multiple OS threads and
isolated event loops. Correctness in `multiloop` cannot rely on single-threaded
cooperative heuristics; it requires **formal axiomatic reasoning, state-machine
invariants, token conservation laws, and multi-thread signal forwarding**.

---

## 2. Directory Map

```
multiloop/
├── src/
│   ├── multiloop/              # Python package (22 modules)
│   │   ├── __init__.py       # Public API surface — start here
│   │   ├── pool.py           # EventLoopThreadPool engine
│   │   ├── primitives.py     # Channel, select_channel, AsyncWaitGroup, AsyncOnce
│   │   ├── context.py        # AsyncContext (O(1) set tracking)
│   │   ├── _cancel.py        # CancelScope, fail_after, move_on_after, shield
│   │   ├── _sync.py          # Lock, Semaphore, Event, Condition, Barrier
│   │   ├── _taskgroup.py     # TaskGroup, TaskHandle
│   │   ├── rwlock.py         # AsyncRWMutex (async read-write lock, Rust-backed)
│   │   ├── asgi.py           # MultiloopASGIWorker (ASGI 3.0, Lifespan, WebSockets, H2)
│   │   ├── _http11.py        # Http11Protocol (Rust SIMD httparse & fused TCP write)
│   │   ├── _websocket.py     # WebSocketProtocol (Rust SIMD unmasking & RFC 6455)
│   │   ├── _lifespan.py      # ASGILifespanManager (ASGI lifespan state machine)
│   │   ├── wsgi.py           # MultiloopWSGIWorker (WSGI 1.0.1, PEP 3333 synchronous runner)
│   │   ├── cli.py            # multiloop run CLI server runner
│   │   ├── server.py         # ConnectionPinningServer (platform-adaptive multi-core listener)
│   │   ├── _channel_base.py  # Shared waiter deques, _wake_all, _wakeup_next
│   │   ├── _rust.py          # _try_import_rust_class helper
│   │   ├── _metrics.py       # MetricsCollector
│   │   ├── _options.py       # PoolOptions dataclass
│   │   ├── _logging.py       # get_logger, set_log_level
│   │   ├── exceptions.py     # MultiloopError hierarchy
│   │   └── testing/          # Test helpers
│   ├── lib.rs                # Rust pymodule registration & tests
│   ├── metrics.rs            # AtomicMetrics & 64-byte PaddedAtomic
│   ├── pool.rs               # NativeWorkerPool & PollerGuard
│   ├── channel.rs            # Channel, RawAsyncChannel (Anti-Barging FIFO)
│   ├── waitgroup.rs          # RawAsyncWaitGroup & WaitGroupInner
│   ├── rwlock.rs             # RawAsyncRWMutex (64-bit atomic state machine)
│   ├── websocket.rs          # FastWebSocketParser & SIMD fast_websocket_unmask
│   ├── http.rs               # FastHttpParser (SIMD httparse & zero-alloc ASGI assembly)
│   └── h2.rs                 # PyH2Bridge, serve_h2_connection (Tokio H2 Runtime)
├── tests/                    # 27 test files (pytest + pytest-asyncio)
├── benchmarks/               # 4 benchmark scripts
├── examples/                 # Runnable examples (python examples/00_*.py)
├── docs/
│   ├── API.md                # Complete API reference
│   ├── CHOOSING.md           # Primitive selection decision table (human entry point)
│   └── CONCURRENCY.md        # Concurrency correctness guide & pre-change checklist
├── Cargo.toml                # Rust crate config (edition 2021, release LTO)
├── pyproject.toml            # Python config (mypy strict, ruff, pytest)
├── Makefile                  # Convenience targets (develop, test, lint, bench)
├── .github/workflows/        # CI (ci.yml) + publish (publish.yml)
└── CONTRIBUTING.md           # Human-oriented dev setup guide
```

## 2.1 Root-Directory Quick Reference (AI use)

One line per tracked root file — what it is and whether the AI should touch it.
Anything not listed here (`.omo/`, `.codegraph/`, `.benchmarks/`, `.ruff_cache/`,
`.mypy_cache/`, `.pytest_cache/`, `.hermes/`) is a tool cache: ignore in
searches, never modify.

| File | Purpose | AI should touch? |
|---|---|---|
| `pyproject.toml` / `Cargo.toml` / `uv.lock` / `Cargo.lock` | Build & dependency manifests | Yes (when changing deps) |
| `Makefile` | Command entry point (develop/test/lint/bench/all) | Yes — prefer it over raw commands |
| `.gitignore` / `.pre-commit-config.yaml` | VCS ignores / pre-commit hooks (ruff + cargo) | Yes (when changing hooks) |
| `AGENTS.md` / `README.md` / `CONTRIBUTING.md` / `docs/` (English files) | Docs | Yes (sync when behavior changes; docs `*_ZH.md` are the Chinese mirrors — same content, do not maintain separately) |
| `build.rs` | libpython link logic (extension-module detection) | Only for cargo-test link paths |
| `deny.toml` / `rust-toolchain.toml` / `.editorconfig` / `.python-version` | Toolchain pinning | No (leave alone) |
| `LICENSE` / `CODE_OF_CONDUCT.md` / `SECURITY.md` / `CODEOWNERS` / `CITATION.cff` / `CHANGELOG.md` | GitHub ecosystem files | No (unless asked) |
| `.github/` | CI + issue/PR templates | When changing CI |
| `test_build.sh` | (deleted — was a one-liner duplicating `make develop`) | — |
| `examples/` | Runnable demos | Yes (add example when adding a feature) |

---

## 3. Key Conventions

### Python

| Tool | Configuration | Gate |
|------|--------------|------|
| **mypy** | `strict = true`, `disallow_untyped_defs = true` | Zero errors |
| **pyright** | `typeCheckingMode = "strict"` | Zero errors |
| **ruff** | `target-version = "py313"`, `line-length = 100`, select `E, F, I, N, UP, W, B, C4, SIM, TCH, RUF` | Zero errors |
| **pytest** | `asyncio_mode = "strict"` | Zero failures |

### Rust

| Tool | Configuration | Gate |
|------|--------------|------|
| **cargo clippy** | `-D warnings` | Zero warnings |
| **cargo fmt** | Edition 2021 | Clean diff |
| **release profile** | `opt-level = 3`, `lto = true`, `codegen-units = 1`, `strip = true` | — |

### General

- **Python version**: `>= 3.14` (free-threaded build recommended).
- **Package manager**: `uv` exclusively — do not use `pip`.
- **Rust toolchain**: stable (via rustup).
- **Rust extension optional**: every Python module that depends on the Rust
  core uses `_try_import_rust_class()` (`src/multiloop/_rust.py`) for graceful
  fallback when the extension is not compiled.
- **Commit style**: conventional, concise. PRs target `main`.
- **Request before code, green before merge**: for any non-trivial change,
  file an issue/request first and wait for approval before writing code.
  A PR is merged only after all required CI checks pass (lint, type checks,
  tests) — never merge a red PR, and never ask a maintainer to.
- **Docs-only exception to green-before-merge**: changes touching only
  `*.md` files or `docs/**` skip the heavy lint/build/test matrix — the
  `Docs Drift Check` job runs `tests/test_docs.py` (EN/ZH parity, code-block
  syntax, API coverage) and the project lead reviews the diff before
  merging or pushing. Anything else (code, config, CI files) runs the full
  suite; a mixed change runs both gates.
- **GitHub Actions is for CI, not debugging**: never use workflow runs as a
  debugging tool — no debug commits, no temporary diagnostic workflows, no
  bisecting platform-specific failures on remote runners (they pollute the
  Actions history and burn CI minutes). If code needs to be debugged on
  another platform's machine, ask the user to arrange access first; never
  push anything to trigger remote runs without explicit permission.
- **Docs: EN/ZH must stay in sync**: every doc has an English original and a
  `*_ZH.md` mirror (`README.md` ↔ `README_ZH.md`, `docs/*.md` ↔
  `docs/*_ZH.md`, `examples/README.md` ↔ `examples/README_ZH.md`). Any change
  to one must be mirrored to the other in the same commit — same sections,
  same headings, same code blocks; only the language differs.
  `tests/test_docs.py` enforces structural parity.

---

## 4. How to Navigate the Codebase

### Recommended reading order for AI agents

1. **`src/multiloop/__init__.py`** — Public API surface. Every exported symbol is
   re-exported here. Start here to understand the module layout.

2. **`src/multiloop/pool.py`** — Core engine. `EventLoopThreadPool` orchestrates
   OS threads running isolated asyncio event loops. The `submit()` method has
   two paths (global queue via `push_global`, local queue via `push_local`).
   `_worker_dispatcher` is the per-worker event loop that calls `pop_work()`
   and spawns tasks.

3. **`src/lib.rs`** — Rust backend. Four `#[pyclass]` types: `NativeWorkerPool`
   (global + per-worker flume queues, batch-pull pop_work), `Channel`
   (lock-free flume channel), `AtomicMetrics` (padded atomic counters),
   `RawAsyncWaitGroup` (atomic counter + mutex-protected waiter list).

4. **`src/multiloop/primitives.py`** — `Channel` Python wrapper with the
   double-checked lock pattern, `select_channel`, `AsyncWaitGroup`,
   `AsyncOnce`.

5. **`src/multiloop/_cancel.py`** — `CancelScope` with shield semantics, using
   `task.uncancel()` / `task.cancel()` snapshot/restore on Python 3.11+.

### For bug investigation

- Run the failing test: `uv run pytest tests/<file>.py::<test_name> -xvs`
- Stress-test for race conditions (**single target file only, never the whole suite**):
  `uv run pytest tests/<file>.py -p no:cacheprovider --count=50`
- Run last-failed tests: `uv run pytest --lf`

### Module dependency map (simplified)

```
__init__.py  ──┬── pool.py ────────── _rust.py ── _multiloop_core (lib.rs)
                ├── primitives.py ──── _channel_base.py
                ├── _cancel.py
                ├── _sync.py
                ├── _taskgroup.py
                ├── context.py
                ├── rwlock.py
                ├── asgi.py ────────── server.py
                ├── exceptions.py
                ├── _metrics.py
                ├── _options.py
                └── _logging.py
```

## 4.1 Mandatory reading before touching concurrency code

Any change touching channels, locks, semaphores, events, conditions, barriers,
waitgroups, cancellation, or the pool **must** pass the checklist in
[`docs/CONCURRENCY.md`](docs/CONCURRENCY.md) (sections 1-3). Hard rules that
hold regardless:

1. **Read `docs/CONCURRENCY.md` first** — the eight trap patterns and the
   pre-change checklist are the operating manual; the six design decisions
   below are the "why" summary.
2. New or modified waiter registration/wakeup paths need a lock-free fast
   path **plus a re-check under the lock** (lost-wakeup is the #1 bug class
   here).
3. Cancellation paths re-acquiring locks must run under a cancellation
   shield; waiter removal must carry generation/identity checks.
4. New public APIs must update `docs/API.md`, `__init__.pyi`, and (for new
   features) an `examples/` script — `tests/test_docs.py` enforces the first
   two.
5. Concurrency behavior changes must be stress-tested with
   `pytest-repeat --count=50` on the **specific target test file or function**,
   never across the entire test suite or multiple socket-heavy files at once.
6. **Bugs require a minimal reproduction test first** (deterministic data,
   stable FAIL, `--count=50` on that target test to confirm reproduction rate);
   a fix is not done until that test passes stably.
7. **GitHub Actions is for CI, not debugging** — no debug commits, no
   temporary diagnostic workflows, no remote-runner bisecting. If code needs
   debugging on another platform's machine, ask the user to arrange access
   first; never push to trigger remote runs without explicit permission.
8. **Request before code, green before merge** — file an issue/request and
   wait for approval before writing non-trivial code; a PR merges only after
   all required CI checks pass. Exception: docs-only changes (`*.md`,
   `docs/**`) skip the heavy matrix — `Docs Drift Check` runs
   `tests/test_docs.py` and the project lead reviews the diff before merge.

---

## 5. Architecture Highlights

### Scheduler: round-robin with work-stealing

Unpinned tasks land on a single global unbounded flume queue (the "global
queue"). Every worker thread runs an independent asyncio event loop and pulls
work via `NativeWorkerPool::pop_work()` with this priority order:

1. **Private buffer** (per-worker `VecDeque` — cached from previous batch pulls)
2. **Global queue** (batch-pull under a soft poller gate)
3. **Local dedicated channel** (for loop-pinned tasks)

The soft poller gate (`num_polling < max(num_workers / 2, 1)`) limits
concurrent global-queue pollers to ~half the workers, preventing contention
while allowing any idle worker to steal. Batch size is
`min(global_len / num_workers + 1, 128)` for fair-share distribution.

On the Python side, `_worker_dispatcher` calls `await asyncio.sleep(0)` between
pops so no single worker greedily drains the global queue.

### Rust core components

| Component | Source file | Crate | Purpose |
|---|---|---|---|
| `NativeWorkerPool` | `src/pool.rs` | PyO3 + flume | Lock-free global queue + per-worker bounded (256) local channels, batch-pull `pop_work`, soft poller gate |
| `Channel` | `src/channel.rs` | flume | Lock-free bounded/unbounded channel, exposed to Python as `multiloop.primitives.Channel` |
| `RawAsyncChannel` | `src/channel.rs` | parking_lot + PyO3 | Python-independent async channel core with anti-barging FIFO ordering protection |
| `AtomicMetrics` | `src/metrics.rs` | `std::sync::atomic` | 64-byte-padded per-worker counters (`active`, `completed`, `global_pull_count`, `park_count`, etc.) — prevents false sharing |
| `RawAsyncWaitGroup` | `src/waitgroup.rs` | parking_lot `Mutex<WaitGroupInner>` | Go-style atomic counter + generation + waiter list with single-mutex state machine |
| `RawAsyncRWMutex` | `src/rwlock.rs` | parking_lot `Mutex<RWMutexInner>` | 64-bit atomic state machine, writer-preference async read-write lock with cancellation cleanup |
| `FastHttpParser` | `src/http.rs` | httparse | SIMD zero-copy HTTP/1.x header parser with zero-alloc ASGI PyList direct assembly & interned methods |
| `FastWebSocketParser` & `fast_websocket_unmask` | `src/websocket.rs` | PyO3 | SIMD 64-bit chunked XOR frame unmasking and zero-copy RFC 6455 frame header parsing |
| `PyH2Bridge` & `serve_h2_connection` | `src/h2.rs` | tokio + h2 | Tokio HTTP/2 async multiplexing runtime bridge |

### Python-side patterns

- **`_try_import_rust_class(module, name)`** (`_rust.py`): Every module that
  depends on `_multiloop_core` imports Rust classes through this function.
  Returns `None` if the extension is missing — no import-time crash.

- **`CancelScope` with shield** (`_cancel.py`): Uses `task.cancelling()` /
  `task.uncancel()` snapshot on enter, `task.cancel()` restore on exit, so
  cleanup code (like re-acquiring a lock in `Condition.wait()`) runs to
  completion even under cancellation.

- **`Condition` latch & cancellation forwarding** (`_sync.py`): `notify()`
  does NOT require the underlying lock. Waiters register via sticky
  `asyncio.Event` under a separate `_waiters_lock`. If a waiter is cancelled
  while re-acquiring the lock after being notified, it automatically forwards
  the notification to the next waiter to conserve notification tokens.

### Nine non-obvious design decisions

These are the places where a change that looks like a simplification usually
breaks a real correctness or performance property. **Read each before modifying
the related code.**

1. **Double-check lock in `Channel`** (`primitives.py:150-227`):
   The flume queue is lock-free, but the Python waiter deques are not. A
   fast-path `try_send`/`try_recv` outside the lock, followed by a re-check
   under the lock, closes the lost-wakeup window between the lock-free buffer
   and the locked waiter list.

2. **`CancelScope` shield semantics** (`_cancel.py:97-205`):
   Shielded scopes snapshot and clear `task.cancelling()`, then restore it on
   exit. This lets cleanup blocks (like lock re-acquisition) run to completion
   without the next `await` re-raising `CancelledError`. Without it,
   `Condition.wait()` deadlocks on cancel.

3. **Condition notification forwarding on re-acquire cancellation** (`_sync.py:624-646`):
   When a `wait()` coroutine is awakened by `notify()` but receives a cancellation
   request while waiting to re-acquire the underlying mutex, the notification
   token has already been deducted. The cancellation handler MUST forward
   `_forward_notify()` to ensure subsequent waiters are not starved.

4. **Barrier automatic Broken state machine & generation tracking** (`_sync.py:774-896`):
   A `_generation` counter ties every waiter to the exact round it joined.
   On cancellation of any waiting party before round completion, the barrier
   automatically marks itself broken, wakes all remaining waiting parties with an
   error, and advances the generation, preventing deadlocks without requiring manual abort.

5. **Condition latch** (`_sync.py:539-744`):
   `notify()` does not require the lock, avoiding the notifier-blocks-while-
   sleeper-sleeps deadlock. Sticky `asyncio.Event` + register-before-release
   closes the lost-notification window.

6. **Work-stealing with the `num_polling` gate** (`src/pool.rs:170-200`):
   A soft atomic gate (`num_polling < max(num_workers/2, 1)`) limits
   concurrent global-queue pollers. The gate is momentary (acquired per
   `pop_work` call, released immediately via `PollerGuard`), so any worker can
   win it — no hard role assignments that drift with load.

7. **Rust global queue and batch pull with 3-source drain** (`src/pool.rs:160-230`):
   Three-tier consumption: private buffer → global batch pull → local channel.
   The push side falls back from local to global when a per-worker channel is
   full, preserving liveness. Drain phase validates all three sources (global queue,
   private buffer, local channel) are completely empty before worker loop termination.

8. **Darwin vs Linux `SO_REUSEPORT` asymmetric routing trap** (`src/multiloop/server.py:99-130`):
   Linux `SO_REUSEPORT` hashes 4-tuples across multiple listening sockets with kernel-level
   load balancing. On macOS (Darwin/BSD), `SO_REUSEPORT` does not implement symmetric
   hashing and routes 100% of loopback connections to the newest bound socket, causing
   severe worker starvation and degrading multi-core throughput to single-worker limits (~50k QPS).
   `server.py` restricts `SO_REUSEPORT` to Linux only; on macOS/Darwin, workers share a single
   listening socket and race concurrently on `loop.sock_accept(shared_sock)`, guaranteeing
   perfect 25% load distribution and ~91k+ multi-core QPS.

9. **Python 3.14t Free-Threaded memory safety via `PyBytes::new_with`** (`src/websocket.rs:80-110`):
   Under Python 3.14t (No-GIL), modifying Python buffer objects in-place across threads
   causes data races and invalidates Python's immutability invariant. Rust SIMD extensions must
   allocate Python objects through atomic single-pass constructors like
   `PyBytes::new_with(py, len, |buf| ...)` to populate unmasked payload bytes without
   intermediate copies or in-place mutations.

---

## 6. Quick Commands

All commands assume you have run `uv sync --group dev` first.

### Build (required before first import)

```bash
make develop          # or: uv run maturin develop --release
```

### Test

```bash
make test             # full test suite
uv run pytest -x -m "not slow"   # skip slow benchmarks
uv run pytest --lf               # re-run last failures
```

### Lint & Type Check

```bash
make lint             # ruff + mypy + pyright + cargo clippy + cargo fmt --check

# Individual:
uv run ruff check .
uv run mypy --strict src/multiloop
uv run pyright src/multiloop
cargo clippy -- -D warnings
cargo fmt --check
```

### Format

```bash
make format           # ruff format . + cargo fmt
```

### Benchmarks

```bash
make bench
```

### Full pre-submission check (all gates)

```bash
make all              # develop + lint + test
```

---

## 7. Key Differences from CONTRIBUTING.md

`CONTRIBUTING.md` is the **human-oriented dev setup guide** (prerequisites,
step-by-step install, PR process, Code of Conduct). This `AGENTS.md` is the
**AI-oriented architecture and navigation guide** — it emphasizes module
layout, design decisions, tooling gates, and reading order. Both documents
reference the same commands and conventions, but serve different audiences.
