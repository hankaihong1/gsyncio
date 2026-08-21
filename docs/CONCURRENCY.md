# multiloop Concurrency Correctness Guide

**[中文版 (Chinese)](CONCURRENCY_ZH.md)**

> **Read before touching any concurrency-related code.** This guide serves
> two audiences: AI agents (walk through the Section 3 checklist before
> writing code) and human contributors (understand each primitive's
> concurrency model before editing). Every claim cites its source location —
> verify by jumping there.

---

## 1. Concurrency Model Overview (who locks what, who waits for what)

All multiloop synchronization primitives follow the same skeleton: a
**`threading.Lock` on the Python side protects the waiter structures, while
the Rust side carries the data plane with atomics + flume**. Cross-thread
wakeups always go through `loop.call_soon_threadsafe(...)` — asyncio objects
are never touched directly from another thread.

### 1.0 Physical Concurrency Axioms (Axiomatic Assumptions)

multiloop borrows the clean, developer-friendly API surface of AnyIO and Trio, but **fundamentally diverges from their single-threaded runtime assumptions**. In single-threaded cooperative engines (AnyIO/Trio), coroutine execution between `await` points is strictly sequential and non-preemptive; data structures require no OS mutexes and primitives act merely as coroutine scheduling throttles.

In contrast, multiloop operates under **Python 3.14t (free-threaded / no-GIL) true multi-core physical parallelism** across multiple OS threads and isolated event loops. Correctness cannot rely on single-threaded heuristics; it rests on seven formal physical concurrency axioms:

0. **Paradigm Divergence from AnyIO/Trio (API Lending vs. Multi-Threaded Physics)**: AnyIO/Trio APIs (`TaskGroup`, `CancelScope`, `CapacityLimiter`, `Event`, etc.) are adopted strictly as an ergonomic interface layer. All underlying semantics are redesigned from first principles for multi-thread / multi-loop execution, requiring formal state-machine proofs, token conservation laws, and OS-level mutex synchronization.
1. **Python 3.14t Free-Threaded True Parallelism**: Bytecode and object operations execute concurrently across CPU cores without a GIL. Any composite state transitions (e.g. counter updates + waiter notifications) must be enclosed within a single OS mutex (`threading.Lock` / `parking_lot::Mutex`) to prevent state tearing.
2. **Cross-Thread EventLoop Isolation**: `asyncio.Task`, `Future`, and `Event` are strictly bound to a single thread and its event loop. Cross-thread notifications must strictly go through `loop.call_soon_threadsafe(...)` or Rust `Channel`.
3. **ContextVar Thread/Task Locality**: `contextvars.ContextVar` is strictly local to the active OS thread and task. Cross-thread cancellation (`scope.cancel()`) must never read or modify `ContextVar` state; it relies exclusively on thread-safe locks and `call_soon_threadsafe`.
4. **Native asyncio Cancellation & Single-Ledger Symmetric Accounting**: Built upon Python 3.11+/3.14t native `task.cancelling()` / `task.uncancel()`. A scope only compensates (`task.uncancel()`) for cancellations it explicitly injected (`_injected == True`), preventing accidental absorption of foreign cancellations. Shielding is a snapshot-and-restore mechanism.
5. **Data Plane vs Wait Plane Separation**: Data transfer is carried out lock-free in Rust (`flume` + 64-byte-padded atomic counters), while async waiters are tracked under Python `threading.Lock` using the double-check lock pattern.
6. **Structured Concurrency Physical Scope**: `TaskGroup` is physically scoped to a single `asyncio.AbstractEventLoop`. Multi-loop concurrency is coordinated via `EventLoopThreadPool`, `Channel`, and `AsyncContext`.

### 1.1 Channels (Channel)

| Component | Lock / primitive | Waiter structure | Key invariant |
|---|---|---|---|
| Rust `Channel` | flume channel (bounded/unbounded) + `AtomicBool is_closed` | none | `try_send` returning `false` means only "full"; **closed ⇒ errors once drained** — a send racing `close()` may still enqueue (the flume side is closed lazily), so a "closed" channel can briefly accept then drain, after which every operation errors (`src/lib.rs:387-418`; R4 decision: tolerant vs Go's panic-on-send-after-close) |
| Python `_BaseChannel` | `threading.Lock` (`_lock`) | `_getters` / `_putters` deques of `(loop, future)` | waiter registration and wakeup must happen under `_lock` (`src/multiloop/_channel_base.py:75-78`) |
| Wakeup protocol | — | — | `_wake_all` consumes from the deque **left side**: one wakeup pops one entry, stale futures are dropped naturally (`_channel_base.py:29-50`) |

**Data plane and wait plane are separated**: flume carries data (lock-free),
the Python lock only tracks "who is waiting". `send`/`recv` follow
「lock-free fast path → re-check under the lock → register future under the
lock → await → unregister under the lock on cancellation」
(`_channel_base.py:136-177`, `primitives.py:165-197`).

### 1.2 Locks & Semaphores

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Lock` | `threading.Lock` | `deque[(task, asyncio.Event)]` | FIFO; a dead owner (`_owner.done()`) is recycled automatically so the lock never leaks (`_sync.py:56-62`); `release()` skips done waiters (`_sync.py:90-97`) |
| `Semaphore` | `threading.Lock` | `deque[(loop, asyncio.Event)]` | FIFO; if a cancelled waiter was already popped by `release()`, its token is **forwarded** to the next waiter or returned to the pool (`_sync.py:192-213`) |
| `CapacityLimiter` | `threading.Lock` (`_lock`) | `deque[(loop, asyncio.Event)]` | Single-lock atomic model: `available + borrowed == total` under `_lock`; atomic borrow, return, and deficit absorption under 3.14t (`_sync.py:329-497`) |

### 1.3 Events & Conditions

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Event` | `threading.Lock` | `list[(loop, asyncio.Event)]` | **sticky** (trio semantics, no clear); `set()` swaps the list out under the lock, then wakes each waiter via `call_soon_threadsafe` outside the lock (`_sync.py:385-398`) |
| `Condition` | `_waiters_lock` (waiter queue) + underlying `Lock` | `deque[(loop, asyncio.Event)]` | `notify()` does **not** require the underlying lock (`_sync.py:507-522`); `wait()` releases the lock → waits → **re-acquires under a shield with cancellation notification forwarding** (`_sync.py:624-646`) |

### 1.4 Barriers & Group Sync

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Barrier` | `threading.Mutex` | `list[(loop, asyncio.Event, box)]` | `_generation` counter binds each waiter to its round; on cancellation of any party before round completion, the barrier automatically enters Broken state, wakes all remaining parties with an error, and advances generation (`_sync.py:774-896`) |
| `AsyncWaitGroup` | Rust: `parking_lot::Mutex<WaitGroupInner>` | `Vec[(loop, future)]` | Single mutex guards `counter`, `generation`, and `waiters`; `add(-n)` and `done()` to zero atomically bump `generation` and hand over the whole waiter list via `mem::take` (`lib.rs:465-540`); `register_waiter` wakes immediately if counter is zero; `holding()` provides RAII tracking |
| `AsyncOnce` | `threading.Lock` | `deque[(loop, future)]` | leader/follower: the lock decides who executes; followers register under the lock; the leader `_wake_all`s **while holding the lock** in `finally` — registration and wakeup share one lock, so there is no lost-wakeup window (`primitives.py:377-416`) |

### 1.5 Cancellation & Structured Concurrency

| Component | Mechanism | Key point |
|---|---|---|
| `CancelScope` | per-task contextvars stack + single-ledger `_injected` | shield snapshots and clears the cancellation count on entry, restores it on exit; single ledger tracks exact injections and symmetrically uncancels only what was injected; strict RAII scope stack lifetime (`_cancel.py`) |
| `select_channel` | 2-phase arbiter (Phase 1 fast `try_recv` with pseudo-random uniform start, Phase 2 multi-channel registration with unicast wakeup) | readiness is reported without TaskGroup speculative cancellation; winner consumes via `try_recv()` and unregisters all watcher tokens in `finally` (`primitives.py:230-290`) |

### 1.6 HTTP & WebSocket Protocol Concurrency Model (Pure Messenger vs Rust Protocol Calculator)

| Component | Architecture Role | Threading / State Guard | Key Invariant |
|---|---|---|---|
| Rust `FastHttpConnection` | Protocol State Machine & Calculator | Thread-local to owning Worker EventLoop | 100% of HTTP parsing, Chunked stream decoding, RFC 9112 smuggling defense, CRLF injection sanitization, single-pass `PyBytes::new_with` wire serialization, and WebSocket RFC 6455 frame fusion (`src/http.rs`). |
| Python `Http11Protocol` | Pure Transport Messenger | Pinned to single Worker EventLoop | ~380 lines handling raw Socket I/O, `_body_queue` backpressure with `pump_events()` residue draining, and ASGI 3.0 lifecycle dispatching (`src/multiloop/_http11.py`). |
| Python `WebSocketConnection` | Full-Duplex RFC 6455 Session | `_send_lock` (Lock) + Rust `Channel` (`_inbound_channel`) | Cross-loop broadcasts must trampoline to `self._home_loop` via `run_coroutine_threadsafe`; `_inbound_channel` provides multi-thread safe queueing (`src/multiloop/_websocket.py`). |

---

## 2. Known Trap Patterns (Race-Condition Trap Patterns)

For each pattern: symptom → root cause → correct approach → source example.
Check your change against these, then walk the Section 3 checklist.

### Pattern 1: check-then-act window

**Symptom**: the two steps of "check then act" are mutated concurrently, so
the operation fails or reads stale state.
**Root cause**: the check and the act are not atomic.
**Correct approach**: merge into a single locked operation; when that is
impossible, fall back with try/except.
**Examples**:
- `CapacityLimiter` holds all token, borrowed, and waiter state under a single `_lock`; reading `snapshot()` atomically yields `(total, avail, borrowed)` (`_sync.py:329-497`).
- General lesson (fixed elsewhere): `contains_key → getitem` can lose the key
  to a concurrent delete → split the two steps + `try/except KeyError`.

### Pattern 2: double-checked lock lost-wakeup

**Symptom**: a sender enqueues and wakes a receiver that "no longer exists",
or a receiver registers its future after the data was already taken — the
future is never resolved → hang.
**Root cause**: the lock-free fast path (`try_send`/`try_recv`) and the
locked waiter list are two different state surfaces; when the fast path wins
over the slow path, the wakeup signal is lost.
**Correct approach**: after the fast-path attempt, **re-check under the
lock** before registering the future; registration and `_wakeup_next` both
happen under `_lock`.
**Examples**: `_wait_and_send` (`_channel_base.py:136-177`),
`_recv_impl` (`primitives.py:165-197`) — the "Double-check lock in
Channel" design decision in AGENTS.md.

### Pattern 3: lock re-acquisition on the cancellation path (missing shield → deadlock)

**Symptom**: after `Condition.wait()` is cancelled, re-acquiring the lock in
the cleanup path immediately re-raises `CancelledError`; the lock is never
taken back → the task is stuck in cleanup and the lock leaks.
**Root cause**: Python cancellation re-raises at every `await`, so cleanup
code cannot `await`.
**Correct approach**: retry-loop re-acquisition — the canonical
`asyncio.Condition.wait` pattern (bpo-34094 family): swallow
`CancelledError` from each re-acquire attempt and retry until the lock is
held, then re-raise the cancellation.  Each swallowed cancellation leaves
`Lock` clean (its cancel path discards the waiter entry), so retrying is
safe — and it survives *repeated* cancels during the re-acquisition, which a
`CancelScope(shield=True)` wrapper cannot do: that shield is
snapshot/restore only and absorbs just the cancellations already injected
*before* entry; a cancel delivered while inside the shield interrupts the
body's await.
**Examples**: `Condition._reacquire_lock` (`_sync.py:718-738`); the
`AsyncRWMutex` reader/writer release paths (`rwlock.py:78-111, 153-175`).

### Pattern 4: notify/wait lock mismatch

**Symptom**: the notifier must hold some lock to `notify()`, but the waiter
has **already released that lock** before waiting → the notifier can never
get the lock → deadlock.
**Root cause**: the lock required by `notify` differs from the lock released
by `wait`, or `notify` is wrongly required to hold the lock.
**Correct approach**: `notify()` must not require the underlying lock; the
waiter queue is guarded by a separate `_waiters_lock` — `notify` only touches
the queue, never the business lock.
**Examples**: `Condition.notify()` (`_sync.py:507-522`) — a lock-gated notify
would also let producers batch notifications and starve waiters.

### Pattern 5: round/generation mismatch (wrong-round removal)

**Symptom**: a cancellation/timeout handler from the previous round deletes a
**next-round** waiter entry, so the barrier never reaches its party count.
**Root cause**: waiter entries are reused, but the removal logic does not
know the entry now belongs to a new round.
**Correct approach**: record the `_generation` when the waiter registers;
the cancellation handler compares the generation before deciding to remove.
**Examples**: `Barrier.wait()` (`_sync.py:628-643`); the same technique in
`Semaphore._cancel_waiter` (token forwarding instead of blind removal,
`_sync.py:192-213`).

**Barrier Broken note**: the generation guard protects *next-round* entries, and a cancelled party automatically transitions the barrier into a Broken state, waking all current round parties with an error to eliminate deadlocks.

**Known limitation**: the cancellation-handler removal in
`Barrier`/`Condition`/`Semaphore` is O(n) per waiter (list/dict rebuild), so
a cancellation storm on N parked waiters costs O(n²) — measured 5000
waiters ≈ 560 ms. Acceptable for realistic party counts;
documented so nobody "optimises" it into a wrong-round bug.

### Pattern 6: register-after-done race

**Symptom**: `wait()` checks "counter is already 0" and is about to
register, when `done()` decrements to 0 and clears the waiter list → the new
waiter is never woken.
**Root cause**: the window between the check and the registration.
**Correct approach**: the registration call returns whether it is already
done; if so, return immediately without waiting (same semantics as Go
`sync.WaitGroup.Wait`).
**Examples**: `AsyncWaitGroup.wait()` + the boolean return of
`register_waiter` (`primitives.py:332-339`, `lib.rs:473-487`), guarded under `parking_lot::Mutex`. Note: **all `add()` calls with a positive delta must happen-before `register_waiter()`** to ensure callers do not observe a stale counter of 0 before workers start.

### Pattern 7: sharing asyncio objects across threads

**Symptom**: an `asyncio.Event`/`Task` owned by one loop is set/cancelled
directly from another thread → `RuntimeError` or silent loss.
**Root cause**: asyncio objects are not thread-safe;
`call_soon_threadsafe` is the only safe cross-thread injection channel.
**Correct approach**: cross-thread, always use
`loop.call_soon_threadsafe(fut.set_result, ...)` or
`call_soon_threadsafe(event.set)`; communicate between threads with
`Channel`, never raw asyncio primitives.
**Examples**: `_wake_all` (`_channel_base.py:41-48`), `Event.set`
(`_sync.py:397-398`), `Lock.release` (`_sync.py:96`).

### Pattern 8: the deadlock family (lock order / holding locks across await / recursive locking)

**Symptom**: two threads each hold the lock the other wants; or a lock is
held across `await`; or the same thread re-acquires a non-reentrant lock.
**Correct approach**:
- with multiple locks, fix the acquisition order (e.g. always take
  `_total_lock` before touching the semaphore's internal lock);
- **never `await` while holding a lock** (`threading.Lock` cannot survive an
  await — equivalent to releasing the lock while believing you hold it;
  only `asyncio.Lock` has that semantics);
- `drop(guard)` before calling a function that may take locks again.
**Examples**: `push_local` drops the guard before falling back to
`push_global` (`lib.rs:276-280`).

### Pattern 9: component anti-patterns and misuse traps

**Symptom**: Runtime errors, unexpected deadlocks, starvation, or livelocks caused by breaking primitive-specific contracts.

**1. `AsyncWaitGroup.track` coroutine object passed to callable acceptor**:
- **Trap**: `wg.track(coro)` immediately increments the counter and returns a coroutine object wrapping `finally: self.done()`. Passing it to `TaskGroup.start_soon` (which expects a callable) raises `TypeError`, or creating it without awaiting leaves the counter incremented forever.
- **Negative example**:
```python
async def worker():
    pass


async with TaskGroup() as tg:
    wg = AsyncWaitGroup()
    tg.start_soon(wg.track(worker()))  # TypeError! Counter leaked!
```
- **Correct approach**:
```python
async with TaskGroup() as tg:
    wg = AsyncWaitGroup()

    async def tracked_worker():
        await wg.track(worker())

    tg.start_soon(tracked_worker)
```

**2. `select_channel` starvation under multi-channel saturation**:
- **Trap**: `select_channel` probes arguments deterministically from left to right in Phase 1. In saturated loops, earlier arguments starve later arguments.
- **Negative example**:
```python
while True:
    ch, val = await select_channel(ch1, ch2)
    process(val)
```
- **Correct approach**: Shuffle or alternate channel argument order if statistical fairness is required across ready channels.

**3. `Barrier` party timeout and automatic broken state**:
- **Trap**: When a waiting party is cancelled or times out, `Barrier` automatically transitions to the Broken state and wakes all remaining waiting parties with a `RuntimeError`. Subsequent `wait()` calls will continue to raise until `barrier.reset()` is called.
- **Negative example**:
```python
try:
    await asyncio.wait_for(barrier.wait(), timeout=1.0)
except TimeoutError:
    pass  # Leaves barrier broken; future waits will immediately fail!
```
- **Correct approach**:
```python
try:
    await asyncio.wait_for(barrier.wait(), timeout=1.0)
except TimeoutError:
    barrier.reset()  # Reset barrier generation to allow future rounds
    raise
```

**4. `CapacityLimiter` anonymous release and fractional capacity checks**:
- **Trap**: `CapacityLimiter` uses anonymous token counters (no borrower task validation), and fractional `total_tokens` (e.g. `2.5`) leaves integer semaphore capacity at `int(total_tokens) = 2`.
- **Negative example**:
```python
if limiter.available_tokens > 0:  # e.g. 0.5 > 0
    await limiter.acquire()  # Blocks if integer capacity (2) is already borrowed!
```
- **Correct approach**: Always use `async with limiter:`; require `limiter.available_capacity >= 1` (or `available_tokens >= 1.0`) for non-blocking expectations.

**5. ASGI `scope["state"]` shared dictionary mutation and cross-request pollution**:
- **Trap**: Passing a shared mutable lifespan state dictionary directly to ASGI request scopes allows endpoints to overwrite shared state, causing cross-request data leaks (e.g. leaking authenticated user context) and multi-thread dictionary write contention under Python 3.14t.
- **Negative example**:
```python
scope["state"] = self.lifespan_state  # Direct shared reference!
```
- **Correct approach**:
```python
scope["state"] = self.lifespan_state.copy()  # Per-request isolated shallow copy
```

**6. `asyncio.Transport` cross-thread write without event loop trampoline**:
- **Trap**: Calling `transport.write()` from an OS thread other than the transport's own event loop corrupts internal `_SelectorSocketTransport` buffers and causes race conditions.
- **Negative example**:
```python
# On Worker Thread B:
transport.write(b"data")  # Thread-unsafe direct write!
```
- **Correct approach**:
```python
# On Worker Thread B:
if cur_loop is not home_loop:
    fut = asyncio.run_coroutine_threadsafe(ws.send(message), home_loop)
    await asyncio.wrap_future(fut)
```

---

## 3. Pre-Change Checklist

**Before changing concurrency code (channels/locks/semaphores/events/
conditions/barriers/waitgroups/cancellation/pool), walk through each item;
if any item is not satisfied, think it through before touching anything.**

- [ ] Does the change add/modify **waiter registration or wakeup paths**? →
      Patterns 2/4: are registration and wakeup under the same lock? Is
      there a lock-free fast path plus a re-check under the lock?
- [ ] Does the change touch the **cancellation/timeout path**? → Patterns
      3/5: is the cleanup-path `await` inside a shield? Does waiter removal
      carry a generation/identity check?
- [ ] Does the change move **asyncio objects across threads**? → Pattern 7:
      cross-thread only `call_soon_threadsafe`.
- [ ] Does the change introduce **multiple locks**? → Pattern 8: is the lock
      order consistent? Any `await` while holding a lock? Are locks released
      before calling functions that may take locks?
- [ ] Does the change touch **counter/token increments**? → Patterns 1/6:
      are check and act atomic? On cancellation/failure, are tokens
      forwarded or returned (never lost, never duplicated)?
- [ ] Rust-side changes touching **poller counts, batch pull, or atomics**? →
      Section 4: does the RAII guard keep the count safe? Is the memory
      ordering sufficient?
- [ ] Is the new behavior stress-tested with `pytest-repeat --count=50`
      on the **specific target test file/function** (never across the whole suite)?
      Is the test marked `@pytest.mark.free_threading` (3.14t-specific stress)?
- [ ] Found a bug? Did you write a **minimal reproduction test** (deterministic
      data, stable FAIL) before fixing? (mandatory process, Section 5.5)

---

## 4. Rust Core Notes (`src/lib.rs`)

### 4.1 The three components and their thread boundaries

| Component | Thread boundary | Note |
|---|---|---|
| `NativeWorkerPool` | any thread may `push_global`/`push_local`; `pop_work` only on worker threads | close: drop senders first, then set the flag (`lib.rs:219-225`) |
| `Channel` | any thread may send/recv | flume itself is lock-free; `is_closed` is a flag store/load (Release/Acquire) |
| `RawAsyncWaitGroup` | any thread may add/done/register | counter AcqRel; waiter list under parking_lot Mutex |

### 4.2 The soft poller gate (`num_polling`)

- `fetch_add(1, Relaxed)` then check `< max(num_workers/2, 1)` — a
  **momentary gate**: winning it only applies to this round's batch pull and
  expires immediately; there are no hard role assignments — any worker can
  steal from the global queue at any time (`lib.rs:301-303`).
- **Always use `PollerGuard` (RAII) to decrement**: the `fetch_sub` runs even
  on panic (`lib.rs:170-176`). Hand-rolled `fetch_add`/`fetch_sub` pairs
  leak the count on any `?`/panic path → always use the guard.

### 4.3 The three-tier consumption of batch pull

Priority: **private buffer → global batch → local channel**
(`lib.rs:287-352`).
- batch size = `min(global_len / num_workers + 1, 128)` amortizes the flume
  pop cost.
- When changing the priority/batch formula, also check: can workers starve
  the global queue? (the anti-greed `await asyncio.sleep(0)` mechanism on the
  Python side lives in `_worker_dispatcher` in `pool.py`).
- `push_local` falls back to **global** when the local queue is full,
  preserving liveness (`lib.rs:276-280`) — don't change it to block, or a
  full per-worker queue would stall the submitter.

### 4.4 Atomics and memory ordering

- Every `AtomicMetrics` counter is a `#[repr(align(64))] PaddedAtomic` —
  prevents false sharing. **Keep 64-byte alignment when adding counters**,
  or throughput mysteriously drops on multi-core (`lib.rs:14-15`).
- Established ordering conventions, follow them in new code:
  - `is_closed`: `Release` store / `Acquire` load (flag pattern,
    `lib.rs:224, 232`)
  - WaitGroup counter: `add` uses `Release`, `done` uses `AcqRel`
    (`lib.rs:439, 444`)
- flume is lock-free but **not zero-cost**: cross-thread send/recv still has
  atomic ops and memory fences. Don't assume "lock-free = free" on
  performance-sensitive paths.

### 4.5 The ordering constraint on `register_waiter`

In `RawAsyncWaitGroup` (`lib.rs:548`), the state machine is guarded by `parking_lot::Mutex<WaitGroupInner>`: **callers must ensure all positive-delta `add()` calls happen-before `register_waiter()`** (same convention as Go's WaitGroup). Otherwise, calling `wait()` when the counter is zero immediately returns ready and misses concurrent work started afterwards.

---

## 5. Testing Methodology

### 5.1 The concurrency regression trio

```bash
# Stress a single test file (race detection): repeat 50 times
uv run pytest tests/test_pool.py -p no:cacheprovider --count=50

# Deadlocks must fail, not hang: pytest-timeout 180s + thread mode
# auto-dumps all thread stacks on timeout (configured in pyproject.toml)
# when stuck, look for "Thread dump" in the output to locate the await point

# Re-run only the last failures
uv run pytest --lf
```

### 5.2 Marker semantics

| marker | Purpose | CI behavior |
|---|---|---|
| `slow` | performance tests | skipped by default (`addopts = -m "not slow"`) |
| `free_threading` | 3.14t free-threaded stress | runs only in free-threaded build jobs |
| `repeat(n)` | declares stress intent | used with `--count` |

New concurrency tests: mark `free_threading` where possible; declare stress
intent with `pytest.mark.repeat` when deterministic.

### 5.3 Test helpers (`tests/conftest.py`)

- `skip_if_no_rust`: skip when the Rust extension is not compiled (the pure
  Python parts still run without `make develop`).
- `yielder` fixture → `wait_all_tasks_blocked()`: wait until all tasks are
  blocked before asserting — eliminates "asserted before it started"
  timing false-positives.

### 5.4 Local resource constraints (measured on M1 8GB)

- **Run function by function, not the whole suite**; always add a timeout
  (pytest-timeout is global; use `--timeout=60` for tighter single tests).
- Local free-threaded (3.14t) stress: **≤ 6 threads**, pool thread counts
  stay at 4-8.
- CI stress principle: **don't reduce load, extend timeouts** — what fails
  locally may pass in CI and vice versa.

### 5.5 Mandatory process when a bug is found (user rule, non-negotiable)

1. **Write the minimal reproduction test first**: deterministic data (fixed
   seed/fixed concurrency, no random timing), stable FAIL.
2. Confirm the reproduction rate with `--count=50`, and keep the test in the
   suite as a regression test.
3. After the fix, the same test must pass stably, then re-run `--count=50`
   to confirm no concurrency regression.
4. **Forbidden**: "fix while experimenting" or "fix without a test" — a fix
   without a reproduction test is not done.

---

## 6. Related Documents

- [API.md](API.md) — complete API reference
- [CHOOSING.md](CHOOSING.md) — primitive selection decision table ("which
  one to use")
- [AGENTS.md](../AGENTS.md) — AI navigation (quick reference for the six
  non-obvious design decisions)
