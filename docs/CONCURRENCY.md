# gsyncio Concurrency Correctness Guide

**[中文版 (Chinese)](CONCURRENCY_ZH.md)**

> **Read before touching any concurrency-related code.** This guide serves
> two audiences: AI agents (walk through the Section 3 checklist before
> writing code) and human contributors (understand each primitive's
> concurrency model before editing). Every claim cites its source location —
> verify by jumping there.

---

## 1. Concurrency Model Overview (who locks what, who waits for what)

All gsyncio synchronization primitives follow the same skeleton: a
**`threading.Lock` on the Python side protects the waiter structures, while
the Rust side carries the data plane with atomics + flume**. Cross-thread
wakeups always go through `loop.call_soon_threadsafe(...)` — asyncio objects
are never touched directly from another thread.

### 1.1 Channels (FastChannel / AsyncChannel)

| Component | Lock / primitive | Waiter structure | Key invariant |
|---|---|---|---|
| Rust `FastChannel` | flume channel (bounded/unbounded) + `AtomicBool is_closed` | none | `try_send` returning `false` means only "full"; closed ⇒ always error (`src/lib.rs:387-418`) |
| Python `_BaseChannel` | `threading.Lock` (`_lock`) | `_getters` / `_putters` deques of `(loop, future)` | waiter registration and wakeup must happen under `_lock` (`src/gsyncio/_channel_base.py:75-78`) |
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
| `CapacityLimiter` | `_total_lock` + embedded Semaphore | same as Semaphore | `available + borrowed == total` holds under a single `_total_lock` acquisition (`snapshot()`, `_sync.py:308-321`) |

### 1.3 Events & Conditions

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Event` | `threading.Lock` | `list[(loop, asyncio.Event)]` | **sticky** (trio semantics, no clear); `set()` swaps the list out under the lock, then wakes each waiter via `call_soon_threadsafe` outside the lock (`_sync.py:385-398`) |
| `Condition` | `_waiters_lock` (waiter queue) + underlying `Lock` | `deque[(loop, asyncio.Event)]` | `notify()` does **not** require the underlying lock (`_sync.py:507-522`); `wait()` releases the lock → waits → **re-acquires under a shield** (`_sync.py:472-505`) |

### 1.4 Barriers & Group Sync

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Barrier` | `threading.Mutex` | `list[(loop, asyncio.Event)]` | `_generation` counter binds each waiter to its round; cancellation handlers check the generation before removing an entry (`_sync.py:628-643`) |
| `AsyncWaitGroup` | Rust: `AtomicUsize` counter + parking_lot `Mutex` | `Vec[(loop, future)]` | `done()` to zero hands over the whole waiter list via `mem::take` (`lib.rs:443-457`); `register_waiter` double-checks: lock-free fast path + re-check under the lock (`lib.rs:473-487`) |
| `AsyncOnce` | `threading.Lock` | `deque[(loop, future)]` | leader/follower: the lock decides who executes; followers register under the lock; the leader `_wake_all`s **while holding the lock** in `finally` — registration and wakeup share one lock, so there is no lost-wakeup window (`primitives.py:377-416`) |

### 1.5 Cancellation & Structured Concurrency

| Component | Mechanism | Key point |
|---|---|---|
| `CancelScope` | per-task contextvars stack + `task.cancelling()`/`uncancel()` | shield snapshots and clears the cancellation count on entry, restores it on exit (`_cancel.py:141-146, 184-189`) |
| `select_channel` | `TaskGroup` + one reader per channel | a successful reader **deliberately raises `CancelledError`** to make the group exit early — a normal return would make the group wait for every channel (`primitives.py:248-255`) |

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
- `CapacityLimiter`'s three properties each take `_total_lock` separately; a
  concurrent resize can mix values → use `snapshot()` to read all three
  under one lock (`_sync.py:308-321`).
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
FastChannel" design decision in AGENTS.md.

### Pattern 3: lock re-acquisition on the cancellation path (missing shield → deadlock)

**Symptom**: after `Condition.wait()` is cancelled, re-acquiring the lock in
the cleanup path immediately re-raises `CancelledError`; the lock is never
taken back → the task is stuck in cleanup and the lock leaks.
**Root cause**: Python cancellation re-raises at every `await`, so cleanup
code cannot `await`.
**Correct approach**: wrap the re-acquisition in
`CancelScope(shield=True)` — clear the injected cancellation count, take the
lock, then let cancellation fire.
**Examples**: the cancellation branch of `Condition.wait()`
(`_sync.py:493-500`); the shield snapshot/restore implementation
(`_cancel.py:141-146, 184-189`).

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

### Pattern 6: register-after-done race

**Symptom**: `wait()` checks "counter is already 0" and is about to
register, when `done()` decrements to 0 and clears the waiter list → the new
waiter is never woken.
**Root cause**: the window between the check and the registration.
**Correct approach**: the registration call returns whether it is already
done; if so, return immediately without waiting (same semantics as Go
`sync.WaitGroup.Wait`).
**Examples**: `AsyncWaitGroup.wait()` + the boolean return of
`register_waiter` (`primitives.py:332-339`, `lib.rs:473-487`). Note the
Rust doc explicitly states: **all `add()` calls with a positive delta must
happen-before `register_waiter()`**, or the lock-free fast path can still
lose to a concurrent `add()` — callers must respect this ordering.

### Pattern 7: sharing asyncio objects across threads

**Symptom**: an `asyncio.Event`/`Task` owned by one loop is set/cancelled
directly from another thread → `RuntimeError` or silent loss.
**Root cause**: asyncio objects are not thread-safe;
`call_soon_threadsafe` is the only safe cross-thread injection channel.
**Correct approach**: cross-thread, always use
`loop.call_soon_threadsafe(fut.set_result, ...)` or
`call_soon_threadsafe(event.set)`; communicate between threads with
`FastChannel`, never raw asyncio primitives.
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
      (never a single run)? Is the test marked `@pytest.mark.free_threading`
      (3.14t-specific stress)?
- [ ] Found a bug? Did you write a **minimal reproduction test** (deterministic
      data, stable FAIL) before fixing? (mandatory process, Section 5.5)

---

## 4. Rust Core Notes (`src/lib.rs`)

### 4.1 The three components and their thread boundaries

| Component | Thread boundary | Note |
|---|---|---|
| `NativeWorkerPool` | any thread may `push_global`/`push_local`; `pop_work` only on worker threads | close: drop senders first, then set the flag (`lib.rs:219-225`) |
| `FastChannel` | any thread may send/recv | flume itself is lock-free; `is_closed` is a flag store/load (Release/Acquire) |
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

The lock-free fast path of `RawAsyncWaitGroup::register_waiter`
(`lib.rs:474`) can lose to a concurrent `add()` 0→1: **callers must ensure
all positive-delta `add()` calls happen-before `register_waiter()`** (same
convention as Go's WaitGroup). When changing waitgroup semantics, don't
break this constraint, or you get a subtle hang where a waiter is never
woken.

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
- `checkpoints` fixture → `assert_checkpoints(...)`: event-ordering context
  manager that verifies the expected interleaving.

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
