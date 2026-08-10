# gsyncio API Reference

**[中文版 (Chinese)](API_ZH.md)**

Complete API documentation for `gsyncio`: a multi-event-loop engine and concurrency toolkit for Python 3.14t (free-threaded / no-GIL).

---

## 📋 Table of Contents

- [Quick Examples](#quick-examples)
- [Core Engine](#core-engine)
  - [`EventLoopThreadPool`](#eventloopthreadpool)
  - [`PoolOptions`](#pooloptions)
- [Top-Level Functions](#top-level-functions)
  - [`create_pool`](#create_pool)
  - [`run_in_pool`](#run_in_pool)
  - [`checkpoint`](#checkpoint)
  - [`fail_after`](#fail_after)
  - [`move_on_after`](#move_on_after)
  - [`fail_at`](#fail_at)
  - [`move_on_at`](#move_on_at)
  - [`current_effective_deadline`](#current_effective_deadline)
  - [`get_logger`](#get_logger)
  - [`set_log_level`](#set_log_level)
- [Golang-Style Channels & Multiplexing](#golang-style-channels--multiplexing)
  - [`FastChannel`](#fastchannel)
  - [`select_channel`](#select_channel)
- [Task Management](#task-management)
  - [`TaskGroup`](#taskgroup)
  - [`TaskHandle`](#taskhandle)
  - [`TaskStatus`](#taskstatus)
  - [`CancelScope`](#cancelscope)
- [Synchronization & Control Primitives](#synchronization--control-primitives)
  - [`Lock`](#lock)
  - [`Semaphore`](#semaphore)
  - [`CapacityLimiter`](#capacitylimiter)
  - [`Event`](#event)
  - [`Condition`](#condition)
  - [`Barrier`](#barrier)
  - [`AsyncContext`](#asynccontext)
  - [`AsyncWaitGroup`](#asyncwaitgroup)
  - [`AsyncOnce`](#asynconce)
  - [`AsyncRWMutex`](#asyncrwmutex)
- [Networking & ASGI Workers](#networking--asgi-workers)
  - [`ConnectionPinningServer`](#connectionpinningserver)
  - [`GsyncioASGIWorker`](#gsyncioasgiworker)
- [Exceptions](#exceptions)
  - [`GsyncioError`](#gsyncioerror)
  - [`ChannelClosedError`](#channelclosederror)
  - [`ThreadPoolClosedError`](#threadpoolclosederror)
  - [`TimeoutError`](#timeouterror)
  - [`WouldBlock`](#wouldblock)

---

## Quick Examples

Copy-paste runnable snippets. Each example is self-contained: it imports `gsyncio`, defines an `async def main()`, and runs it with `asyncio.run(main())`. Simulated work uses `asyncio.sleep`.

### 1. `EventLoopThreadPool` + `submit`

```python
import asyncio
import gsyncio


async def heavy_task(x: int) -> int:
    await asyncio.sleep(0.01)  # simulated work
    return x * 2


async def main():
    async with gsyncio.EventLoopThreadPool(num_threads=2) as pool:
        fut1 = pool.submit(heavy_task, 21)
        fut2 = pool.submit(heavy_task, 21, pin_to=0)  # pinned to worker 0
        results = await asyncio.gather(fut1, fut2)
        print(results)  # [42, 42]


if __name__ == "__main__":
    asyncio.run(main())
```

### 2. `FastChannel` send/recv

```python
import asyncio
import gsyncio


async def main():
    ch = gsyncio.FastChannel()

    async def producer():
        for i in range(3):
            await ch.send(i)
        ch.close()  # signal end of stream

    asyncio.create_task(producer())
    async for item in ch:  # terminates when closed & empty
        print("got", item)


if __name__ == "__main__":
    asyncio.run(main())
```

### 3. `select_channel`

```python
import asyncio
import gsyncio


async def main():
    ch1 = gsyncio.FastChannel()
    ch2 = gsyncio.FastChannel()

    async def feeder():
        await asyncio.sleep(0.01)
        await ch2.send("data from ch2")
        ch2.close()

    asyncio.create_task(feeder())
    selected, val = await gsyncio.select_channel(ch1, ch2, timeout=2.0)
    print(f"selected: {val}")  # ("data from ch2") from ch2


if __name__ == "__main__":
    asyncio.run(main())
```

### 4. `AsyncContext` cancel

```python
import asyncio
import gsyncio


async def main():
    async with gsyncio.EventLoopThreadPool(num_threads=2) as pool:
        ctx = gsyncio.AsyncContext()

        async def slow_work():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                return "cancelled"

        fut = ctx.submit(pool, slow_work)
        ctx.cancel()  # cascades cancellation to the pool task
        print(await fut)  # "cancelled"
        print("ctx cancelled:", ctx.is_cancelled)  # True


if __name__ == "__main__":
    asyncio.run(main())
```

### 5. `AsyncWaitGroup`

```python
import asyncio
import gsyncio


async def worker(wg: gsyncio.AsyncWaitGroup, name: str):
    await asyncio.sleep(0.01)
    print(f"{name} done")
    wg.done()


async def main():
    wg = gsyncio.AsyncWaitGroup()
    wg.add(2)
    asyncio.create_task(worker(wg, "a"))
    asyncio.create_task(worker(wg, "b"))
    await wg.wait()  # blocks until both workers call done()
    print("all workers finished")


if __name__ == "__main__":
    asyncio.run(main())
```

### 6. `AsyncOnce`

```python
import asyncio
import gsyncio


async def init_db() -> str:
    await asyncio.sleep(0.01)  # simulated one-time setup
    return "db ready"


async def main():
    once = gsyncio.AsyncOnce()
    r1 = await once.do(init_db)
    r2 = await once.do(init_db)  # skipped: runs once
    print(r1)  # "db ready"
    print(r2)  # "db ready" (cached result)


if __name__ == "__main__":
    asyncio.run(main())
```

### 7. `AsyncRWMutex`

```python
import asyncio
import gsyncio


async def main():
    rw = gsyncio.AsyncRWMutex()
    data = {"value": 0}

    async with rw.reader():  # concurrent readers allowed
        await asyncio.sleep(0.01)
        print("read:", data["value"])

    async with rw.writer():  # exclusive writer
        await asyncio.sleep(0.01)
        data["value"] = 1
        print("wrote:", data["value"])


if __name__ == "__main__":
    asyncio.run(main())
```

### 8. `Lock`

```python
import asyncio
import gsyncio


async def main():
    lock = gsyncio.Lock()
    counter = 0

    async def inc():
        nonlocal counter
        async with lock:
            await asyncio.sleep(0.01)
            counter += 1

    await asyncio.gather(*(inc() for _ in range(5)))
    print("counter:", counter)  # 5 (no lost updates)


if __name__ == "__main__":
    asyncio.run(main())
```

### 9. `TaskGroup`

```python
import asyncio
import gsyncio


async def worker(name: str) -> str:
    await asyncio.sleep(0.01)
    return f"{name} done"


async def main():
    async with gsyncio.TaskGroup() as tg:
        h1 = tg.start_soon(worker, "a")
        h2 = tg.start_soon(worker, "b")
    # Both tasks are guaranteed finished here.
    print(await h1, await h2)


if __name__ == "__main__":
    asyncio.run(main())
```

### 10. `CancelScope` with `fail_after` / `move_on_after`

```python
import asyncio
import gsyncio


async def slow():
    await asyncio.sleep(10)


async def main():
    # fail_after raises TimeoutError when the deadline expires.
    try:
        async with gsyncio.fail_after(0.05):
            await slow()
    except gsyncio.TimeoutError:
        print("timed out with error")

    # move_on_after exits silently when the deadline expires.
    async with gsyncio.move_on_after(0.05) as scope:
        await slow()
    if scope.cancelled_caught:
        print("timed out silently")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Core Engine

### `EventLoopThreadPool`

A multi-event-loop thread pool for CPython 3.14t. Each worker thread runs its own event loop. Tasks are pushed into a shared lock-free queue and pulled by idle workers (work-stealing model); wakeup notifications are dispatched round-robin across the worker loops.

#### Constructor

```text
EventLoopThreadPool(
    options: PoolOptions | None = None,
    num_threads: int | None = None,
    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
)
```

- **`num_threads`** (*int | None*): Number of physical worker threads to spawn. Defaults to `os.cpu_count() or 4`. Must be $\ge 1$.
- **`loop_factory`** (*Callable | None*): Factory that creates each worker's event loop. Defaults to `asyncio.new_event_loop`.
- **`options`** (*PoolOptions | None*): Optional settings that supply defaults for `num_threads` and `loop_factory`.

#### Properties & Methods

##### `is_running` -> `bool`
Returns `True` if the pool is actively running worker loops.

##### `async start()` -> `None`
Starts all worker threads and their work-stealing queue dispatchers.

##### `async close()` -> `None`
Gracefully stops all worker loops, drains pending tasks, and joins worker threads.

Drain is bounded: `close()` waits up to ~5 seconds for in-flight tasks to finish, then force-stops every worker loop — tasks still running at that point are cancelled and are not guaranteed to complete.

##### `async abort()` -> `None`
Forcefully stops all worker loops without draining pending tasks. Queued but unexecuted work is discarded.

##### `async wait_closed()` -> `None`
Waits until the pool has been fully stopped. Returns immediately if the pool is not running.

##### `submit(target: Callable[..., Any], *args: Any, pin_to: asyncio.AbstractEventLoop | int | None = None, cancel_scope: CancelScope | None = None, **kwargs: Any)` -> `asyncio.Future`
Submits a coroutine function, coroutine object, or callable to the shared work queue. Workers pull tasks as they become idle. Returns an `asyncio.Future` that resolves with the task's result.

- **`pin_to`**: Optionally pins the task to a specific worker loop (an `int` worker index or an `AbstractEventLoop` managed by the pool). When `None`, the task goes to the global shared queue.
- **`cancel_scope`**: Optionally binds the task to a `CancelScope`; cancelled scopes suppress the task result.
- **Raises**: `ThreadPoolClosedError` if the pool is closed, `ValueError` for an invalid `pin_to` index.

##### `get_metrics()` -> `dict[str, Any]`
Returns a JSON-serializable dictionary containing health metrics:
```json
{
  "is_running": true,
  "thread_count": 4,
  "completed_tasks": [120, 98, 131, 87],
  "active_tasks": [1, 0, 2, 0]
}
```



##### Context Manager
Supports `async with EventLoopThreadPool(...) as pool:` for automatic startup (`start()`) and shutdown (`close()`).

---

### `PoolOptions`

A dataclass carrying configuration defaults for `EventLoopThreadPool`.

```text
PoolOptions(
    num_threads: int = os.cpu_count() or 4,
    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
)
```

- **`num_threads`** (*int*): Number of worker threads. `0` means auto-detect via `os.cpu_count()`.
- **`loop_factory`** (*Callable | None*): Event loop factory for worker threads.

---

## Top-Level Functions

### `create_pool`

```text
async def create_pool(
    num_threads: int = 0,
    options: PoolOptions | None = None,
    **kwargs: Any,
) -> EventLoopThreadPool
```

Creates and **starts** a pool, returning it ready for use (`asyncssh`-style facade). `num_threads=0` auto-detects via `os.cpu_count()`. Use it as an async context manager, or call `close()` manually when finished.

### `run_in_pool`

```text
async def run_in_pool(coro: Any, *args: Any, num_threads: int = 0, **kwargs: Any) -> Any
```

Runs a coroutine in a freshly-created pool (one-shot convenience). Spawns a pool, submits the task, awaits its result, then closes the pool.

### `checkpoint`

```text
async def checkpoint() -> None
```

Checks for effective cancellation and raises `asyncio.CancelledError` if the current scope, or any unshielded ancestor, has been cancelled. Call periodically inside long-running code that cannot `await` frequently.

### `fail_after`

```text
def fail_after(seconds: float) -> CancelScope
```

Returns a `CancelScope` that raises `TimeoutError` when the deadline expires.

```python
async with fail_after(5):
    await long_operation()
```

### `move_on_after`

```text
def move_on_after(seconds: float) -> CancelScope
```

Returns a `CancelScope` that silently exits when the deadline expires. `scope.cancelled_caught` is `True` after the deadline fires.

```python
async with move_on_after(5) as scope:
    await maybe_slow()
if scope.cancelled_caught:
    print("timed out silently")
```

### `fail_at`

```text
def fail_at(deadline: float) -> CancelScope
```

Returns a `CancelScope` with an **absolute monotonic deadline** that raises `TimeoutError` when the deadline expires. Use when a deadline is already known as a monotonic timestamp (e.g. derived from `current_effective_deadline()`); for relative seconds, use `fail_after`.

```python
deadline = asyncio.get_running_loop().time() + 5
async with fail_at(deadline):
    await long_operation()
```

### `move_on_at`

```text
def move_on_at(deadline: float) -> CancelScope
```

Returns a `CancelScope` with an **absolute monotonic deadline** that silently exits when the deadline expires. `scope.cancelled_caught` is `True` after the deadline fires. For relative seconds, use `move_on_after`.

```python
deadline = asyncio.get_running_loop().time() + 5
async with move_on_at(deadline) as scope:
    await maybe_slow()
if scope.cancelled_caught:
    print("timed out silently")
```

### `current_effective_deadline`

```text
def current_effective_deadline() -> float
```

Walks the task-local scope stack and returns the tightest (nearest) effective deadline among the active scopes. Shielded scopes act as a barrier: deadlines from scopes outside a shielded scope are invisible to code inside the shield. Returns `float("inf")` when no deadline is active.

```python
async with fail_after(10):
    async with CancelScope(shield=True):
        dl = current_effective_deadline()  # float("inf"): outer deadline is shielded
```

### `get_logger`

```text
def get_logger(name: str | None = None) -> logging.LoggerAdapter[Any]
```

Returns a structured logger adapter for the `gsyncio` namespace. The adapter wraps the `"gsyncio"` logger (or the `"gsyncio.<name>"` sub-logger when `name` is given, which propagates to the root `"gsyncio"` logger and inherits its level) and injects `task_id` and `span` structured fields into every emitted record.

- **`name`** (*str | None*): Optional sub-logger name (e.g. `"pool"`).

```python
log = get_logger("pool")
log.info("pool started", extra={"span": "boot"})
```

### `set_log_level`

```text
def set_log_level(level: int) -> None
```

Sets the gsyncio logger's minimum log level. `level` is a `logging` level constant (e.g. `logging.INFO` or `logging.DEBUG`).

```python
import logging

set_log_level(logging.DEBUG)
```

---

## Golang-Style Channels & Multiplexing

### `FastChannel`

High-performance cross-thread channel backed by Rust (`flume` crate) with a double-check lock pattern for zero lost-wakeup signal races.

#### Constructor

```text
FastChannel(maxsize: int = 0)
```

- **`maxsize`** (*int*): Channel capacity limit. `0` for unbounded channel.

#### Methods

##### `async send(item: Any)` -> `None`
Sends an item into the channel. Suspends if the channel is full until space is available.
- **Raises**: `ChannelClosedError` if the channel is closed.

##### `async recv(timeout: float | None = None)` -> `Any`
Receives an item from the channel. Suspends if empty until an item is available.
- **`timeout`** (*float | None*): Timeout in seconds.
- **Raises**: `ChannelClosedError` if closed and empty, `TimeoutError` if timed out.

##### `try_send(item: Any)` -> `bool`
Non-blocking send. Returns `True` if the item was enqueued, `False` if the channel is full.
- **Raises**: `ChannelClosedError` if the channel is closed.

##### `try_recv()` -> `Any`
Non-blocking receive.
- **Raises**: `WouldBlock` if the channel is empty, `ChannelClosedError` if closed and empty.

##### `qsize()` -> `int`
Returns the number of items currently buffered in the channel.



##### `close()` -> `None`
Closes the channel. All pending senders/receivers are woken up with `ChannelClosedError`.

##### `is_closed` -> `bool`
Returns `True` if closed.

##### `__aiter__()` & `__anext__()`
Supports `async for item in ch:` iteration. Automatically terminates when the channel is closed and empty.

---

### `select_channel`

```text
async def select_channel(
    *channels: Any,
    timeout: float | None = None,
    default: Any = ...,
) -> Any
```

Selects the first ready channel from multiple `FastChannel` instances (Go `select`-style).

- **Returns**: `(selected_channel, value)`.
- **`default`**: When provided, `select_channel` is non-blocking: it tries each channel with `try_recv()` and returns `(channel, value)` for the first ready channel, or `default` if none is ready.
- **Raises**: `ValueError` if no channels are given, `TimeoutError` if `timeout` is reached before any channel becomes ready.

**Example**:
```python
selected_ch, val = await select_channel(ch1, ch2, timeout=2.0)
```

---

## Task Management

### `TaskGroup`

An async context manager that spawns and manages child tasks, backed by `CancelScope` for cancellation propagation. Inspired by trio's nursery and anyio's `TaskGroup`.

```python
async with TaskGroup(name=None) as tg:
    h1 = tg.start_soon(worker, "a")
    h2 = tg.start_soon(worker, "b")
# Both tasks are guaranteed finished here.
```

- **`start_soon(coro_fn, *args)` -> `TaskHandle`**: Spawns a child task and returns its handle immediately without blocking.
- **`start(coro_fn, *args)` -> `TaskHandle`**: Spawns a child task, blocking until it calls `task_status.started()`.
- **Raises**: Child exceptions are re-raised on exit; multiple failures surface as an `ExceptionGroup`.

### `TaskHandle`

A handle to a child task spawned inside a `TaskGroup`, returned by `TaskGroup.start_soon()` and `TaskGroup.start()`. Awaiting the handle returns the task's result (or raises its exception).

#### Properties

- **`status`** -> `_TaskStatus`: Current lifecycle status — `_TaskStatus.PENDING`, `_TaskStatus.STARTED`, or `_TaskStatus.FINISHED`.
- **`result`** -> `Any`: The task's result once finished.
  - **Raises**: `RuntimeError` if the task is not finished yet.
- **`exception`** -> `BaseException | None`: The task's exception, or `None` if it succeeded.
  - **Raises**: `RuntimeError` if the task is not finished yet.

#### Awaitable
`await handle` returns the task's result and re-raises its exception.

```python
h = tg.start_soon(worker, "a")
result = await h
```

### `TaskStatus`

Status tracker used with `TaskGroup.start()`. The spawned coroutine receives a `TaskStatus` instance as its first argument and calls `started()` once initialised, unblocking `TaskGroup.start()` so it can return the handle.

```python
async def worker(task_status: TaskStatus):
    await setup()
    task_status.started()  # unblocks TaskGroup.start()
    await run()


async with TaskGroup() as tg:
    h = await tg.start(worker)
```

- **`started()`**: Marks the task as started, unblocking the pending `TaskGroup.start()`.

### `CancelScope`

A task-local cancellation scope that propagates cancellation hierarchically.

```text
CancelScope(deadline: float = float("inf"), shield: bool = False)
```

- **`deadline`** (*float*): Absolute monotonic deadline. `float("inf")` means no deadline.
- **`shield`** (*bool*): When `True`, blocks parent-scope cancellation from penetrating inward.

#### Properties & Methods

- **`cancel_called`** -> `bool`: `True` after `cancel()` has been called.
- **`cancelled_caught`** -> `bool`: `True` if the scope silently absorbed a cancellation (move-on-* scopes).
- **`deadline`** -> `float`: The absolute monotonic deadline.
- **`shield`** -> `bool`: Whether the scope blocks parent cancellation.
- **`cancel()`**: Marks the scope as cancelled and injects cancellation into the hosting task. Idempotent.

#### Context Manager
`async with CancelScope(...) as scope:` enters the scope on the current task's scope stack.

---

## Synchronization & Control Primitives

### `Lock`

A cross-thread async mutual-exclusion lock.

```python
lock = Lock()

async with lock:
    critical_section()
```

- **`locked`** -> `bool`: `True` if the lock is held.
- **`owner`** -> `asyncio.Task | None`: The task currently holding the lock, if any.
- **`acquire()`**: Acquires the lock, suspending until it is free.
- **`release()`**: Releases the lock. Must be called by the owner.

### `Semaphore`

An async counting semaphore.

```text
Semaphore(max_value: int)
```

- **`value`** -> `int`: Number of tokens currently available.
- **`max_value`** -> `int`: Maximum number of tokens.
- **`acquire()`**: Acquires a token, suspending until one is available.
- **`release()`**: Releases a token back to the semaphore.

### `CapacityLimiter`

A token-bucket style limiter supporting fractional tokens.

```text
CapacityLimiter(total_tokens: float)
```

- **`total_tokens`** -> `float`: Total capacity (readable and writable).
- **`available_tokens`** -> `float`: Tokens currently available.
- **`borrowed_tokens`** -> `float`: Tokens currently borrowed.
- **`snapshot()`** -> `(total, available, borrowed)`: Atomically consistent
  read of all three counters under one lock acquisition. Use this instead of
  reading the three properties separately when another thread may be resizing
  `total_tokens` concurrently — separate reads can mix values computed
  against different totals.
- **`acquire()`**: Acquires one token, suspending until available.
- **`release()`**: Returns one token.

### `Event`

A cross-thread async event flag.

```text
Event()
```

- **`is_set`** -> `bool`: `True` once `set()` has been called.
- **`set()`**: Sets the event and wakes all waiters.
- **`wait()`**: Suspends until the event is set.

### `Condition`

A cross-thread async condition variable, typically paired with a `Lock`.

```text
Condition(lock: Lock | None = None)
```

- **`acquire()`** / **`release()`**: Manage the underlying lock.
- **`wait()`**: Atomically releases the lock and suspends until notified.
- **`notify(n=1)`**: Wakes `n` waiting tasks.
- **`notify_all()`**: Wakes all waiting tasks.

### `Barrier`

A cross-thread async barrier that blocks until a fixed number of parties arrive.

```text
Barrier(parties: int)
```

- **`parties`** -> `int`: Number of parties required to trip the barrier.
- **`n_waiting`** -> `int`: Number of parties currently waiting.
- **`wait()`** -> `BarrierWaitResult`: Blocks until all parties arrive.
- **`abort()`**: Aborts the barrier, waking all waiters.

### `BarrierWaitResult`

Result object returned by `Barrier.wait()`.

- **`parties`** -> `int`: Total number of parties in this barrier round.

### `AsyncContext`

Go `context.Context`-style cancellation context supporting cross-thread cascading task cancellation.

```text
ctx = AsyncContext(parent: AsyncContext | None = None)
```

- **`ctx.cancel()`**: Cancels this context and cascades cancellation thread-safely to all child contexts and submitted futures.
- **`ctx.submit(pool, target, *args, **kwargs)`**: Submits a task to the pool bound to this context.
- **`ctx.parent`**: The parent context, or `None` for a root context (read-only, fixed at construction).
- **`ctx.is_cancelled`**: Returns `True` if cancelled.

---

### `AsyncWaitGroup`

Go `sync.WaitGroup`-style cross-thread synchronization primitive.

```python
wg = AsyncWaitGroup()
wg.add(2)
# In worker tasks: wg.done()
await wg.wait()
```

All `add()` calls with a positive delta must happen-before the first `wait()` on the counter (matches Go `sync.WaitGroup`): a concurrent `add()` racing with `wait()` can lose its wakeup and hang forever. Negative deltas are allowed (Go semantics) but must never drive the counter below zero — doing so raises `RuntimeError`.

---

### `AsyncOnce`

Go `sync.Once`-style single execution primitive across multiple event loop threads.

```python
once = AsyncOnce()
res = await once.do(init_function, arg1)
```

---

### `AsyncRWMutex`

Go `sync.RWMutex`-style reader-writer lock allowing concurrent readers or single exclusive writer.

```python
rw = AsyncRWMutex()

# Concurrent read access
async with rw.reader():
    read_data()

# Exclusive write access
async with rw.writer():
    write_data()
```

---

## Networking & ASGI Workers

### `ConnectionPinningServer`

Pins each incoming client TCP connection to a specific Worker Event Loop thread for zero cross-thread syscall overhead.

```python
async with ConnectionPinningServer(pool, host="127.0.0.1", port=8080) as server:
    await server.start(handler_coro)
```

---

### `GsyncioASGIWorker`

Mounts FastAPI / Starlette / ASGI 3.0 applications directly onto `EventLoopThreadPool`.

```python
from fastapi import FastAPI
from gsyncio import EventLoopThreadPool, GsyncioASGIWorker

app = FastAPI()


async def main():
    async with EventLoopThreadPool(num_threads=4) as pool:
        async with GsyncioASGIWorker(app, pool, port=8000):
            print("FastAPI running on multi-threaded gsyncio pool...")
            await asyncio.sleep(3600)
```

---

## Exceptions

- **`GsyncioError`**: Base exception for all `gsyncio` errors.

### `GsyncioError`

Base exception for all `gsyncio` errors.

### `ChannelClosedError`

`ChannelClosedError(GsyncioError)` — raised when operating on a closed channel.

### `ThreadPoolClosedError`

`ThreadPoolClosedError(GsyncioError, RuntimeError)` — raised when submitting tasks to a closed thread pool.

### `TimeoutError`

`TimeoutError(GsyncioError)` — raised when a concurrency operation times out.

### `WouldBlock`

`WouldBlock(GsyncioError)` — raised when a non-blocking operation (e.g. `try_recv()`) cannot proceed immediately.
