# gsyncio API 参考（中文版）

**[English](API.md)**

`gsyncio` 的完整 API 文档：为 Python 3.14t（free-threaded / 无 GIL）打造的
多事件循环引擎与并发工具包。

---

## 📋 目录

- [快速示例](#快速示例-quick-examples)
- [核心引擎](#核心引擎-core-engine)
  - [`EventLoopThreadPool`](#eventloopthreadpool)
  - [`PoolOptions`](#pooloptions)
- [顶层函数](#顶层函数-top-level-functions)
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
- [Golang 风格通道与多路复用](#golang-风格通道与多路复用-golang-style-channels--multiplexing)
  - [`FastChannel`](#fastchannel)
  - [`select_channel`](#select_channel)
- [任务管理](#任务管理-task-management)
  - [`TaskGroup`](#taskgroup)
  - [`TaskHandle`](#taskhandle)
  - [`TaskStatus`](#taskstatus)
  - [`CancelScope`](#cancelscope)
- [同步与控制原语](#同步与控制原语-synchronization--control-primitives)
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
- [网络与 ASGI Worker](#网络与-asgi-worker-networking--asgi-workers)
  - [`ConnectionPinningServer`](#connectionpinningserver)
  - [`GsyncioASGIWorker`](#gsyncioasgiworker)
- [异常](#异常-exceptions)
  - [`GsyncioError`](#gsyncioerror)
  - [`ChannelClosedError`](#channelclosederror)
  - [`ThreadPoolClosedError`](#threadpoolclosederror)
  - [`TimeoutError`](#timeouterror)
  - [`WouldBlock`](#wouldblock)

---

## 快速示例 (Quick Examples)

可直接复制运行的代码片段。每个示例都是自包含的：导入 `gsyncio`，定义
`async def main()`，用 `asyncio.run(main())` 执行。模拟工作用
`asyncio.sleep`。

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

## 核心引擎 (Core Engine)

### `EventLoopThreadPool`

为 CPython 3.14t 设计的多事件循环线程池。每个 worker 线程运行自己的事件
循环。任务推入共享无锁队列，由空闲 worker 拉取执行（工作窃取模型）；
唤醒通知以 Round-Robin 方式分发到各 worker 循环。

#### 构造函数

```text
EventLoopThreadPool(
    options: PoolOptions | None = None,
    num_threads: int | None = None,
    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
)
```

- **`num_threads`** (*int | None*)：启动的物理 worker 线程数。默认为
  `os.cpu_count() or 4`。必须 $\ge 1$。
- **`loop_factory`** (*Callable | None*)：创建每个 worker 事件循环的工厂
  函数。默认为 `asyncio.new_event_loop`。
- **`options`** (*PoolOptions | None*)：为 `num_threads` 与 `loop_factory`
  提供默认值的可选配置。

#### 属性与方法

##### `is_running` -> `bool`
池是否正在运行 worker 循环。

##### `async start()` -> `None`
启动所有 worker 线程及其工作窃取队列分发器。

##### `async close()` -> `None`
优雅停止所有 worker 循环：排空待处理任务、join 所有 worker 线程。

排空有上限：`close()` 最多等待约 5 秒让进行中的任务完成，之后强制停止所有 worker 循环——届时仍在运行的任务会被取消，不保证完成。

##### `async abort()` -> `None`
强制停止所有 worker 循环，**不**排空待处理任务。已排队未执行的工作被丢弃。

##### `async wait_closed()` -> `None`
等待池完全停止。池未运行时立即返回。

##### `submit(target: Callable[..., Any], *args: Any, pin_to: asyncio.AbstractEventLoop | int | None = None, cancel_scope: CancelScope | None = None, **kwargs: Any)` -> `asyncio.Future`
把协程函数、协程对象或可调用对象提交到共享工作队列。worker 空闲时拉取
执行。返回解析为任务结果的 `asyncio.Future`。

- **`pin_to`**：可选地把任务钉到指定 worker 循环（`int` worker 下标或池管理
  的 `AbstractEventLoop`）。为 `None` 时任务进全局共享队列。
- **`cancel_scope`**：可选地把任务绑定到 `CancelScope`；已取消的 scope
  会抑制任务结果。
- **抛出**：池已关闭时抛 `ThreadPoolClosedError`，非法 `pin_to` 下标抛
  `ValueError`。

##### `get_metrics()` -> `dict[str, Any]`
返回可 JSON 序列化的健康指标字典：
```json
{
  "is_running": true,
  "thread_count": 4,
  "completed_tasks": [120, 98, 131, 87],
  "active_tasks": [1, 0, 2, 0]
}
```



##### 上下文管理器
支持 `async with EventLoopThreadPool(...) as pool:` 自动启动
（`start()`）与关闭（`close()`）。

---

### `PoolOptions`

携带 `EventLoopThreadPool` 配置默认值的 dataclass。

```text
PoolOptions(
    num_threads: int = os.cpu_count() or 4,
    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
)
```

- **`num_threads`** (*int*)：worker 线程数。`0` 表示通过
  `os.cpu_count()` 自动检测。
- **`loop_factory`** (*Callable | None*)：worker 线程的事件循环工厂。

---

## 顶层函数 (Top-Level Functions)

### `create_pool`

```text
async def create_pool(
    num_threads: int = 0,
    options: PoolOptions | None = None,
    **kwargs: Any,
) -> EventLoopThreadPool
```

创建并**启动**一个池，返回可直接使用的池（`asyncssh` 风格 facade）。
`num_threads=0` 时通过 `os.cpu_count()` 自动检测。可作异步上下文管理器
使用，或完成后手动调用 `close()`。

### `run_in_pool`

```text
async def run_in_pool(coro: Any, *args: Any, num_threads: int = 0, **kwargs: Any) -> Any
```

在全新创建的池中运行协程（一次性便捷函数）。创建池、提交任务、等待
结果、然后关闭池。

### `checkpoint`

```text
async def checkpoint() -> None
```

检查有效取消状态：如果当前 scope（或任一未屏蔽的祖先 scope）已被取消，
抛出 `asyncio.CancelledError`。在无法频繁 `await` 的长运行代码中周期性
调用。

该 raise 会消费任务挂起的取消计数，使其成为*唯一*投递——在
`checkpoint()` 的 raise 处被捕获的取消绝不会在下一个 await 处二次投递。

### `fail_after`

```text
def fail_after(seconds: float) -> CancelScope
```

返回一个到期抛 `TimeoutError` 的 `CancelScope`。

```python
async with fail_after(5):
    await long_operation()
```

### `move_on_after`

```text
def move_on_after(seconds: float) -> CancelScope
```

返回一个到期静默退出的 `CancelScope`。到期后 `scope.cancelled_caught`
为 `True`。

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

返回一个使用**绝对单调截止时间**、到期抛 `TimeoutError` 的 `CancelScope`。
当截止时间已经是单调时间戳（例如由 `current_effective_deadline()` 得出）
时使用；相对秒数请用 `fail_after`。

```python
deadline = asyncio.get_running_loop().time() + 5
async with fail_at(deadline):
    await long_operation()
```

### `move_on_at`

```text
def move_on_at(deadline: float) -> CancelScope
```

返回一个使用**绝对单调截止时间**、到期静默退出的 `CancelScope`。到期后
`scope.cancelled_caught` 为 `True`。相对秒数请用 `move_on_after`。

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

遍历任务本地的 scope 栈，返回活动 scope 中最紧（最近）的有效截止时间。
屏蔽（shield）的 scope 是屏障：屏蔽层外的 scope 截止时间对屏蔽层内的
代码不可见。无活动截止时间时返回 `float("inf")`。

```python
async with fail_after(10):
    async with CancelScope(shield=True):
        dl = current_effective_deadline()  # float("inf"): outer deadline is shielded
```

### `get_logger`

```text
def get_logger(name: str | None = None) -> logging.LoggerAdapter[Any]
```

返回 `gsyncio` 命名空间的结构化日志适配器。适配器包装 `"gsyncio"`
logger（给定 `name` 时为 `"gsyncio.<name>"` 子 logger，向根 `"gsyncio"`
logger 传播并继承其级别），并向每条日志记录注入 `task_id` 与 `span`
结构化字段。

- **`name`** (*str | None*)：可选子 logger 名（如 `"pool"`）。

```python
log = get_logger("pool")
log.info("pool started", extra={"span": "boot"})
```

### `set_log_level`

```text
def set_log_level(level: int) -> None
```

设置 gsyncio logger 的最低日志级别。`level` 是 `logging` 级别常量
（如 `logging.INFO` 或 `logging.DEBUG`）。

```python
import logging

set_log_level(logging.DEBUG)
```

---

## Golang 风格通道与多路复用 (Golang-Style Channels & Multiplexing)

### `FastChannel`

由 Rust（`flume` crate）支撑的高性能跨线程通道，使用 double-check lock
模式实现零丢失唤醒信号竞争。

#### 构造函数

```text
FastChannel(maxsize: int = 0)
```

- **`maxsize`** (*int*)：通道容量上限。`0` 表示无界通道。

#### 方法

##### `async send(item: Any)` -> `None`
向通道发送一个元素。通道满时挂起直到有空间。
- **抛出**：通道已关闭时抛 `ChannelClosedError`。

##### `async recv(timeout: float | None = None)` -> `Any`
从通道接收一个元素。为空时挂起直到有元素。
- **`timeout`** (*float | None*)：超时秒数。
- **抛出**：关闭且为空时抛 `ChannelClosedError`，超时抛 `TimeoutError`。

##### `try_send(item: Any)` -> `bool`
非阻塞发送。成功入队返回 `True`，通道满返回 `False`。
- **抛出**：通道已关闭时抛 `ChannelClosedError`。

##### `try_recv()` -> `Any`
非阻塞接收。
- **抛出**：为空时抛 `WouldBlock`，关闭且为空抛 `ChannelClosedError`。

##### `qsize()` -> `int`
返回当前缓冲在通道中的元素数量。



##### `close()` -> `None`
关闭通道。所有挂起的发送者/接收者以 `ChannelClosedError` 被唤醒。

##### `is_closed` -> `bool`
是否已关闭。

##### `__aiter__()` & `__anext__()`
支持 `async for item in ch:` 迭代。通道关闭且为空时自动终止。

---

### `select_channel`

```text
async def select_channel(
    *channels: Any,
    timeout: float | None = None,
    default: Any = ...,
) -> Any
```

从多个 `FastChannel` 中选择第一个就绪的通道（Go
`select` 风格）。

- **返回**：`(selected_channel, value)`。
- **`default`**：提供时 `select_channel` 变为非阻塞：用 `try_recv()` 依次
  尝试每个通道，第一个就绪的返回 `(channel, value)`，全不就绪返回
  `default`。
- **抛出**：无通道传入抛 `ValueError`，`timeout` 到期仍无通道就绪抛
  `TimeoutError`，**所有**通道都已关闭且为空时抛 `ChannelClosedError`
  （关闭且为空的通道永远不会就绪，等待会永久挂起）。

**关闭语义**（U5-FIX-19）：
- 已关闭但仍缓冲着数据的通道照常报告就绪并返回数据——关闭不会销毁
  缓冲值。
- 只要还有通道开着，关闭且为空的通道会被静默忽略（select 继续等待
  开着的通道）。
- 当**所有**通道都关闭且为空时，`select_channel` 抛
  `ChannelClosedError` 而不是挂起。

**示例**：
```python
selected_ch, val = await select_channel(ch1, ch2, timeout=2.0)
```

---

## 任务管理 (Task Management)

### `TaskGroup`

派生并管理子任务的异步上下文管理器，底层由 `CancelScope` 驱动取消传播。
灵感来自 trio 的 nursery 与 anyio 的 `TaskGroup`。

```python
async with TaskGroup(name=None) as tg:
    h1 = tg.start_soon(worker, "a")
    h2 = tg.start_soon(worker, "b")
# Both tasks are guaranteed finished here.
```

- **`start_soon(coro_fn, *args)` -> `TaskHandle`**：派生子任务并立即返回
  句柄，不阻塞。进入组之前派生的子任务由首次进入纳入追踪。
- **`start(coro_fn, *args)` -> `TaskHandle`**：派生子任务，阻塞直到子任务
  调用 `task_status.started()`。
  - **抛出**：组退出后调用抛 `RuntimeError`（与 `start_soon` 相同的孤儿防护）；子任务未调用 `task_status.started()` 即退出也抛 `RuntimeError`（trio/anyio 对齐——静默返回已死任务的句柄会掩盖协议违规）。
- **抛出**：子任务异常在退出时重抛；多个失败以 `ExceptionGroup` 聚合。被取消的子任务不作为错误上报（trio/anyio 对齐）；宿主在组等待期间被取消时，保证所有子任务完成后再退出块。

### `TaskHandle`

`TaskGroup` 内子任务的句柄，由 `TaskGroup.start_soon()` 与
`TaskGroup.start()` 返回。await 句柄返回任务结果（或抛出其异常）。

#### 属性

- **`status`** -> `_TaskStatus`：当前生命周期状态——`_TaskStatus.PENDING`、
  `_TaskStatus.STARTED` 或 `_TaskStatus.FINISHED`。
- **`result`** -> `Any`：任务完成后的结果。
  - **抛出**：任务未完成时抛 `RuntimeError`。
- **`exception`** -> `BaseException | None`：任务的异常；成功则为 `None`。
  - **抛出**：任务未完成时抛 `RuntimeError`。

#### 可等待

`await handle` 返回任务结果并重抛其异常。

```python
h = tg.start_soon(worker, "a")
result = await h
```

### `TaskStatus`

配合 `TaskGroup.start()` 使用的状态跟踪器。被派生的协程以
`TaskStatus` 实例作为第一个参数，初始化完成后调用 `started()`，解除
`TaskGroup.start()` 的阻塞以返回句柄。

```python
async def worker(task_status: TaskStatus):
    await setup()
    task_status.started()  # unblocks TaskGroup.start()
    await run()


async with TaskGroup() as tg:
    h = await tg.start(worker)
```

- **`started()`**：标记任务已启动，解除挂起的 `TaskGroup.start()`。

### `CancelScope`

按任务层级传播取消的任务本地取消作用域。

```text
CancelScope(deadline: float = float("inf"), shield: bool = False)
```

- **`deadline`** (*float*)：绝对单调截止时间。`float("inf")` 表示无截止。
- **`shield`** (*bool*)：快照/恢复式屏蔽：进入时快照并清除任务挂起的取消计数，退出时恢复。只吸收*进入前*已注入的取消；屏蔽*期间*到达的取消**不会**被延迟（与 trio/anyio 不同）——需要在中途取消下完成清理的代码应使用重试循环（见 `Condition.wait`）。

#### 属性与方法

- **`cancel_called`** -> `bool`：调用过 `cancel()` 后为 `True`。
- **`cancelled_caught`** -> `bool`：scope 静默吸收了取消（move-on-* scope）
  为 `True`。
- **`deadline`** -> `float`：绝对单调截止时间。
- **`shield`** -> `bool`：是否阻止父取消。
- **`cancel()`**：把 scope 标记为已取消并向宿主任务注入取消。幂等。

退出时 scope 总是消费自己的注入：*在 body 内被捕获*（或从未投递）的取消
绝不会把计数泄漏到外层作用域——泄漏的计数会被外层 shield 当作真实取消
快照，并在其退出后重新注入。

#### 上下文管理器
`async with CancelScope(...) as scope:` 把 scope 压入当前任务的 scope 栈。

---

## 同步与控制原语 (Synchronization & Control Primitives)

### `Lock`

跨线程异步互斥锁。

```python
lock = Lock()

async with lock:
    critical_section()
```

- **`locked`** -> `bool`：锁是否被持有。
- **`owner`** -> `asyncio.Task | None`：当前持有锁的任务（如有）。

> **诊断属性**（`locked`/`owner`/`Semaphore.value`/`qsize`/`Event.is_set`/`TaskHandle.status`）是各自内部锁下的一致快照——彼此之间**不是**原子的，也不能替代并发下的真实 acquire/wait 操作（FIX-14）。
- **`acquire()`**：获取锁，挂起直到空闲。
- **`release()`**：释放锁。必须由持有者调用。

### `Semaphore`

异步计数信号量。

```text
Semaphore(max_value: int)
```

- **`value`** -> `int`：当前可用令牌数。
- **`max_value`** -> `int`：最大令牌数。
- **`acquire()`**：获取一个令牌，挂起直到可用。
- **`release()`**：归还一个令牌。

### `CapacityLimiter`

支持分数令牌的令牌桶式限流器。

```text
CapacityLimiter(total_tokens: float)
```

- **`total_tokens`** -> `float`：总容量（可读写）。
- **`available_tokens`** -> `float`：当前可用令牌。
- **`borrowed_tokens`** -> `float`：当前借出的令牌。
- **`snapshot()`** -> `(total, available, borrowed)`：在**一次**锁获取下
  原子一致地读取三个计数器。当其他线程可能并发调整 `total_tokens` 时，
  请用此方法而不是分别读三个属性——分开读可能混入针对不同总额计算出的
  值。
- **`acquire()`**：获取一个令牌，挂起直到可用。
- **`release()`**：归还一个令牌。

### `Event`

跨线程异步事件标志。

```text
Event()
```

- **`is_set`** -> `bool`：调用过 `set()` 后为 `True`。
- **`set()`**：置位并唤醒所有等待者。
- **`wait()`**：挂起直到事件被置位。

### `Condition`

跨线程异步条件变量，通常与 `Lock` 配对使用。

```text
Condition(lock: Lock | None = None)
```

- **`acquire()`** / **`release()`**：管理底层锁。
- **`wait()`**：原子地释放锁并挂起直到被通知。
- **`notify(n=1)`**：唤醒 `n` 个等待任务。
- **`notify_all()`**：唤醒所有等待任务。

### `Barrier`

阻塞直到固定数量 party 到齐的跨线程异步屏障。

```text
Barrier(parties: int)
```

- **`parties`** -> `int`：触发屏障所需的 party 数。
- **`n_waiting`** -> `int`：当前等待的 party 数。
- **`wait()`** -> `BarrierWaitResult`：阻塞直到所有 party 到齐。
- **`abort()`**：中止屏障，唤醒所有等待者。

### `BarrierWaitResult`

`Barrier.wait()` 返回的结果对象。

- **`parties`** -> `int`：本回合屏障的 party 总数。

### `AsyncContext`

Go `context.Context` 风格的取消上下文，支持跨线程级联任务取消。

```text
ctx = AsyncContext(parent: AsyncContext | None = None)
```

- **`ctx.cancel()`**：取消本上下文，并以线程安全方式级联取消所有子上下文
  与已提交的 future。
- **`ctx.submit(pool, target, *args, **kwargs)`**：向池提交绑定到本上下文
  的任务。
- **`ctx.parent`**：父上下文，根上下文为 `None`（只读，构造时固定）。
- **`ctx.is_cancelled`**：已取消则返回 `True`。

---

### `AsyncWaitGroup`

Go `sync.WaitGroup` 风格的跨线程同步原语。

```python
wg = AsyncWaitGroup()
wg.add(2)
# In worker tasks: wg.done()
await wg.wait()
```

所有正数 `add()` 调用必须先于同一计数器的第一次 `wait()`（与 Go `sync.WaitGroup` 一致）：并发的 `add()` 与 `wait()` 竞争会丢失唤醒并永久挂起。允许负数增量（Go 语义），但不得把计数器减到 0 以下——否则抛出 `RuntimeError`。

---

### `AsyncOnce`

Go `sync.Once` 风格的跨事件循环线程单次执行原语。

```python
once = AsyncOnce()
res = await once.do(init_function, arg1)
```

若执行抛出异常，后续调用者重抛同一异常。若领袖任务被**取消**，领袖重抛
`CancelledError`，而后续调用者收到 `RuntimeError("AsyncOnce execution was cancelled")`
——在无关任务中抛出的 `CancelledError` 会静默地把该任务标记为已取消。

---

### `AsyncRWMutex`

Go `sync.RWMutex` 风格的读写锁：允许多个并发读者，或单个独占写者。

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

## 网络与 ASGI Worker (Networking & ASGI Workers)

### `ConnectionPinningServer`

把每个入站客户端 TCP 连接钉到特定 Worker 事件循环线程，实现零跨线程
系统调用开销。

```python
async with ConnectionPinningServer(pool, host="127.0.0.1", port=8080) as server:
    await server.start(handler_coro)
```

---

### `GsyncioASGIWorker`

把 FastAPI / Starlette / ASGI 3.0 应用直接挂载到 `EventLoopThreadPool` 上。

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

## 异常 (Exceptions)

- **`GsyncioError`**：所有 `gsyncio` 错误的基类。

### `GsyncioError`

所有 `gsyncio` 错误的基类。

### `ChannelClosedError`

`ChannelClosedError(GsyncioError)` —— 对已关闭通道进行操作时抛出。

### `ThreadPoolClosedError`

`ThreadPoolClosedError(GsyncioError, RuntimeError)` —— 向已关闭的线程池
提交任务时抛出。

### `TimeoutError`

`TimeoutError(GsyncioError)` —— 并发操作超时时抛出。

### `WouldBlock`

`WouldBlock(GsyncioError)` —— 非阻塞操作（如 `try_recv()`）无法立即继续
时抛出。
