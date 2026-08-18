# multiloop 原语选择决策表（Primitive Selection Guide）
**[English](CHOOSING.md)**


> 30 秒找到正确的 API。按场景查下表；每个原语的核心思想见后文速览。
> 完整签名与参数见 [API_ZH.md](API_ZH.md)；并发语义见 [CONCURRENCY_ZH.md](CONCURRENCY_ZH.md)。
>
> **架构提示**：multiloop 虽借用了 AnyIO/Trio 的顶层 API 命名与人体工程学设计（如 `TaskGroup`、`CancelScope`、`CapacityLimiter` 等），但所有原语均运行在 **Python 3.14t 多线程多事件循环物理真并行** 之下，由 OS 互斥锁与形式化代币守恒律严密保护，绝非单线程协作式运行时的无锁假定。详见 [CONCURRENCY_ZH.md](CONCURRENCY_ZH.md)。

---

## 决策表（Decision Table）

| 你想做什么 | 用这个 | 为什么 |
|---|---|---|
| 把异步任务派到多线程池并行执行 | `EventLoopThreadPool.submit()` / `run_in_pool()` | 唯一能跨线程执行协程的入口；池生命周期用 `async with` 管理 |
| 显式创建池对象（延迟启动/自定义选项） | `create_pool(num_threads=..., **PoolOptions)` | 比 `run_in_pool` 更可控，返回可复用的池 |
| 一次提交多个任务、统一收结果 | `pool.submit_group()`（配合 `group.start_soon()`） | 批量分发 + 批量等待；池关闭时自动取消子任务 |
| 指定任务跑在某个固定 worker（有状态连接亲和） | `pool.submit(..., pin_to=N)` | 本地专用队列，确定性路由 |
| 任务间通信（Go channel 风格） | `Channel` | Rust flume 无锁通道，跨线程最快 |
| 多通道竞争取数（Go select 风格） | `select_channel(*channels)` | 最先就绪的通道胜出 |
| 只发/只收，接口上显式隔离方向 | `SendChannel` / `ReceiveChannel`（`ch.split()`） | 编译期/接口层防误用 |
| 优雅迭代数据流（直到 close） | `Channel`（`async for item in ch:`） | 迭代协议 + close 自动终止 |
| 只要一个现成通道（asyncssh 风格零配置） | `Channel()` | 直接构造，免建池 |
| 互斥访问共享资源 | `Lock` | 公平 FIFO，跨 loop/线程安全 |
| 限制并发数（N 个许可） | `Semaphore` | 固定许可池，FIFO 等待 |
| 动态调整并发上限 | `CapacityLimiter` | 可在运行期 resize 总额度 |
| 一次性广播"某状态已发生" | `Event` | sticky（trio 语义），set 后永不清除 |
| 等待条件满足（配合锁） | `Condition` | `wait()` 释放锁、`notify()` 无需持锁 |
| N 个任务互相等待到齐（回合同步） | `Barrier` | 每轮自动重置，可复用 |
| 等一组任务全部完成 | `AsyncWaitGroup` | Go `sync.WaitGroup` 语义，跨线程 |
| 只执行一次的初始化（单例） | `AsyncOnce` | 并发安全，只跑一次，结果共享 |
| 读多写少的共享状态 | `AsyncRWMutex` | `async with rw.reader():` 多读者并行，`async with rw.writer():` 独占 |
| 结构化并发：子任务必回收 | `TaskGroup`（`tg.start_soon()` + `TaskHandle`） | 离开作用域时所有子任务必定已结束 |
| 整体限时，超时抛异常 | `fail_after(sec)` / `fail_at(deadline)` | 到期抛 `TimeoutError` |
| 整体限时，超时静默跳过 | `move_on_after(sec)` / `move_on_at(deadline)` | 到期静默退出，查 `scope.cancelled_caught` |
| 精细控制取消（手动/屏蔽） | `CancelScope(deadline=..., shield=...)` | 手动 `scope.cancel()`；shield 阻止父取消穿透 |
| 长循环里定期检查取消 | `checkpoint()` | 同步代码里查取消点 |
| 查当前最紧的截止时间 | `current_effective_deadline()` | shield 会截断外部 deadline |
| 跨线程级联取消/超时广播 | `AsyncContext` | 树状传播取消到所有相关任务 |
| 跑 ASGI 服务（FastAPI、Starlette、WebSocket） | `MultiloopASGIWorker` | 支持 Lifespan 与 WebSocket 的多事件循环 ASGI 3.0 Worker |
| 跑同步 WSGI 服务（Django、Flask、PEP 3333） | `MultiloopWSGIWorker` | 线程池同步执行并通过无锁通道流式回传 |
| 长连接钉在固定 worker | `ConnectionPinningServer` | 连接亲和性，状态不迁移 |
| 记日志 / 调日志级别 | `get_logger()` / `set_log_level()` | 统一日志出口 |

---

## 原语速览（核心思想，一行一个）

- **`EventLoopThreadPool`** — 线程池 = N 个独立 asyncio loop；`submit` 走全局
  无锁队列 + 工作窃取；`pin_to=` 参数走本地专用队列。池是**异步上下文管理器**：
  `async with EventLoopThreadPool(num_threads=4) as pool:`。
- **`Channel`** — 数据面在 Rust（flume），等待面在 Python（双检锁）。
  有界（`maxsize=N`）或无界（默认）。跨线程可 send/recv。
- **`select_channel`** — 每个通道起一个 reader，谁先读到谁赢，其余 reader
  被取消；支持 `timeout=` 与 `default=`（非阻塞轮询模式）。
- **`AsyncWaitGroup`** — `add()` 计数，`done()` 递减，`wait()` 等到零。
  **规则：所有 `add()` 必须在 `wait()` 之前 happens-before**（Go 同款）。
- **`AsyncOnce`** — 第一个调用者是 leader 执行，其余 follower 等结果；
  异常也会缓存并重抛给所有 follower。
- **`TaskGroup`** — 子任务全部结束后才退出 `async with`；任一子任务失败会
  取消其余并聚合为 `BaseExceptionGroup`。
- **`CancelScope`** — 每任务一个 scope 栈；`shield=True` 把父作用域的取消
  挡在门外（内部清理代码可用）；`fail_after`/`move_on_after` 是它的便捷
  封装。
- **`Lock`/`Semaphore`/`Event`/`Condition`/`Barrier`** — 语义对标 Go +
  trio：FIFO 公平、跨 loop/线程安全、取消安全。`Event` 没有 `clear()`。
- **`AsyncContext`** — 全局级联取消树：一个 context 关联的任务/通道被取消
  时，整个子树一起取消。
- **`AsyncRWMutex`** — `async with rw.reader():` 共享读、`async with rw.writer():` 独占写；用法见 [API_ZH.md](API_ZH.md) 对应条目。
- **异常** — `ChannelClosedError`（通道已关闭）、`ThreadPoolClosedError`（池已关闭）、`TimeoutError`（multiloop 自己的超时，非内置）、`WouldBlock`（非阻塞操作无数据）、基类 `MultiloopError`。

---

## 快速上手（最小代码）

```python
import asyncio
import multiloop


async def main():
    # 池：提交 + 收结果
    async with multiloop.EventLoopThreadPool(num_threads=4) as pool:
        fut = pool.submit(asyncio.sleep, 0.01)
        await fut

    # 通道：跨任务通信
    ch = multiloop.Channel()
    await ch.send(42)
    print(await ch.recv())  # 42

    # 等一组任务
    wg = multiloop.AsyncWaitGroup()
    wg.add(1)
    wg.done()
    await wg.wait()


asyncio.run(main())
```

完整可运行示例见 [`examples/`](../examples/README_ZH.md)。