# multiloop 并发正确性指南（Concurrency Correctness Guide）
**[English](CONCURRENCY.md)**


> **修改任何并发相关代码前必读**。本指南面向两类读者：AI 代理（写代码前
> 逐条过第 3 节检查清单）与人类贡献者（理解每个原语的并发模型再动手）。
> 每个论断都标注了源码位置，可跳转验证。

---

## 1. 并发模型总览（谁锁什么、谁等什么）

multiloop 的同步原语遵循同一个骨架：**Python 侧一把 `threading.Lock` 保护
waiter 结构，Rust 侧用原子操作 + flume 承载数据面**。跨线程唤醒一律走
`loop.call_soon_threadsafe(...)`，绝不跨线程直接触碰 asyncio 对象。

### 1.0 并发物理假设公理体系（Physical Concurrency Axioms）

multiloop 借用了 AnyIO 和 Trio 的顶层 API 命名与人体工程学接口，但在底层**彻底脱离其单线程无竞争假设**。在单线程协作式运行时（AnyIO/Trio）中，协程在 `await` 之间的执行是绝对串行非抢占的，容器操作无需 OS 锁，原语仅作为协作式并发量节流阀。

与之相反，multiloop 运行在 **Python 3.14t（自由线程 / 无 GIL）多核物理真并行**之上的多 OS 线程与独立事件循环中。并发正确性不能依赖单线程直觉，必须建立在以下七项严密的形式化物理假设与公理之上：

0. **与 AnyIO/Trio 的范式分水岭（API 借用 vs 多线程物理公理）**：AnyIO/Trio API（`TaskGroup`、`CancelScope`、`CapacityLimiter`、`Event` 等）仅作为人体工程学调用界面被借用。所有底层语义完全基于多线程/多 Loop 物理模型重新设计，严格依赖状态机代数闭包、代币守恒律与 OS 互斥锁同步。
1. **Python 3.14t 自由线程（无 GIL）真并行假设**：字节码与对象读写在多核 CPU 上真并行执行，无全局解释器锁保护。任何涉及复合状态的操作（如计数器增减 + 等待者队列出入队）必须收敛在单一 OS 互斥锁（`threading.Lock` / `parking_lot::Mutex`）临界区内，杜绝复合锁竞争撕裂。
2. **跨线程 EventLoop 物理隔离公理**：`asyncio.Task`、`Future` 与 `Event` 严格物理绑定于单一 OS 线程及其 EventLoop。跨线程通信与唤醒绝对只能通过 `loop.call_soon_threadsafe(...)` 或 Rust `Channel` 派发。
3. **ContextVar 线程/任务局部性公理**：`contextvars.ContextVar` 严格归属于当前 OS 线程与当前 Task。跨线程取消（`scope.cancel()`）严禁跨线程读取 ContextVar 栈，仅依赖线程互斥锁与 `call_soon_threadsafe`。
4. **asyncio 原生取消与单账本对称记账公理**：完全基于 Python 3.11+/3.14t 原生 `task.cancelling()` 与 `task.uncancel()` 机制。Scope 仅对其显式注入的取消（`_injected == True`）执行对称的 `task.uncancel()` 冲销，绝不误吞外部第三方取消。Shield 严格采用快照-恢复（Snapshot-and-Restore）模型。
5. **数据面与等待面物理分离公理**：数据流由 Rust 核心无锁承载（`flume` + 64 字节对齐原子计数器），异步等待队列由 Python `threading.Lock` 保护并执行双检锁协议。
6. **结构化并发物理作用域公理**：`TaskGroup` 物理限定于单个 `asyncio.AbstractEventLoop` 内部。跨 Loop、跨线程并发由 `EventLoopThreadPool`、`Channel` 与 `AsyncContext` 协同编排。

### 1.1 通道类（Channel）

| 组件 | 锁/原语 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| Rust `Channel` | flume channel（有界/无界）+ `AtomicBool is_closed` | 无 | `try_send` 返回 `false` 仅表示满；**关闭后先排空再报错**——与 `close()` 竞争的 send 可能仍短暂入队（flume 侧惰性关闭），因此"已关闭"通道可能先短暂接收再排空，之后所有操作才报错（`src/lib.rs:387-418`；R4 决议：比 Go 的关闭后 send panic 更宽容） |
| Python `_BaseChannel` | `threading.Lock`（`_lock`） | `_getters` / `_putters` 两个 `deque[(loop, future)]` | waiter 注册与唤醒必须在 `_lock` 下完成（`src/multiloop/_channel_base.py:75-78`） |
| 唤醒协议 | — | — | `_wake_all` 从 deque **左侧消费式**唤醒：唤醒一个就弹出，stale future 自然丢弃（`_channel_base.py:29-50`） |

**数据面与等待面分离**：flume 管数据（无锁），Python 锁只管"谁在等"。
`send`/`recv` 都是「锁外快路径尝试 → 锁内双检 → 锁内注册 future → await →
取消时锁内注销」（`_channel_base.py:136-177`、`primitives.py:165-197`）。

### 1.2 锁与信号量

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Lock` | `threading.Lock` | `deque[(task, asyncio.Event)]` | FIFO；owner 死亡（`_owner.done()`）自动回收，不泄漏锁（`_sync.py:56-62`）；release 时跳过已 done 的 waiter（`_sync.py:90-97`） |
| `Semaphore` | `threading.Lock` | `deque[(loop, asyncio.Event)]` | FIFO；取消的 waiter 若已被 release 弹出，令牌**转发**给下一个 waiter 或归还池（`_sync.py:192-213`） |
| `CapacityLimiter` | `threading.Lock`（`_lock`） | `deque[(loop, asyncio.Event)]` | 单锁原子模型：`available + borrowed == total` 在 `_lock` 下恒成立；3.14t 下原子借还与动态亏空吸收（`_sync.py:329-497`） |

### 1.3 事件与条件

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Event` | `threading.Lock` | `list[(loop, asyncio.Event)]` | **sticky**（trio 语义，无 clear）；`set()` 锁内换出列表、锁外逐个 `call_soon_threadsafe`（`_sync.py:385-398`） |
| `Condition` | `_waiters_lock`（waiter 队列）+ 底层 `Lock` | `deque[(loop, asyncio.Event)]` | `notify()` **不需要**持底层锁（`_sync.py:507-522`）；`wait()` 释放锁 → 等通知 → **shield 下重获锁且取消时自动转发通知**（`_sync.py:624-646`） |

### 1.4 屏障与组同步

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Barrier` | `threading.Mutex` | `list[(loop, asyncio.Event, box)]` | `_generation` 计数器绑定轮次；轮次完成前有任意 party 被取消，屏障自动进入 Broken 状态并唤醒其余所有 party 抛出异常，并自增代际（`_sync.py:774-896`） |
| `AsyncWaitGroup` | Rust：`parking_lot::Mutex<WaitGroupInner>` | `Vec[(loop, future)]` | 单互斥锁统一管理 `counter`、`generation` 和 `waiters`；`add(-n)` 与 `done()` 归零时原子递增 `generation` 并通过 `mem::take` 移交全部 waiter 列表（`lib.rs:465-540`）；`register_waiter` 对已归零状态立即返回已就绪；提供 `holding()` 上下文管理器支持 RAII 追踪 |
| `AsyncOnce` | `threading.Lock` | `deque[(loop, future)]` | leader/follower：锁内决定谁执行、follower 锁内注册，leader 在 finally 中**持锁** `_wake_all`——注册与唤醒在同一把锁下，无 lost-wakeup 窗口（`primitives.py:377-416`） |

### 1.5 取消与结构化并发

| 组件 | 机制 | 关键点 |
|---|---|---|
| `CancelScope` | 每任务 contextvars 栈 + 单账本 `_injected` | shield 进入时 snapshot 取消计数并清零，退出时恢复；单账本精确跟踪实际注入并对称冲销；严格 RAII 作用域栈生命周期（`_cancel.py`） |
| `select_channel` | 两阶段仲裁器（Phase 1 伪随机均匀快速 `try_recv`，Phase 2 单播唤醒多通道注册） | 废除 TaskGroup 投机取消，赢家通过 `try_recv()` 消费并在 `finally` 中切除所有通道 watcher 注册（`primitives.py:230-290`） |

---

## 2. 已知陷阱模式（Race-Condition Trap Patterns）

每个模式：症状 → 根因 → 正确姿势 → 源码例证。改代码时对照这里，改完
逐条过第 3 节清单。

### 模式 1：check-then-act 窗口

**症状**：先检查后操作的两步之间被并发修改，操作失败或读到脏状态。
**根因**：检查与操作不是原子的。
**正确姿势**：合并为单次锁内操作；无法合并时用 try/except 兜底。
**例证**：
- `CapacityLimiter` 统一由单一把 `_lock` 保护全部 token、借出与 waiter 状态；调用 `snapshot()` 原子上锁读出 `(total, avail, borrowed)`（`_sync.py:329-497`）。
- 通用教训（他处修复过）：`contains_key → getitem` 之间 key 可被并发删除
  → 拆开两步 + `try/except KeyError`。

### 模式 2：double-checked lock 的 lost-wakeup

**症状**：发送方成功入队后唤醒一个"已经不存在"的接收者，或接收方注册
future 时数据已经被取走，future 永远没人 resolve → 挂死。
**根因**：无锁快路径（`try_send`/`try_recv`）与加锁的 waiter 列表是两个
不同的状态面，快路径赢过慢路径时唤醒信号丢失。
**正确姿势**：快路径尝试后，**在锁内再检查一次**才注册 future；注册与
`_wakeup_next` 都在 `_lock` 下完成。
**例证**：`_wait_and_send`（`_channel_base.py:136-177`）、`_recv_impl`
（`primitives.py:165-197`）——这就是 AGENTS.md 六项决策里的 "Double-check
lock in Channel"。

### 模式 3：取消路径的锁重获取（shield 缺失 → 死锁）

**症状**：`Condition.wait()` 被取消后，`finally`/异常路径里重获取锁时，
`await lock.acquire()` 立刻再抛 `CancelledError`，锁永远拿不回 → 任务
卡死在清理路径，锁泄漏。
**根因**：Python 的取消是"每次 await 都会重抛"的，清理代码里的 await
不能执行。
**正确姿势**：用"取消重试循环"重获取锁——`asyncio.Condition.wait` 的官方模式（bpo-34094 家族）：吞掉每次重获取尝试抛出的 `CancelledError` 并重试，直到锁拿回，再重抛取消。每次被吞的取消都会让 `Lock` 保持干净（其取消路径会清理 waiter 条目），所以重试是安全的——而且能扛住重获取期间*反复*到达的取消。`CancelScope(shield=True)` 做不到这一点：它是快照/恢复式，只吸收*进入前*已注入的取消；屏蔽期间到达的取消会打断 body 的 await。
**例证**：`Condition._reacquire_lock`（`_sync.py:718-738`）；`AsyncRWMutex` 的 reader/writer 释放路径（`rwlock.py:78-111, 153-175`）。

### 模式 4：通知与等待的锁不一致

**症状**：通知方需要持有某把锁才能 notify，而等待方在等通知前**已经释放
了那把锁** → 通知方永远拿不到锁 → 死锁。
**根因**：notify 依赖的锁与 wait 释放的锁不是同一把，或 notify 被错误地
要求持锁。
**正确姿势**：`notify()` 不要求底层锁；waiter 队列用独立的
`_waiters_lock` 保护，`notify` 只操作队列、不碰业务锁。
**例证**：`Condition.notify()`（`_sync.py:507-522`）——持锁 notify 还会
让生产者批量攒通知、饿死 waiter。

### 模式 5：轮次/代际错位（wrong-round removal）

**症状**：上一轮的取消/超时处理删掉了**下一轮**的 waiter 条目，屏障永远
凑不齐 party 数。
**根因**：waiter 条目复用了，但删除逻辑不知道条目已经属于新一轮。
**正确姿势**：waiter 注册时记录当时的 `_generation`，取消处理先比对
generation 再决定是否删除。
**例证**：`Barrier.wait()`（`_sync.py:628-643`）；同样手法在
`Semaphore._cancel_waiter`（令牌转发而非盲删，`_sync.py:192-213`）。

**Barrier Broken 注**：generation 守卫保护的是*下一轮*条目，且当轮次凑齐前有 party 被取消时，屏障自动转移至 Broken 状态并唤醒当前轮次所有等待者抛出异常，从根源杜绝死锁。

**已知限制**：`Barrier`/`Condition`/`Semaphore` 取消处理里的
删除是每个 waiter O(n)（重建列表/dict），N 个 waiter 的取消风暴总成本
O(n²)——实测 5000 waiter ≈ 560 ms。真实 party 数量级下可
接受；记录在案，防止后人"优化"成 wrong-round bug。

### 模式 6：注册-完成竞态（register-after-done）

**症状**：`wait()` 检查"计数已为 0"后正要注册，`done()` 恰好把计数减到 0
并清空 waiter 列表 → 新 waiter 永远不被唤醒。
**根因**：检查与注册之间的窗口。
**正确姿势**：注册后由注册函数返回"是否已完成"；已完成则不等待直接返回
（Go `sync.WaitGroup.Wait` 同款语义）。
**例证**：`AsyncWaitGroup.wait()` + `register_waiter` 的布尔返回
（`primitives.py:332-339`、`lib.rs:473-487`），内部受 `parking_lot::Mutex` 保护。注意：**所有正 delta 的 `add()` 必须 happens-before `register_waiter()`**，确保调用方不会在工作协程启动前误观察到计数器为 0 并提前放行。

### 模式 7：跨线程共享 asyncio 对象

**症状**：一个 loop 的 `asyncio.Event`/`Task` 被另一个线程直接 set/cancel
→ `RuntimeError` 或静默丢失。
**根因**：asyncio 对象不是线程安全的；`call_soon_threadsafe` 是唯一安全
的跨线程注入通道。
**正确姿势**：跨线程一律 `loop.call_soon_threadsafe(fut.set_result, ...)`
或 `call_soon_threadsafe(event.set)`；线程间通信用 `Channel`，不要
直接传 asyncio 原语。
**例证**：`_wake_all`（`_channel_base.py:41-48`）、`Event.set`
（`_sync.py:397-398`）、`Lock.release`（`_sync.py:96`）。

### 模式 8：死锁家族（锁顺序 / await 持锁 / 递归持锁）

**症状**：两个线程各自持有对方要的锁；或持锁时 await；或同一线程重复
获取非重入锁。
**正确姿势**：
- 多锁场景固定加锁顺序（如总是先 `_total_lock` 再碰 semaphore 内部锁）；
- **绝不在持锁期间 await**（`threading.Lock` 无法跨 await 保持，等价于
  释放了锁还以为是持锁状态——`asyncio.Lock` 才有此语义）；
- 调用可能再取锁的函数前先 `drop(guard)` 释放已持锁。
**例证**：`push_local` 满队列回退时先 `drop(guard)` 再 `push_global`
（`lib.rs:276-280`）。

### 模式 9：组件特定反模式与误用陷阱

**症状**：由于违反各组件特定的 API 契约或并发边界，导致运行时报错、死锁、饿死或限流失效。

**1. `AsyncWaitGroup.track` 协程对象误传给 Callable 接收方**：
- **陷阱**：`wg.track(coro)` 在调用瞬间即执行 `self.add(1)`，并返回包裹了 `finally: self.done()` 的协程对象（Coroutine Object）。若误传给期望 Callable 的 `TaskGroup.start_soon`，会引发 `TypeError` 并导致计数器永久泄漏与死锁。
- **错误示例**：
```python
async def worker():
    pass


async with TaskGroup() as tg:
    wg = AsyncWaitGroup()
    tg.start_soon(wg.track(worker()))  # TypeError! 计数器永久泄漏!
```
- **正确姿势**：
```python
async with TaskGroup() as tg:
    wg = AsyncWaitGroup()

    async def tracked_worker():
        await wg.track(worker())

    tg.start_soon(tracked_worker)
```

**2. `select_channel` 高吞吐饱和社会下的顺序饿死**：
- **陷阱**：`select_channel` 在 Phase 1 中按照参数由左至右顺序确定性探测。若多个通道持续有数据堆积，右侧通道会被 100% 饿死。
- **错误示例**：
```python
while True:
    ch, val = await select_channel(ch1, ch2)
    process(val)
```
- **正确姿势**：若需要就绪通道间的统计学公平调度，在循环轮询前打乱（shuffle）通道列表。

**3. `Barrier` 单方超时与自动破损状态机**：
- **陷阱**：当某个等待方被取消或超时退出时，`Barrier` 会自动进入 Broken 破损状态并唤醒当前轮次的所有其他等待方抛出 `RuntimeError`。在此之后，后续的 `wait()` 会持续抛出异常，直到显式调用 `barrier.reset()`。
- **错误示例**：
```python
try:
    await asyncio.wait_for(barrier.wait(), timeout=1.0)
except TimeoutError:
    pass  # 屏障已破损；后续 wait() 调用将直接失败!
```
- **正确姿势**：
```python
try:
    await asyncio.wait_for(barrier.wait(), timeout=1.0)
except TimeoutError:
    barrier.reset()  # 重置屏障代际以允许后续轮次继续运行
    raise
```

**4. `CapacityLimiter` 匿名归还与浮点容量感知断层**：
- **陷阱**：`CapacityLimiter` 是匿名令牌模型（不校验借用者 Task 身份），且浮点容量（如 `2.5`）底层信号量仅有整型容量（`2`）。
- **错误示例**：
```python
if limiter.available_tokens > 0:  # 例如 0.5 > 0
    await limiter.acquire()  # 若整型容量已借满则必然阻塞!
```
- **正确姿势**：统一推荐 `async with limiter:` 上下文管理器；使用 `limiter.available_capacity >= 1`（或 `available_tokens >= 1.0`）确保可立即无阻借出。

---

## 3. 修改前必查清单（Pre-Change Checklist）

**改并发相关代码（通道/锁/信号量/事件/条件/屏障/waitgroup/取消/pool）
前，逐条核对；任何一条不满足，先想清楚再动手。**

- [ ] 改动是否新增/修改 **waiter 注册或唤醒路径**？→ 过模式 2/4：注册与
      唤醒是否在同一把锁下？是否有锁外快路径+锁内双检？
- [ ] 改动是否触及 **取消/超时路径**？→ 过模式 3/5：清理路径的 await
      是否在 shield 内？waiter 删除是否带 generation/身份校验？
- [ ] 改动是否让 **asyncio 对象跨线程**？→ 过模式 7：跨线程只允许
      `call_soon_threadsafe`。
- [ ] 改动是否引入 **多个锁**？→ 过模式 8：锁顺序是否一致？是否在 await
      期间持锁？调用可能取锁的函数前是否先释放已持锁？
- [ ] 改动是否涉及 **计数/令牌的增减**？→ 过模式 1/6：check 与 act 是否
      原子？取消/失败时令牌是否转发或归还（不丢失、不凭空多出）？
- [ ] Rust 侧改动是否触碰 **poller 计数、batch pull 或原子操作**？→ 见
      第 4 节：RAII guard 是否保住计数？内存序是否够用？
- [ ] 新行为是否针对**目标测试文件/函数**用 `pytest-repeat --count=50` 压测
      （禁止全量套件盲目多倍压测）？测试是否加 `@pytest.mark.free_threading`（3.14t 专用压测）？
- [ ] 发现 bug 时：是否先写了**最小复现测试**（确定性数据、稳定 FAIL）
      才开始修？（见第 5 节强制流程）

---

## 4. Rust 核心注意事项（`src/lib.rs`）

### 4.1 三个组件、各自的并发边界

| 组件 | 线程边界 | 注意 |
|---|---|---|
| `NativeWorkerPool` | 任意线程可 `push_global`/`push_local`；`pop_work` 仅 worker 线程 | 关池先丢 sender 再置 flag（`lib.rs:219-225`） |
| `Channel` | 任意线程可 send/recv | flume 本身无锁；`is_closed` 是 flag store/load（Release/Acquire） |
| `RawAsyncWaitGroup` | 任意线程可 add/done/register | 计数 AcqRel；waiter 列表 parking_lot Mutex |

### 4.2 软 poller 门（`num_polling`）

- `fetch_add(1, Relaxed)` 后判断 `< max(num_workers/2, 1)`，**瞬时门**：
  抢到门只对本轮 batch pull 有效，释放即失效——没有硬角色分配，任何
  worker 随时可以偷全局队列（`lib.rs:301-303`）。
- **必须用 `PollerGuard`（RAII）递减**：panic 也能保证 `fetch_sub`
  执行（`lib.rs:170-176`）。手写 `fetch_add`/`fetch_sub` 配对时，任何
  `?`/panic 路径都会泄漏计数 → 永远用 guard。

### 4.3 batch pull 的三层消费

优先级：**private buffer → global batch → local channel**（`lib.rs:287-352`）。
- batch_size = `min(global_len / num_workers + 1, 128)`，摊薄 flume pop 成本。
- 改优先级/批次公式时，同时检查：worker 是否会饿死全局队列（Python 侧
  `await asyncio.sleep(0)` 的防贪婪机制在 `pool.py` 的 `_worker_dispatcher`）。
- `push_local` 满队列**回退 global** 保 liveness（`lib.rs:276-280`）——
  别把它改成阻塞等待，否则单 worker 队列满会卡住提交方。

### 4.4 原子操作与内存序

- `AtomicMetrics` 每个计数器是 `#[repr(align(64))] PaddedAtomic`——防
  false sharing。**加新计数器时保持 64 字节对齐**，否则多核下吞吐会
  莫名下降（`lib.rs:14-15`）。
- 已确立的序约定，新代码沿用：
  - `is_closed`：`Release` store / `Acquire` load（flag 模式，`lib.rs:224, 232`）
  - WaitGroup 计数：`add` 用 `Release`，`done` 用 `AcqRel`（`lib.rs:439, 444`）
- flume 是 lock-free 但**不是零成本**：跨线程 send/recv 仍有原子操作与
  内存栅栏。性能敏感路径不要假设"无锁=免费"。

### 4.5 `register_waiter` 的顺序约束

`RawAsyncWaitGroup`（`lib.rs:548`）由 `parking_lot::Mutex<WaitGroupInner>` 单互斥锁保护：**调用方必须保证所有正 delta 的 `add()` 在 `register_waiter()` 之前 happens-before**（Go WaitGroup 同款约定）。否则在计数为 0 时调用 `wait()` 会立即返回已就绪，从而漏过后续启动的并发任务。

---

## 5. 测试方法论

### 5.1 并发回归三件套

```bash
# 压测单个测试文件（race 检测）：重复 50 次
uv run pytest tests/test_pool.py -p no:cacheprovider --count=50

# 死锁必失败而非挂死：pytest-timeout 180s + thread 模式，
# 超时自动 dump 所有线程栈（pyproject.toml 已配置）
# 卡住时看输出里的 "Thread dump" 定位 await 点

# 只跑上次失败
uv run pytest --lf
```

### 5.2 marker 语义

| marker | 用途 | CI 行为 |
|---|---|---|
| `slow` | 性能测试 | 默认跳过（`addopts = -m "not slow"`） |
| `free_threading` | 3.14t 自由线程压测 | 仅在 free-threaded 构建的 job 跑 |
| `repeat(n)` | 声明需要重复跑 | 与 `--count` 配合 |

新写的并发测试：能标 `free_threading` 就标；能确定性的就用
`pytest.mark.repeat` 声明压测意图。

### 5.3 测试辅助设施（`tests/conftest.py`）

- `skip_if_no_rust`：Rust 扩展未编译时跳过（本地没跑 `make develop` 也能
  跑纯 Python 部分）。
- `yielder` fixture → `wait_all_tasks_blocked()`：等所有任务阻塞后再断言，
  消除"还没开始就断言"的时序假阳性。

### 5.4 本地资源约束（M1 8GB 实测）

- **逐函数跑，不跑全量**；每次命令加 timeout（`pytest-timeout` 已全局
  配置，单测用 `--timeout=60` 更紧）。
- 3.14t 自由线程本地压测 **≤ 6 线程**，池线程数保持 4-8。
- CI 压力测试原则：**不降负载、延长超时**——本地压不过不代表 CI 压不过，
  反之亦然。

### 5.5 发现 bug 的强制流程（用户规则，不可跳过）

1. **先构造最小复现测试**：确定性数据（固定 seed/固定并发数，不用随机
   时序），能稳定 FAIL。
2. 用 `--count=50` 确认复现率，把复现测试留在测试套件里（回归测试）。
3. 修复后同一测试必须稳定 PASS，再跑 `--count=50` 确认无并发回归。
4. **禁止**"边改边试"或"只修不测"——没有复现测试的修复一律视为未完成。

---

## 6. 相关文档

- [API_ZH.md](API_ZH.md) — 完整 API 参考
- [CHOOSING_ZH.md](CHOOSING_ZH.md) — 原语选择决策表（"该用哪个"）
- [AGENTS.md](../AGENTS.md) — AI 导航（六项非显而易见设计决策的速查）