# gsyncio 并发正确性指南（Concurrency Correctness Guide）
**[English](CONCURRENCY.md)**


> **修改任何并发相关代码前必读**。本指南面向两类读者：AI 代理（写代码前
> 逐条过第 3 节检查清单）与人类贡献者（理解每个原语的并发模型再动手）。
> 每个论断都标注了源码位置，可跳转验证。

---

## 1. 并发模型总览（谁锁什么、谁等什么）

gsyncio 的同步原语遵循同一个骨架：**Python 侧一把 `threading.Lock` 保护
waiter 结构，Rust 侧用原子操作 + flume 承载数据面**。跨线程唤醒一律走
`loop.call_soon_threadsafe(...)`，绝不跨线程直接触碰 asyncio 对象。

### 1.1 通道类（FastChannel / AsyncChannel）

| 组件 | 锁/原语 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| Rust `FastChannel` | flume channel（有界/无界）+ `AtomicBool is_closed` | 无 | `try_send` 返回 `false` 仅表示满；closed 后一律报错（`src/lib.rs:387-418`） |
| Python `_BaseChannel` | `threading.Lock`（`_lock`） | `_getters` / `_putters` 两个 `deque[(loop, future)]` | waiter 注册与唤醒必须在 `_lock` 下完成（`src/gsyncio/_channel_base.py:75-78`） |
| 唤醒协议 | — | — | `_wake_all` 从 deque **左侧消费式**唤醒：唤醒一个就弹出，stale future 自然丢弃（`_channel_base.py:29-50`） |

**数据面与等待面分离**：flume 管数据（无锁），Python 锁只管"谁在等"。
`send`/`recv` 都是「锁外快路径尝试 → 锁内双检 → 锁内注册 future → await →
取消时锁内注销」（`_channel_base.py:136-177`、`primitives.py:165-197`）。

### 1.2 锁与信号量

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Lock` | `threading.Lock` | `deque[(task, asyncio.Event)]` | FIFO；owner 死亡（`_owner.done()`）自动回收，不泄漏锁（`_sync.py:56-62`）；release 时跳过已 done 的 waiter（`_sync.py:90-97`） |
| `Semaphore` | `threading.Lock` | `deque[(loop, asyncio.Event)]` | FIFO；取消的 waiter 若已被 release 弹出，令牌**转发**给下一个 waiter 或归还池（`_sync.py:192-213`） |
| `CapacityLimiter` | `_total_lock` + 内嵌 Semaphore | 同 Semaphore | `available + borrowed == total` 在单次 `_total_lock` 下成立（`snapshot()`，`_sync.py:308-321`） |

### 1.3 事件与条件

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Event` | `threading.Lock` | `list[(loop, asyncio.Event)]` | **sticky**（trio 语义，无 clear）；`set()` 锁内换出列表、锁外逐个 `call_soon_threadsafe`（`_sync.py:385-398`） |
| `Condition` | `_waiters_lock`（waiter 队列）+ 底层 `Lock` | `deque[(loop, asyncio.Event)]` | `notify()` **不需要**持底层锁（`_sync.py:507-522`）；`wait()` 释放锁 → 等通知 → **shield 下重获取锁**（`_sync.py:472-505`） |

### 1.4 屏障与组同步

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Barrier` | `threading.Mutex` | `list[(loop, asyncio.Event)]` | `_generation` 计数器绑定轮次；取消处理先查 generation 再删条目（`_sync.py:628-643`） |
| `AsyncWaitGroup` | Rust：`AtomicUsize` counter + parking_lot `Mutex` | `Vec[(loop, future)]` | `done()` 到 0 时 `mem::take` 整体移交 waiter 列表（`lib.rs:443-457`）；`register_waiter` 双检：无锁快路径 + 锁内复检（`lib.rs:473-487`） |
| `AsyncOnce` | `threading.Lock` | `deque[(loop, future)]` | leader/follower：锁内决定谁执行、follower 锁内注册，leader 在 finally 中**持锁** `_wake_all`——注册与唤醒在同一把锁下，无 lost-wakeup 窗口（`primitives.py:377-416`） |

### 1.5 取消与结构化并发

| 组件 | 机制 | 关键点 |
|---|---|---|
| `CancelScope` | 每任务 contextvars 栈 + `task.cancelling()`/`uncancel()` | shield 进入时 snapshot 取消计数并清零，退出时恢复（`_cancel.py:141-146, 184-189`） |
| `select_channel` | `TaskGroup` + 每个 channel 一个 reader | reader 成功读到后**主动抛 `CancelledError`** 触发组提前退出——正常 return 会让组等所有通道（`primitives.py:248-255`） |

---

## 2. 已知陷阱模式（Race-Condition Trap Patterns）

每个模式：症状 → 根因 → 正确姿势 → 源码例证。改代码时对照这里，改完
逐条过第 3 节清单。

### 模式 1：check-then-act 窗口

**症状**：先检查后操作的两步之间被并发修改，操作失败或读到脏状态。
**根因**：检查与操作不是原子的。
**正确姿势**：合并为单次锁内操作；无法合并时用 try/except 兜底。
**例证**：
- `CapacityLimiter` 的三个属性分别取 `_total_lock`，并发 resize 会读到
  混合值 → 用 `snapshot()` 单次锁内读完（`_sync.py:308-321`）。
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
lock in FastChannel"。

### 模式 3：取消路径的锁重获取（shield 缺失 → 死锁）

**症状**：`Condition.wait()` 被取消后，`finally`/异常路径里重获取锁时，
`await lock.acquire()` 立刻再抛 `CancelledError`，锁永远拿不回 → 任务
卡死在清理路径，锁泄漏。
**根因**：Python 的取消是"每次 await 都会重抛"的，清理代码里的 await
不能执行。
**正确姿势**：清理路径用 `CancelScope(shield=True)` 包住重获取，把父作用域
注入的取消计数清零，等锁拿回后再让取消生效。
**例证**：`Condition.wait()` 的取消分支（`_sync.py:493-500`）；shield 的
snapshot/restore 实现（`_cancel.py:141-146, 184-189`）。

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

### 模式 6：注册-完成竞态（register-after-done）

**症状**：`wait()` 检查"计数已为 0"后正要注册，`done()` 恰好把计数减到 0
并清空 waiter 列表 → 新 waiter 永远不被唤醒。
**根因**：检查与注册之间的窗口。
**正确姿势**：注册后由注册函数返回"是否已完成"；已完成则不等待直接返回
（Go `sync.WaitGroup.Wait` 同款语义）。
**例证**：`AsyncWaitGroup.wait()` + `register_waiter` 的布尔返回
（`primitives.py:332-339`、`lib.rs:473-487`）。注意 Rust 侧文档明确：
**所有正 delta 的 `add()` 必须 happens-before `register_waiter()`**，
否则无锁快路径仍可能输给并发 `add()`——调用方要遵守这个顺序约束。

### 模式 7：跨线程共享 asyncio 对象

**症状**：一个 loop 的 `asyncio.Event`/`Task` 被另一个线程直接 set/cancel
→ `RuntimeError` 或静默丢失。
**根因**：asyncio 对象不是线程安全的；`call_soon_threadsafe` 是唯一安全
的跨线程注入通道。
**正确姿势**：跨线程一律 `loop.call_soon_threadsafe(fut.set_result, ...)`
或 `call_soon_threadsafe(event.set)`；线程间通信用 `FastChannel`，不要
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
- [ ] 新行为是否用 `pytest-repeat --count=50` 压测（不能只跑单次）？测试
      是否加 `@pytest.mark.free_threading`（3.14t 专用压测）？
- [ ] 发现 bug 时：是否先写了**最小复现测试**（确定性数据、稳定 FAIL）
      才开始修？（见第 5 节强制流程）

---

## 4. Rust 核心注意事项（`src/lib.rs`）

### 4.1 三个组件、各自的并发边界

| 组件 | 线程边界 | 注意 |
|---|---|---|
| `NativeWorkerPool` | 任意线程可 `push_global`/`push_local`；`pop_work` 仅 worker 线程 | 关池先丢 sender 再置 flag（`lib.rs:219-225`） |
| `FastChannel` | 任意线程可 send/recv | flume 本身无锁；`is_closed` 是 flag store/load（Release/Acquire） |
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

`RawAsyncWaitGroup::register_waiter` 的无锁快路径（`lib.rs:474`）能输给
并发 `add()` 0→1：**调用方必须保证所有正 delta 的 `add()` 在
`register_waiter()` 之前 happens-before**（Go WaitGroup 同款约定）。修改
waitgroup 语义时不要破坏这个约束，否则出现"waiter 永不唤醒"的隐蔽挂死。

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
- `checkpoints` fixture → `assert_checkpoints(...)`：记录事件顺序的上下文
  管理器，验证交错顺序符合预期。

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