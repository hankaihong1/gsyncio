# examples/ — 可运行示例

**[English](README.md)**

克隆仓库后（需先 `make develop` 编译 Rust 核心），直接运行：

```bash
uv run python examples/00_pool_basics.py      # 线程池：submit / loop 钉选 / metrics
uv run python examples/01_channels_select.py  # 通道：send/recv / async for / select_channel / 非阻塞
uv run python examples/02_waitgroup_once.py   # 组同步：AsyncWaitGroup / AsyncOnce（含异常缓存）
uv run python examples/03_taskgroup_timeout.py# 结构化并发：TaskGroup / fail_after / move_on_after / CancelScope
uv run python examples/04_sync_primitives.py  # 同步原语：Lock / Semaphore / Event / Condition / Barrier
uv run python examples/05_asgi_websocket.py    # ASGI 3.0：Lifespan 生命周期管理 / WebSocket 全双工通信
uv run python examples/06_wsgi_flask.py         # WSGI 1.0.1：同步 Flask/Django 框架在 Worker 池无阻塞执行
```

每个脚本是独立完整的（`async def main()` + `asyncio.run`），只依赖
`multiloop` 与标准库 `asyncio`，输出即结果，肉眼可验证。

> 想先选对原语？看 [docs/CHOOSING_ZH.md](../docs/CHOOSING_ZH.md) 决策表。
> 改并发代码前？看 [docs/CONCURRENCY_ZH.md](../docs/CONCURRENCY_ZH.md)。
