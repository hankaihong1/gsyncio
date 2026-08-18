# examples/ — Runnable Examples

**[中文版 (Chinese)](README_ZH.md)**

After cloning (run `make develop` first to compile the Rust core), execute
directly:

```bash
uv run python examples/00_pool_basics.py      # Thread pool: submit / loop pinning / submit_group / metrics
uv run python examples/01_channels_select.py  # Channels: send/recv / async for / select_channel / non-blocking
uv run python examples/02_waitgroup_once.py   # Group sync: AsyncWaitGroup / AsyncOnce (incl. exception caching)
uv run python examples/03_taskgroup_timeout.py# Structured concurrency: TaskGroup / fail_after / move_on_after / CancelScope
uv run python examples/04_sync_primitives.py  # Sync primitives: Lock / Semaphore / Event / Condition / Barrier
uv run python examples/05_asgi_websocket.py    # ASGI 3.0: Lifespan management / WebSocket full-duplex communication
uv run python examples/06_wsgi_flask.py         # WSGI 1.0.1: Synchronous Flask/Django apps on worker pool
```

Each script is self-contained (`async def main()` + `asyncio.run`), depends
only on `multiloop` and the standard library `asyncio`, and prints its results
for visual verification.

> Choosing the right primitive? See the decision table in
> [docs/CHOOSING.md](../docs/CHOOSING.md).
> Touching concurrency code? Read [docs/CONCURRENCY.md](../docs/CONCURRENCY.md) first.
