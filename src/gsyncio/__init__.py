"""gsyncio: Multi-event-loop engine & concurrency toolkit for Python 3.14t.

A high-performance, Go-like concurrency library for free-threaded Python,
built around a Rust core. Key capabilities:

- EventLoopThreadPool with round-robin scheduling and work stealing
- FastChannel and select_channel for Go-style communication
- CancelScope and TaskGroup for structured concurrency
- AsyncContext, AsyncWaitGroup, AsyncOnce, AsyncRWMutex primitives
- ASGI worker (GsyncioASGIWorker) for FastAPI-style servers

Docs: https://github.com/hankaihong1/gsyncio (docs/API.md, docs/sphinx_html/).
"""

from typing import Any

from gsyncio._cancel import (
    CancelScope,
    checkpoint,
    current_effective_deadline,
    fail_after,
    fail_at,
    move_on_after,
    move_on_at,
)
from gsyncio._logging import get_logger, set_log_level
from gsyncio._sync import (
    Barrier,
    BarrierWaitResult,
    CapacityLimiter,
    Condition,
    Event,
    Lock,
    Semaphore,
)
from gsyncio._taskgroup import TaskGroup, TaskHandle, TaskStatus
from gsyncio.asgi import GsyncioASGIWorker
from gsyncio.context import AsyncContext
from gsyncio.exceptions import (
    ChannelClosedError,
    GsyncioError,
    ThreadPoolClosedError,
    TimeoutError,
    WouldBlock,
)
from gsyncio.pool import (
    EventLoopThreadPool,
    PoolOptions,
    create_pool,
)
from gsyncio.primitives import (
    AsyncOnce,
    AsyncWaitGroup,
    FastChannel,
    select_channel,
)
from gsyncio.rwlock import AsyncRWMutex
from gsyncio.server import ConnectionPinningServer

__version__ = "0.1.0"


async def run_in_pool(coro: Any, *args: Any, num_threads: int = 0, **kwargs: Any) -> Any:
    """Run a coroutine in a freshly-created pool (one-shot convenience)."""
    pool = EventLoopThreadPool(num_threads=num_threads)
    await pool.start()
    try:
        fut = pool.submit(coro, *args, **kwargs)
        return await fut
    finally:
        await pool.close()


__all__ = [
    "AsyncContext",
    "AsyncOnce",
    "AsyncRWMutex",
    "AsyncWaitGroup",
    "Barrier",
    "BarrierWaitResult",
    "CancelScope",
    "CapacityLimiter",
    "ChannelClosedError",
    "Condition",
    "ConnectionPinningServer",
    "Event",
    "EventLoopThreadPool",
    "FastChannel",
    "GsyncioASGIWorker",
    "GsyncioError",
    "Lock",
    "PoolOptions",
    "Semaphore",
    "TaskGroup",
    "TaskHandle",
    "TaskStatus",
    "ThreadPoolClosedError",
    "TimeoutError",
    "WouldBlock",
    "__version__",
    "checkpoint",
    "create_pool",
    "current_effective_deadline",
    "fail_after",
    "fail_at",
    "get_logger",
    "move_on_after",
    "move_on_at",
    "run_in_pool",
    "select_channel",
    "set_log_level",
]
