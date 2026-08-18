"""multiloop: Multi-event-loop engine & concurrency toolkit for Python 3.14t.

A high-performance, Go-like concurrency library for free-threaded Python,
built around a Rust core. Key capabilities:

- EventLoopThreadPool with round-robin scheduling and work stealing
- Channel and select_channel for Go-style communication
- CancelScope and TaskGroup for structured concurrency
- AsyncContext, AsyncWaitGroup, AsyncOnce, AsyncRWMutex primitives
- ASGI worker (MultiloopASGIWorker) for FastAPI-style servers

Docs: https://github.com/hankaihong1/multiloop (docs/API.md, docs/sphinx_html/).
"""

from typing import Any

from multiloop._cancel import (
    CancelScope,
    checkpoint,
    current_effective_deadline,
    fail_after,
    fail_at,
    move_on_after,
    move_on_at,
)
from multiloop._logging import get_logger, set_log_level
from multiloop._sync import (
    Barrier,
    BarrierWaitResult,
    CapacityLimiter,
    Condition,
    Event,
    Lock,
    Semaphore,
)
from multiloop._taskgroup import TaskGroup, TaskHandle, TaskStatus
from multiloop.asgi import MultiloopASGIWorker
from multiloop.context import AsyncContext
from multiloop.exceptions import (
    ChannelClosedError,
    MultiloopError,
    ThreadPoolClosedError,
    TimeoutError,
    WouldBlock,
)
from multiloop.pool import (
    EventLoopThreadPool,
    PoolOptions,
    create_pool,
)
from multiloop.primitives import (
    AsyncOnce,
    AsyncWaitGroup,
    Channel,
    select_channel,
)
from multiloop.rwlock import AsyncRWMutex
from multiloop.server import ConnectionPinningServer
from multiloop.wsgi import MultiloopWSGIWorker

__version__ = "0.1.0"


async def run_in_pool(coro: Any, *args: Any, num_threads: int = 0, **kwargs: Any) -> Any:
    """Run a coroutine in a freshly-created pool (one-shot convenience).

    :param coro: Coroutine function or coroutine object to run.
    :param num_threads: Worker thread count (0 for auto-detection).
    :param args: Positional arguments for ``coro``.
    :param kwargs: Keyword arguments for ``coro``.
    :returns: Result returned by ``coro``.
    """
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
    "Channel",
    "ChannelClosedError",
    "Condition",
    "ConnectionPinningServer",
    "Event",
    "EventLoopThreadPool",
    "Lock",
    "MultiloopASGIWorker",
    "MultiloopError",
    "MultiloopWSGIWorker",
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
