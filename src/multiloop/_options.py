"""Module-level defaults and PoolOptions configuration dataclass for multiloop."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio

__all__ = ["PoolOptions"]

# Module-level defaults
_DEFAULT_NUM_THREADS: int = 0  # 0 → auto-detect (os.cpu_count())
_DEFAULT_LOOP_FACTORY: Callable[[], asyncio.AbstractEventLoop] | None = None


@dataclass
class PoolOptions:
    """Configuration options for an :class:`~multiloop.EventLoopThreadPool`.

    Encapsulates worker count and custom loop factories so callers can compose,
    share, and inspect pool configurations cleanly.

    Examples::

        # Auto-detect CPUs
        pool = EventLoopThreadPool()

        # Explicit options dataclass
        opts = PoolOptions(num_threads=8)
        pool = EventLoopThreadPool(options=opts)

        # Keyword arguments override
        pool = EventLoopThreadPool(num_threads=4)
    """

    num_threads: int = _DEFAULT_NUM_THREADS
    """Number of worker threads (0 = auto-detect via :func:`os.cpu_count`)."""

    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = _DEFAULT_LOOP_FACTORY
    """Callable returning a new event loop instance (``None`` = default asyncio event loop)."""
