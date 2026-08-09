"""Module-level defaults and PoolOptions dataclass for gsyncio.

Inspired by asyncssh's SSHClientConnectionOptions pattern —
collect thread-pool configuration into a single dataclass so
callers can compose, share, and override settings easily.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio

# Module-level defaults (asyncssh style)
_DEFAULT_NUM_THREADS: int = 0  # 0 → auto-detect (os.cpu_count())
_DEFAULT_LOOP_FACTORY: Callable[[], asyncio.AbstractEventLoop] | None = None


@dataclass
class PoolOptions:
    """Configuration for an :class:`~gsyncio.EventLoopThreadPool`.

    Every field defaults to a well-known module-level constant so
    callers can import and customise the defaults directly.

    Examples::

        # Full auto — use defaults
        pool = EventLoopThreadPool()

        # Explicit options via dataclass
        opts = PoolOptions(num_threads=8)
        pool = EventLoopThreadPool(options=opts)

        # Override single fields via constructor kwargs
        pool = EventLoopThreadPool(num_threads=4)
    """

    num_threads: int = _DEFAULT_NUM_THREADS
    """Number of worker threads (0 = auto-detect via :func:`os.cpu_count`)."""

    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = _DEFAULT_LOOP_FACTORY
    """Callable returning a new event loop (``None`` = best available)."""
