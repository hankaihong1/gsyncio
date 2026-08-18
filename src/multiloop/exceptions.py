"""Exception hierarchy for multiloop.

All public exceptions in multiloop inherit from :class:`MultiloopError`, allowing
callers to easily catch library-specific errors or target specific failure modes.
"""

from __future__ import annotations

import asyncio

__all__ = [
    "ChannelClosedError",
    "MultiloopError",
    "ThreadPoolClosedError",
    "TimeoutError",
    "WouldBlock",
]


class MultiloopError(Exception):
    """Base exception class for all multiloop errors."""


class WouldBlock(MultiloopError):  # noqa: N818
    """Raised when a non-blocking channel or lock operation cannot proceed immediately."""


class ChannelClosedError(MultiloopError):
    """Raised when attempting to send or receive on a closed channel."""


class ThreadPoolClosedError(MultiloopError, RuntimeError):
    """Raised when submitting tasks to a closed or aborted EventLoopThreadPool."""


class TimeoutError(MultiloopError, asyncio.TimeoutError):
    """Raised when a multiloop concurrency operation times out."""
