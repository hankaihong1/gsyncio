"""Custom exception hierarchy for gsyncio."""

import asyncio


class GsyncioError(Exception):
    """Base exception class for all gsyncio errors."""


class WouldBlock(GsyncioError):  # noqa: N818
    """Raised when a non-blocking channel operation would block."""


class ChannelClosedError(GsyncioError):
    """Raised when operating on a closed channel."""


class ThreadPoolClosedError(GsyncioError, RuntimeError):
    """Raised when submitting tasks to a closed thread pool."""


class TimeoutError(GsyncioError, asyncio.TimeoutError):
    """Raised when a gsyncio concurrency operation times out."""
