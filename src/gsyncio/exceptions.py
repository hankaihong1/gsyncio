"""Custom exception hierarchy for gsyncio."""

import asyncio


class GsyncioError(Exception):
    """Base exception class for all gsyncio errors."""

    def __init__(
        self, message: str = "", *, code: int | None = None, reason: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.reason = reason


class WouldBlock(GsyncioError):  # noqa: N818
    """Raised when a non-blocking channel operation would block."""


class ChannelClosedError(GsyncioError):
    """Raised when operating on a closed channel."""


class ThreadPoolClosedError(GsyncioError, RuntimeError):
    """Raised when submitting tasks to a closed thread pool."""


class TimeoutError(GsyncioError, asyncio.TimeoutError):
    """Raised when a gsyncio concurrency operation times out."""
