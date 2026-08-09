"""Asynchronous Read-Write Mutex (AsyncRWMutex)."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from gsyncio._sync import Condition, Lock


class AsyncRWMutex:
    """Read-Write Lock (RWMutex) for asynchronous Python code.

    Allows multiple readers to hold the lock concurrently, but only a single writer.
    Uses cross-thread-safe :class:`gsyncio.Lock` and :class:`gsyncio.Condition` primitives
    with writer-priority fairness to prevent writer starvation.

    """

    def __init__(self) -> None:
        self._readers = 0
        self._writer = False
        self._pending_writers = 0
        self._lock = Lock()
        self._read_ok = Condition(self._lock)
        self._write_ok = Condition(self._lock)

    def __repr__(self) -> str:
        return (
            f"<AsyncRWMutex readers={self._readers} writer={self._writer}"
            f" pending_writers={self._pending_writers}>"
        )

    @asynccontextmanager
    async def reader(self) -> AsyncGenerator[None]:
        """Acquire a shared read lock context manager."""
        async with self._lock:
            # Block while ANY writer (active or pending) exists — writer priority.
            while self._writer or self._pending_writers > 0:
                await self._read_ok.wait()
            self._readers += 1

        try:
            yield
        finally:
            async with self._lock:
                self._readers -= 1
                if self._readers == 0:
                    # Wake writers first (writer priority).
                    self._write_ok.notify_all()

    @asynccontextmanager
    async def writer(self) -> AsyncGenerator[None]:
        """Acquire an exclusive write lock context manager."""
        async with self._lock:
            self._pending_writers += 1
            try:
                while self._writer or self._readers > 0:
                    await self._write_ok.wait()
                self._writer = True
            finally:
                self._pending_writers -= 1

        try:
            yield
        finally:
            async with self._lock:
                self._writer = False
                # If pending writers exist, wake them first (writer priority).
                # Otherwise wake readers.
                if self._pending_writers > 0:
                    self._write_ok.notify_all()
                else:
                    self._read_ok.notify_all()
