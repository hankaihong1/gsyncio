"""Asynchronous Read-Write Mutex (AsyncRWMutex) with writer-preference fairness."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from multiloop._sync import Condition, Lock

__all__ = ["AsyncRWMutex"]


class AsyncRWMutex:
    """Read-Write Lock (RWMutex) for asynchronous Python coroutines.

    Allows concurrent shared access for multiple readers while granting exclusive access
    to a single writer. Built upon :class:`multiloop.Lock` and :class:`multiloop.Condition`
    with writer-preference fairness to eliminate writer starvation under heavy read traffic.

    Nesting rules:
    - ``reader()`` is re-entrant for the same task.
    - Upgrades from reader to writer or recursive acquisitions of ``writer()`` raise
      :class:`RuntimeError` immediately to prevent deadlock.
    """

    def __init__(self) -> None:
        self._readers = 0
        self._reader_depth: dict[asyncio.Task[Any], int] = {}
        self._writer = False
        self._writer_task: asyncio.Task[Any] | None = None
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
        """Acquire a shared read lock context manager.

        :raises RuntimeError: If called by a task that already holds an active writer lock.
        """
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("reader() must be used inside an active asyncio task")

        async with self._lock:
            if self._writer_task is task:
                raise RuntimeError(
                    "AsyncRWMutex: reader() cannot be used while the same task holds writer()"
                )
            if task not in self._reader_depth:
                # Writer-preference: suspend if an active writer or pending writer exists
                while self._writer or self._pending_writers > 0:
                    await self._read_ok.wait()
                self._readers += 1
            self._reader_depth[task] = self._reader_depth.get(task, 0) + 1

        try:
            yield
        finally:
            cancelled = False
            while True:
                try:
                    await self._lock.acquire()
                    break
                except asyncio.CancelledError:
                    cancelled = True
            try:
                depth = self._reader_depth.get(task, 0)
                if depth > 0:
                    if depth == 1:
                        del self._reader_depth[task]
                        self._readers -= 1
                        if self._readers == 0:
                            self._write_ok.notify_all()
                    else:
                        self._reader_depth[task] = depth - 1
            finally:
                self._lock.release()
            if cancelled:
                raise asyncio.CancelledError()

    @asynccontextmanager
    async def writer(self) -> AsyncGenerator[None]:
        """Acquire an exclusive write lock context manager.

        :raises RuntimeError: If called by a task holding a reader lock or another writer lock.
        """
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("writer() must be used inside an active asyncio task")

        async with self._lock:
            if task in self._reader_depth:
                raise RuntimeError(
                    "AsyncRWMutex: writer() cannot be used while the same task holds reader()"
                )
            if self._writer_task is task:
                raise RuntimeError("AsyncRWMutex: writer() is not reentrant")
            self._pending_writers += 1
            acquired = False
            try:
                while self._writer or self._readers > 0:
                    await self._write_ok.wait()
                self._writer = True
                self._writer_task = task
                acquired = True
            finally:
                self._pending_writers -= 1
                if self._pending_writers == 0 and not self._writer:
                    self._read_ok.notify_all()

        try:
            yield
        finally:
            if acquired:
                cancelled = False
                while True:
                    try:
                        await self._lock.acquire()
                        break
                    except asyncio.CancelledError:
                        cancelled = True
                try:
                    self._writer = False
                    self._writer_task = None
                    if self._pending_writers > 0:
                        self._write_ok.notify_all()
                    else:
                        self._read_ok.notify_all()
                finally:
                    self._lock.release()
                if cancelled:
                    raise asyncio.CancelledError()
