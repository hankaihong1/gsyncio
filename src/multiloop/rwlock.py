"""Asynchronous Read-Write Mutex (AsyncRWMutex) with writer-preference fairness."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from multiloop._channel_base import _wake_all
from multiloop._rust import _try_import_rust_class
from multiloop._sync import Condition, Lock

__all__ = ["AsyncRWMutex"]

_RawAsyncRWMutex = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncRWMutex")


class AsyncRWMutex:
    """Read-Write Lock (RWMutex) for asynchronous Python coroutines.

    Allows concurrent shared access for multiple readers while granting exclusive access
    to a single writer. Built upon Rust ``RawAsyncRWMutex`` (or fallback ``Lock``/``Condition``)
    with writer-preference fairness to eliminate writer starvation under heavy read traffic.

    Nesting rules:
    - ``reader()`` is re-entrant for the same task.
    - Upgrades from reader to writer or recursive acquisitions of ``writer()`` raise
      :class:`RuntimeError` immediately to prevent deadlock.
    """

    def __init__(self) -> None:
        self._raw = _RawAsyncRWMutex() if _RawAsyncRWMutex is not None else None
        self._reader_depth: dict[asyncio.Task[Any], int] = {}
        self._writer_task: asyncio.Task[Any] | None = None

        # Fallback fields when Rust extension is missing
        self._fallback_readers = 0
        self._fallback_writer = False
        self._fallback_pending_writers = 0
        self._lock = Lock()
        self._read_ok = Condition(self._lock)
        self._write_ok = Condition(self._lock)

    @property
    def _readers(self) -> int:
        if self._raw is not None:
            return int(self._raw.snapshot()[0])
        return self._fallback_readers

    @property
    def _writer(self) -> bool:
        if self._raw is not None:
            return bool(self._raw.snapshot()[1])
        return self._fallback_writer

    @property
    def _pending_writers(self) -> int:
        if self._raw is not None:
            return int(self._raw.snapshot()[2])
        return self._fallback_pending_writers

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

        if self._writer_task is task:
            raise RuntimeError(
                "AsyncRWMutex: reader() cannot be used while the same task holds writer()"
            )

        if self._raw is not None:
            depth = self._reader_depth.get(task, 0)
            if depth > 0:
                self._reader_depth[task] = depth + 1
            else:
                if not self._raw.try_acquire_read():
                    loop = asyncio.get_running_loop()
                    fut = loop.create_future()
                    if not self._raw.acquire_read_or_register(loop, fut):
                        try:
                            await fut
                        except BaseException:
                            wakers = self._raw.remove_waiter(loop, fut, False)
                            if wakers:
                                _wake_all(wakers)
                            raise
                self._reader_depth[task] = 1

            try:
                yield
            finally:
                depth = self._reader_depth.get(task, 0)
                if depth > 1:
                    self._reader_depth[task] = depth - 1
                else:
                    self._reader_depth.pop(task, None)
                    wakers = self._raw.release_read()
                    if wakers:
                        _wake_all(wakers)
            return

        # Pure Python fallback path
        async with self._lock:
            if self._writer_task is task:
                raise RuntimeError(
                    "AsyncRWMutex: reader() cannot be used while the same task holds writer()"
                )
            if task not in self._reader_depth:
                while self._fallback_writer or self._fallback_pending_writers > 0:
                    await self._read_ok.wait()
                self._fallback_readers += 1
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
                        self._fallback_readers -= 1
                        if self._fallback_readers == 0:
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

        if task in self._reader_depth:
            raise RuntimeError(
                "AsyncRWMutex: writer() cannot be used while the same task holds reader()"
            )
        if self._writer_task is task:
            raise RuntimeError("AsyncRWMutex: writer() is not reentrant")

        if self._raw is not None:
            if not self._raw.try_acquire_write():
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                if not self._raw.acquire_write_or_register(loop, fut):
                    try:
                        await fut
                    except BaseException:
                        wakers = self._raw.remove_waiter(loop, fut, True)
                        if wakers:
                            _wake_all(wakers)
                        raise
            self._writer_task = task

            try:
                yield
            finally:
                self._writer_task = None
                wakers = self._raw.release_write()
                if wakers:
                    _wake_all(wakers)
            return

        # Pure Python fallback path
        async with self._lock:
            if task in self._reader_depth:
                raise RuntimeError(
                    "AsyncRWMutex: writer() cannot be used while the same task holds reader()"
                )
            if self._writer_task is task:
                raise RuntimeError("AsyncRWMutex: writer() is not reentrant")
            self._fallback_pending_writers += 1
            acquired = False
            try:
                while self._fallback_writer or self._fallback_readers > 0:
                    await self._write_ok.wait()
                self._fallback_writer = True
                self._writer_task = task
                acquired = True
            finally:
                self._fallback_pending_writers -= 1
                if self._fallback_pending_writers == 0 and not self._fallback_writer:
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
                    self._fallback_writer = False
                    self._writer_task = None
                    if self._fallback_pending_writers > 0:
                        self._write_ok.notify_all()
                    else:
                        self._read_ok.notify_all()
                finally:
                    self._lock.release()
                if cancelled:
                    raise asyncio.CancelledError()
