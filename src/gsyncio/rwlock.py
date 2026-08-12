"""Asynchronous Read-Write Mutex (AsyncRWMutex)."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from gsyncio._sync import Condition, Lock


class AsyncRWMutex:
    """Read-Write Lock (RWMutex) for asynchronous Python code.

    Allows multiple readers to hold the lock concurrently, but only a single writer.
    Uses cross-thread-safe :class:`gsyncio.Lock` and :class:`gsyncio.Condition` primitives
    with writer-priority fairness to prevent writer starvation.

    Nesting rules (asyncio-style, enforced up front instead of deadlocking):
    ``reader()`` is re-entrant for the same task (shared lock), but every
    writer-nesting combination raises :class:`RuntimeError` — including a
    ``writer()`` while the same task still holds a ``reader()`` (tracked by
    depth so the check survives nested reader exits).

    """

    def __init__(self) -> None:
        self._readers = 0
        # WHY: a dict[Task, int] depth counter, not a set — a reader that
        # re-enters n times must stay registered until its *outermost* exit.
        # A set drops the registration on the first exit and would let a
        # writer() slip in while the task is still inside an inner reader
        # (R2 FIX-10 revision A).
        self._reader_depth: dict[asyncio.Task[Any], int] = {}
        self._writer = False
        self._writer_task: asyncio.Task[Any] | None = None
        self._pending_writers = 0
        self._lock = Lock()
        self._read_ok = Condition(self._lock)
        self._write_ok = Condition(self._lock)

    def __repr__(self) -> str:
        # WHY: diagnostic only — the counters are read WITHOUT the state
        # lock (it is an async Lock that cannot be taken in repr), so the
        # snapshot may be stale under concurrency on free-threaded builds.
        # Never use these values for check-then-act decisions (R5 FIX-J).
        return (
            f"<AsyncRWMutex readers={self._readers} writer={self._writer}"
            f" pending_writers={self._pending_writers}>"
        )

    @asynccontextmanager
    async def reader(self) -> AsyncGenerator[None]:
        """Acquire a shared read lock context manager."""
        task = asyncio.current_task()
        if task is None:  # pragma: no cover — defensive, asynccontextmanager runs in-task
            raise RuntimeError("reader() must be used inside an asyncio task")

        async with self._lock:
            # A task that already holds the write lock must not read (would
            # bypass writer exclusivity).  Re-entrant reads by a current
            # reader are fine — and must skip the wait loop: a re-entering
            # reader already owns its slot, so blocking it on a queued writer
            # would self-deadlock.
            if self._writer_task is task:
                raise RuntimeError(
                    "AsyncRWMutex: reader() cannot be used while the same task holds writer()"
                )
            if task not in self._reader_depth:
                # Block while ANY writer (active or pending) exists — writer priority.
                while self._writer or self._pending_writers > 0:
                    await self._read_ok.wait()
                self._readers += 1
            self._reader_depth[task] = self._reader_depth.get(task, 0) + 1

        try:
            yield
        finally:
            # WHY: the release path re-acquires the inner lock, and a
            # cancellation delivered on that re-acquire must not abort the
            # cleanup — a leaked reader slot hangs every queued writer
            # forever (R1 probe A).  Retry-loop (asyncio.Condition.wait
            # pattern): each swallowed CancelledError leaves the inner Lock
            # clean (its cancel path discards the waiter entry), so the
            # bookkeeping always runs; the cancellation is re-raised after
            # it completes.
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
                            # Wake writers first (writer priority).
                            self._write_ok.notify_all()
                    else:
                        self._reader_depth[task] = depth - 1
            finally:
                self._lock.release()
            if cancelled:
                raise asyncio.CancelledError()

    @asynccontextmanager
    async def writer(self) -> AsyncGenerator[None]:
        """Acquire an exclusive write lock context manager."""
        task = asyncio.current_task()
        if task is None:  # pragma: no cover
            raise RuntimeError("writer() must be used inside an asyncio task")

        async with self._lock:
            # Enforce all nesting rules up front — waiting would deadlock:
            # a task that holds a reader (or the writer) would wait forever
            # for the very lock it already owns (R2 FIX-10).
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
                # WHY: a cancelled writer just changed the readers' wait
                # condition (pending_writers dropped) — wake them, otherwise
                # blocked readers wait forever on a condition that is now
                # false (BUG-10).
                if self._pending_writers == 0 and not self._writer:
                    self._read_ok.notify_all()

        try:
            yield
        finally:
            # WHY: only a task that actually acquired the writer may clear
            # the holder state.  Cancellation during the acquire phase throws
            # before this try block is entered (so the guard is belt-and-
            # suspenders), but it keeps the invariant local and explicit: a
            # queued writer must never flip _writer while the real holder is
            # still inside its critical section (U3 contract test).  Like
            # reader(), the release re-acquires with the retry-loop pattern
            # so a pending cancel cannot interrupt the bookkeeping.
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
                    # If pending writers exist, wake them first (writer priority).
                    # Otherwise wake readers.
                    if self._pending_writers > 0:
                        self._write_ok.notify_all()
                    else:
                        self._read_ok.notify_all()
                finally:
                    self._lock.release()
                if cancelled:
                    raise asyncio.CancelledError()
