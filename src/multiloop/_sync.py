"""Cross-event-loop safe synchronization primitives for multiloop.

Includes Lock, Semaphore, CapacityLimiter, Event, Condition, and Barrier — all engineered
for Python 3.14t multi-core physical parallelism and cross-event-loop thread safety.
"""

from __future__ import annotations

import asyncio
import collections
import threading
from collections.abc import Callable
from typing import Any, Self

__all__ = [
    "Barrier",
    "BarrierWaitResult",
    "CapacityLimiter",
    "Condition",
    "Event",
    "Lock",
    "Semaphore",
]

# ============================================================================
# Lock — fair FIFO mutex
# ============================================================================


class Lock:
    """A fair FIFO mutex that is safe to use across independent event loops and OS threads.

    Acquisition and release are bound to the *owning task*: only the task that acquired
    the lock may release it. Calling :meth:`release` from any other task or non-async context
    raises :class:`RuntimeError`.

    Unlike standard :class:`asyncio.Lock`, `multiloop.Lock` can be acquired by tasks running
    on physically different OS threads and distinct event loops.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._owner: asyncio.Task[Any] | None = None
        self._waiters: collections.OrderedDict[asyncio.Event, asyncio.Task[Any]] = (
            collections.OrderedDict()
        )

    @property
    def locked(self) -> bool:
        """Return True when the lock is currently held."""
        with self._lock:
            return self._owner is not None

    @property
    def owner(self) -> asyncio.Task[Any] | None:
        """Return the owning asyncio task, or None if free."""
        with self._lock:
            return self._owner

    async def acquire(self) -> None:
        """Acquire the lock, blocking in strict FIFO order until it becomes available.

        If the waiting task is cancelled while blocked, its waiter registration is safely
        removed and ownership token forwarding is executed if ownership was already transferred.
        """
        try:
            task = asyncio.current_task()
        except RuntimeError:
            raise RuntimeError("acquire() must be called from an active asyncio task") from None
        if task is None:
            raise RuntimeError("acquire() must be called from an active asyncio task")

        with self._lock:
            if self._owner is task:
                raise RuntimeError("Lock is not reentrant: already held by the current task")
            # If the previous owner died without release, recover gracefully
            if self._owner is not None and self._owner.done():
                self._release_locked()

            if self._owner is None:
                self._owner = task
                return

            event = asyncio.Event()
            self._waiters[event] = task

        try:
            await event.wait()
        except BaseException:
            with self._lock:
                # If release() had already popped our waiter and granted ownership before cancellation,
                # we must forward the ownership to the next pending waiter to prevent deadlock.
                was_waiter = self._discard_waiter(event)
                if self._owner is task and not was_waiter:
                    self._release_locked()
            raise

    def release(self) -> None:
        """Release the lock, passing ownership to the next queued FIFO waiter.

        :raises RuntimeError: If called by a task that does not own the lock.
        """
        try:
            task = asyncio.current_task()
        except RuntimeError:
            raise RuntimeError("release() must be called from an active asyncio task") from None
        if task is None:
            raise RuntimeError("release() must be called from an active asyncio task")

        with self._lock:
            if self._owner is not task:
                raise RuntimeError("Lock.release() called by a task that does not own the lock")
            self._release_locked()

    def _release_locked(self) -> None:
        """Hand ownership to the next live waiter, or free the lock (caller must hold ``_lock``)."""
        while self._waiters:
            event, waiter_task = self._waiters.popitem(last=False)
            if waiter_task.done():
                continue
            self._owner = waiter_task
            waiter_loop = waiter_task.get_loop()
            try:
                waiter_loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                # Closed loop — continue scanning for next live waiter
                continue
            return

        self._owner = None

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool | None:
        self.release()
        return None

    def _discard_waiter(self, event: asyncio.Event) -> bool:
        """Remove waiter matching `event` from `_waiters`. Caller must hold `_lock`."""
        return self._waiters.pop(event, None) is not None


# ============================================================================
# Semaphore — cross-thread-safe async semaphore
# ============================================================================


class Semaphore:
    """A cross-thread-safe async semaphore with fair FIFO waiter queuing.

    Provides fair FIFO scheduling across multiple OS threads and event loops.
    Cancellation is token-conservative: if a waiter is cancelled after a permit has been
    transferred to it by a concurrent `release()`, that permit is automatically forwarded
    to the next waiting task or returned to the value pool.

    :param max_value: Maximum permit capacity. Must be >= 0.
    """

    def __init__(self, max_value: int) -> None:
        if max_value < 0:
            raise ValueError("max_value must be >= 0")
        self._value = max_value
        self._max_value = max_value
        self._lock = threading.Lock()
        self._waiters: collections.OrderedDict[asyncio.Event, asyncio.AbstractEventLoop] = (
            collections.OrderedDict()
        )

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"<Semaphore value={self._value}, "
                f"max_value={self._max_value}, "
                f"waiters={len(self._waiters)}>"
            )

    @property
    def value(self) -> int:
        """Number of currently available permits."""
        with self._lock:
            return self._value

    @property
    def max_value(self) -> int:
        """Maximum permit capacity."""
        with self._lock:
            return self._max_value

    async def acquire(self) -> None:
        """Acquire a permit, blocking in FIFO order if none are currently available."""
        loop = asyncio.get_running_loop()
        event = asyncio.Event()

        with self._lock:
            if self._value > 0:
                self._value -= 1
                return
            self._waiters[event] = loop

        try:
            await event.wait()
        except BaseException:
            self._cancel_waiter(event)
            raise

    def _cancel_waiter(self, event: asyncio.Event) -> None:
        """Handle cancellation of a waiting task, preserving permit conservation."""
        with self._lock:
            was_present = self._waiters.pop(event, None) is not None

            if not was_present:
                # Permit was already popped and transferred to this waiter. Forward to next:
                while self._waiters:
                    next_event, next_loop = self._waiters.popitem(last=False)
                    try:
                        next_loop.call_soon_threadsafe(next_event.set)
                        return
                    except RuntimeError:
                        continue
                self._value += 1

    def release(self) -> None:
        """Release a permit, waking the first FIFO waiter or restoring available count.

        :raises ValueError: If released more times than the initialized max_value without pending waiters.
        """
        with self._lock:
            if self._waiters:
                while self._waiters:
                    event, loop = self._waiters.popitem(last=False)
                    try:
                        loop.call_soon_threadsafe(event.set)
                        return
                    except RuntimeError:
                        continue
                self._value += 1
                return
            if self._value >= self._max_value:
                raise ValueError("Semaphore released too many times")
            self._value += 1

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> bool | None:
        self.release()
        return None


# ============================================================================
# CapacityLimiter — dynamic token limiter with single-lock atomic state
# ============================================================================


class CapacityLimiter:
    """An async capacity limiter safe across event loops and OS threads under free-threading.

    Maintains a dynamically resizable token budget (`total_tokens`). All token allocations,
    borrowing counts, and waiter queues are protected by a single `threading.Lock`,
    guaranteeing atomic updates under Python 3.14t multi-core parallelism.

    :param total_tokens: Initial total capacity token budget. Must be > 0.
    """

    def __init__(self, total_tokens: float) -> None:
        if total_tokens <= 0:
            raise ValueError("total_tokens must be positive")
        self._total_tokens = float(total_tokens)
        self._borrowed = 0
        self._lock = threading.Lock()
        self._waiters: collections.OrderedDict[asyncio.Event, asyncio.AbstractEventLoop] = (
            collections.OrderedDict()
        )

    def __repr__(self) -> str:
        return (
            f"<CapacityLimiter total_tokens={self._total_tokens}, "
            f"available={self.available_tokens}, "
            f"borrowed={self.borrowed_tokens}>"
        )

    @property
    def total_capacity(self) -> int:
        """Total integer token capacity."""
        with self._lock:
            return int(self._total_tokens)

    @property
    def available_capacity(self) -> int:
        """Currently available discrete integer token capacity."""
        with self._lock:
            return max(0, int(self._total_tokens) - self._borrowed)

    @property
    def total_tokens(self) -> float:
        """Total token capacity budget."""
        with self._lock:
            return self._total_tokens

    @total_tokens.setter
    def total_tokens(self, value: float) -> None:
        if value <= 0:
            raise ValueError("total_tokens must be positive")
        with self._lock:
            self._total_tokens = float(value)
            capacity = int(self._total_tokens)
            while self._waiters and self._borrowed < capacity:
                event, loop = self._waiters.popitem(last=False)
                try:
                    loop.call_soon_threadsafe(event.set)
                    self._borrowed += 1
                except RuntimeError:
                    continue

    @property
    def available_tokens(self) -> float:
        """Currently available tokens."""
        with self._lock:
            return self._total_tokens - self._borrowed

    @property
    def borrowed_tokens(self) -> float:
        """Currently borrowed tokens."""
        with self._lock:
            return float(self._borrowed)

    def snapshot(self) -> tuple[float, float, float]:
        """Return ``(total_tokens, available_tokens, borrowed_tokens)`` atomically."""
        with self._lock:
            avail = self._total_tokens - self._borrowed
            return (self._total_tokens, avail, float(self._borrowed))

    async def acquire(self) -> None:
        """Acquire one token, blocking if current borrowed count has reached capacity."""
        loop = asyncio.get_running_loop()
        event = asyncio.Event()

        with self._lock:
            if self._borrowed < int(self._total_tokens):
                self._borrowed += 1
                return
            self._waiters[event] = loop

        try:
            await event.wait()
        except BaseException:
            self._cancel_waiter(event)
            raise

    def _cancel_waiter(self, event: asyncio.Event) -> None:
        """Handle cancellation while waiting for a token."""
        with self._lock:
            was_present = self._waiters.pop(event, None) is not None

            if not was_present:
                capacity = int(self._total_tokens)
                while self._waiters and self._borrowed <= capacity:
                    next_event, next_loop = self._waiters.popitem(last=False)
                    try:
                        next_loop.call_soon_threadsafe(next_event.set)
                        return
                    except RuntimeError:
                        continue
                self._borrowed -= 1

    def release(self) -> None:
        """Release one borrowed token.

        :raises ValueError: If released more times than acquired.
        """
        with self._lock:
            if self._borrowed <= 0:
                raise ValueError("CapacityLimiter released too many times")
            self._borrowed -= 1
            capacity = int(self._total_tokens)
            if self._borrowed < capacity:
                while self._waiters:
                    event, loop = self._waiters.popitem(last=False)
                    try:
                        loop.call_soon_threadsafe(event.set)
                        self._borrowed += 1
                        return
                    except RuntimeError:
                        continue

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> bool | None:
        self.release()
        return None


# ============================================================================
# Event — cross-thread-safe event (trio semantics, no clear)
# ============================================================================


class Event:
    """A cross-event-loop safe one-shot event with trio-style semantics (no clear).

    Once set via :meth:`set`, the event remains set permanently. Subsequent calls to
    :meth:`wait` return immediately.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._flag = False
        self._waiters: collections.OrderedDict[asyncio.Event, asyncio.AbstractEventLoop] = (
            collections.OrderedDict()
        )

    def is_set(self) -> bool:
        """Return True if the event has been set."""
        with self._lock:
            return self._flag

    def set(self) -> None:
        """Set the event and wake all pending waiters across any event loops."""
        with self._lock:
            self._flag = True
            waiters = list(self._waiters.items())
            self._waiters.clear()

        for event, loop in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                pass

    async def wait(self) -> None:
        """Wait until the event is set. Returns immediately if already set."""
        loop = asyncio.get_running_loop()

        with self._lock:
            if self._flag:
                return
            event = asyncio.Event()
            self._waiters[event] = loop
        try:
            await event.wait()
        except BaseException:
            with self._lock:
                self._waiters.pop(event, None)
            raise


# ============================================================================
# Condition — async condition variable atop multiloop.Lock
# ============================================================================


class Condition:
    """An async condition variable backed by a cross-thread-safe :class:`Lock`.

    Waiters release the underlying lock while suspended and re-acquire it before :meth:`wait`
    returns. The notification protocol does NOT require holding the lock, preventing notifier-sleeper
    deadlocks and thundering herd contention.

    :param lock: Optional underlying :class:`Lock` instance. If omitted, creates a new Lock.
    """

    def __init__(self, lock: Lock | None = None) -> None:
        self._lock = lock if lock is not None else Lock()
        self._waiters_lock = threading.Lock()
        self._waiters: collections.OrderedDict[asyncio.Event, asyncio.AbstractEventLoop] = (
            collections.OrderedDict()
        )

    # -- context-manager support (delegates to the underlying Lock) ---------

    async def acquire(self) -> None:
        """Acquire the underlying lock."""
        await self._lock.acquire()

    def release(self) -> None:
        """Release the underlying lock."""
        self._lock.release()

    async def __aenter__(self) -> Self:
        await self._lock.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        self._lock.release()
        return None

    # -- core protocol ------------------------------------------------------

    async def wait(self) -> None:
        """Wait until notified.

        The **underlying lock must be held** when calling this method. It is released
        during wait and re-acquired before returning. If the task is cancelled, the lock
        is re-acquired under a cancellation shield before propagating the cancellation.
        """
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        if task is None or not self._lock.locked or self._lock.owner is not task:
            raise RuntimeError("cannot wait on un-acquired lock")

        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        with self._waiters_lock:
            self._waiters[event] = loop
        try:
            self._lock.release()
        except BaseException:
            self._discard_waiter(event)
            raise

        try:
            await event.wait()
        except BaseException:
            was_present = self._discard_waiter(event)
            if not was_present:
                self._forward_notify()
            await self._reacquire_lock()
            raise
        else:
            try:
                await self._reacquire_lock()
            except BaseException:
                self._forward_notify()
                raise

    async def wait_for(self, predicate: Callable[[], Any]) -> Any:
        """Wait until a predicate returns a truthy value.

        The **underlying lock must be held** when calling this method.

        :param predicate: Synchronous or asynchronous callable returning a boolean or truthy value.
        :returns: The truthy value returned by ``predicate``.
        """
        while True:
            result = predicate()
            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                result = await result
            if result:
                return result
            await self.wait()

    def notify(self, n: int = 1) -> None:
        """Wake up to `n` waiters (default: 1). Safe to call without holding the lock.

        :param n: Maximum number of waiting tasks to wake.
        """
        if n <= 0:
            return
        for waiter_loop, event in self._pop_waiters(n):
            try:
                waiter_loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                pass

    def notify_all(self) -> None:
        """Wake up all currently waiting tasks."""
        with self._waiters_lock:
            n = len(self._waiters)
        self.notify(n)

    # -- internals ----------------------------------------------------------

    def _pop_waiters(self, n: int) -> list[tuple[asyncio.AbstractEventLoop, asyncio.Event]]:
        with self._waiters_lock:
            count = min(n, len(self._waiters))
            popped: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
            for _ in range(count):
                event, loop = self._waiters.popitem(last=False)
                popped.append((loop, event))
            return popped

    def _discard_waiter(self, event: asyncio.Event) -> bool:
        with self._waiters_lock:
            return self._waiters.pop(event, None) is not None

    def _forward_notify(self) -> None:
        with self._waiters_lock:
            while self._waiters:
                next_event, next_loop = self._waiters.popitem(last=False)
                try:
                    next_loop.call_soon_threadsafe(next_event.set)
                    return
                except RuntimeError:
                    continue

    async def _reacquire_lock(self) -> None:
        cancelled = False
        while True:
            try:
                await self._lock.acquire()
                break
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError()


# ============================================================================
# Barrier — synchronize N parties
# ============================================================================


class BarrierWaitResult:
    """Result object returned by :meth:`Barrier.wait`.

    :param parties: Total number of parties in this barrier round.
    :param index: Zero-based arrival index (0 to parties - 1) of the calling task.
    """

    __slots__ = ("index", "parties")

    def __init__(self, parties: int, index: int = 0) -> None:
        self.parties = parties
        self.index = index

    @property
    def is_leader(self) -> bool:
        """Return True if this task was selected as leader (arrival index 0)."""
        return self.index == 0

    def __repr__(self) -> str:
        return f"<BarrierWaitResult parties={self.parties} index={self.index}>"


class Barrier:
    """An async barrier that synchronizes *N* parties across event loops.

    Each :meth:`wait` call blocks until *parties* tasks have arrived, at which point all
    are released simultaneously and receive a :class:`BarrierWaitResult`. Automatically resets
    for the next generation upon round completion.

    :param parties: Number of parties required to pass the barrier. Must be >= 1.
    """

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be >= 1")
        self._parties = parties
        self._mutex = threading.Lock()
        self._generation = 0
        self._waiters: collections.OrderedDict[
            asyncio.Event, tuple[asyncio.AbstractEventLoop, list[int | None]]
        ] = collections.OrderedDict()
        self._aborted = False
        self._broken = False

    @property
    def parties(self) -> int:
        """Total number of parties required to pass the barrier."""
        return self._parties

    @property
    def broken(self) -> bool:
        """Return True if the barrier is in a broken or aborted state."""
        with self._mutex:
            return self._broken or self._aborted

    @property
    def n_waiting(self) -> int:
        """Number of tasks currently waiting at the barrier."""
        with self._mutex:
            return len(self._waiters)

    async def wait(self) -> BarrierWaitResult:
        """Wait at the barrier until *parties* tasks have arrived.

        :raises RuntimeError: If the barrier is broken, aborted, or reset.
        """
        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        box: list[int | None] = [None]

        with self._mutex:
            if self._aborted:
                raise RuntimeError("barrier has been aborted")
            if self._broken:
                raise RuntimeError("barrier is broken")
            n = len(self._waiters) + 1
            if n == self._parties:
                for idx, (e, (l, b)) in enumerate(self._waiters.items(), start=1):
                    b[0] = idx
                    try:
                        l.call_soon_threadsafe(e.set)
                    except RuntimeError:
                        pass
                self._waiters.clear()
                self._generation += 1
                return BarrierWaitResult(parties=self._parties, index=0)
            self._waiters[event] = (loop, box)
            generation = self._generation

        try:
            await event.wait()
        except BaseException:
            with self._mutex:
                if generation == self._generation:
                    self._broken = True
                    broken_waiters = list(self._waiters.items())
                    self._waiters.clear()
                    self._generation += 1
                    for e, (l, b) in broken_waiters:
                        b[0] = -1
                        try:
                            l.call_soon_threadsafe(e.set)
                        except RuntimeError:
                            pass
            raise

        with self._mutex:
            if self._aborted and generation == self._generation:
                raise RuntimeError("barrier has been aborted")
            if box[0] == -1:
                raise RuntimeError("barrier broken by concurrent cancellation")
            if box[0] is None:
                raise RuntimeError("barrier was reset while waiting")
            return BarrierWaitResult(parties=self._parties, index=box[0])

    def abort(self) -> None:
        """Abort the barrier, waking all waiting tasks with a :exc:`RuntimeError`."""
        with self._mutex:
            self._aborted = True
            self._broken = True
            waiters = list(self._waiters.items())
            self._waiters.clear()

        for event, (loop, _box) in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                pass

    def reset(self) -> None:
        """Reset the barrier to its initial un-broken state, waking existing waiters with an error."""
        with self._mutex:
            waiters = list(self._waiters.items())
            self._waiters.clear()
            self._generation += 1
            self._aborted = False
            self._broken = False

        for event, (loop, _box) in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                pass
