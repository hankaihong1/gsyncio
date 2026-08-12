"""Cross-event-loop-safe synchronization primitives.

Lock, Event, Semaphore, and CapacityLimiter — all safe for use across
multiple event-loop threads and external OS threads.
"""

from __future__ import annotations

import asyncio
import collections
import threading
from typing import Any, Self

# ============================================================================
# Lock — fair FIFO mutex
# ============================================================================


class Lock:
    """A fair FIFO mutex that is safe to use across event loops and OS threads.

    The lock itself is thread-safe, but release is bound to the *owning
    task*: the task that acquired the lock must release it (calling
    :meth:`release` from any other task — or from a thread with no running
    asyncio task — raises :class:`RuntimeError`).  Unlike
    :class:`asyncio.Lock`, the same lock may be acquired by tasks living on
    different event loops, so a hand-off between loops is possible by
    design — the owner task is the only authority over release (FIX-5).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._owner: asyncio.Task[Any] | None = None
        self._waiters: collections.deque[tuple[asyncio.Task[Any], asyncio.Event]] = (
            collections.deque()
        )

    @property
    def locked(self) -> bool:
        """Return ``True`` when the lock is currently held."""
        with self._lock:
            return self._owner is not None

    @property
    def owner(self) -> asyncio.Task[Any] | None:
        """Return the owning task, or ``None`` if the lock is free."""
        with self._lock:
            return self._owner

    async def acquire(self) -> None:
        """Acquire the lock, blocking until it becomes available.

        Waiters are served in strict FIFO order.  If the current task
        is cancelled while waiting, its waiter entry is removed from
        the queue and a :class:`asyncio.CancelledError` is propagated.
        """
        # WHY: on 3.14 current_task() RAISES RuntimeError when no loop is
        # running instead of returning None — the try/except converts the
        # bare "no running event loop" into the documented message (R5
        # FIX-J).
        try:
            task = asyncio.current_task()
        except RuntimeError:
            raise RuntimeError("acquire() must be called from an asyncio task") from None
        if task is None:  # defensive narrowing for older interpreters
            raise RuntimeError("acquire() must be called from an asyncio task")

        with self._lock:
            # WHY: asyncio.Lock rejects same-task re-acquisition up front;
            # silently queueing would self-deadlock, and a cancelled
            # re-entrant acquire would hit the ambiguous _owner-is-task check
            # in the cancel path below, handing the lock to a waiter while
            # the still-running outer holder believes it owns it (R2 FIX-9).
            if self._owner is task:
                raise RuntimeError("Lock is not reentrant: already held by the current task")
            # WHY: The owner task can die without releasing (CancelledError racing
            # release). Without this break every future acquirer waits forever on
            # a lock whose owner will never call release(); the check recycles the
            # lock to the next caller instead of leaking it.
            if self._owner is not None and self._owner.done():
                self._owner = None

            if self._owner is None:
                self._owner = task
                return

            event = asyncio.Event()
            self._waiters.append((task, event))

        try:
            await event.wait()
        except asyncio.CancelledError:
            with self._lock:
                # WHY: release() may already have popped this waiter and handed
                # it ownership (BUG-8).  The lock would then belong to a dead
                # task and every later FIFO waiter starves.  Forward it to the
                # next live waiter — the same token-forwarding pattern as
                # Semaphore._cancel_waiter; a cancelled successor forwards
                # again via its own cancel handler (chain forwarding).
                # was_waiter discriminates: only an entry that release() had
                # already popped (ownership handed to us) must be forwarded —
                # a task still queued as a waiter is a re-entrant acquire whose
                # outer holder still legitimately owns the lock (R2 FIX-9).
                was_waiter = self._discard_waiter(task)
                if self._owner is task and not was_waiter:
                    self._release_locked()
            raise

    def release(self) -> None:
        """Release the lock, handing ownership to the next waiter (if any).

        :raises RuntimeError: if called by a task that does not own the lock.
        """
        # WHY: same 3.14 current_task() semantics as acquire() (R5 FIX-J).
        try:
            task = asyncio.current_task()
        except RuntimeError:
            raise RuntimeError("release() must be called from an asyncio task") from None
        if task is None:  # defensive narrowing for older interpreters
            raise RuntimeError("release() must be called from an asyncio task")

        with self._lock:
            if self._owner is not task:
                raise RuntimeError("Lock.release() called by a task that does not own the lock")
            self._release_locked()

    def _release_locked(self) -> None:
        """Hand ownership to the next live waiter, or free the lock.

        Caller must hold ``_lock``.
        """
        while self._waiters:
            waiter_task, event = self._waiters.popleft()
            if waiter_task.done():
                continue
            self._owner = waiter_task
            waiter_loop = waiter_task.get_loop()
            try:
                waiter_loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                # WHY: the waiter's loop was closed (abandoned loop) — it can
                # never take ownership.  Keep scanning for a live waiter
                # instead of leaving the lock owned by a dead task, which
                # would break every later acquire (R5 FIX-E).
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

    def _discard_waiter(self, task: asyncio.Task[Any]) -> bool:
        """Remove *task* from ``_waiters`` (caller must hold ``_lock``).

        Returns ``True`` if the entry was still queued, ``False`` if it had
        already been popped (e.g. by ``_release_locked``) — the discrimination
        the cancel path needs to decide whether ownership must be forwarded.
        """
        remaining: collections.deque[tuple[asyncio.Task[Any], asyncio.Event]] = collections.deque(
            w for w in self._waiters if w[0] is not task
        )
        was_present = len(remaining) != len(self._waiters)
        self._waiters.clear()
        self._waiters.extend(remaining)
        return was_present


# ============================================================================
# Semaphore — cross-thread-safe async semaphore
# ============================================================================


class Semaphore:
    """A cross-thread-safe async semaphore with fair FIFO waiters.

    Waiters are queued in a :class:`~collections.deque`.  ``acquire()``
    implements fair FIFO ordering by pushing new waiters to the right and
    waking from the left.  Cancelling a waiting ``acquire()`` is safe: the
    cancelled waiter is removed from the queue and, if a token had already
    been transferred by a concurrent ``release()``, the token is passed to
    the next waiter or returned to the pool.

    All internal state mutations are protected by a :class:`threading.Lock`,
    making the semaphore safe for use across event-loop threads.
    """

    def __init__(self, max_value: int) -> None:
        # WHY: asyncio.Semaphore(0) is legal — a gate that starts closed and
        # is opened by release().  Rejecting it broke CapacityLimiter with
        # fractional totals in (0,1) (R2 FIX-11).
        if max_value < 0:
            raise ValueError("max_value must be >= 0")
        self._value = max_value
        self._max_value = max_value
        self._lock = threading.Lock()
        self._waiters: collections.deque[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = (
            collections.deque()
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
        """Number of available permits."""
        with self._lock:
            return self._value

    @property
    def max_value(self) -> int:
        """Maximum number of permits."""
        # WHY: CapacityLimiter's total_tokens setter rewrites _max_value from
        # another thread — the read must be under the same lock or the
        # free-threaded build races (U2 re-audit).
        with self._lock:
            return self._max_value

    async def acquire(self) -> None:
        """Acquire a permit, blocking in FIFO order if none are available.

        Cancellation-safe: if cancelled while waiting, the waiter is removed
        from the queue and any token already transferred is passed on.
        """
        loop = asyncio.get_running_loop()
        event = asyncio.Event()

        with self._lock:
            if self._value > 0:
                self._value -= 1
                return
            self._waiters.append((loop, event))

        try:
            await event.wait()
        except asyncio.CancelledError:
            self._cancel_waiter(event)
            raise

    def _cancel_waiter(self, event: asyncio.Event) -> None:
        """Remove *event* from the waiter deque if still present.

        If *event* was already popped by ``release()`` the token that was
        transferred to the cancelled waiter is forwarded to the next waiter
        or returned to the value pool.
        """
        with self._lock:
            remaining = collections.deque(w for w in self._waiters if w[1] is not event)
            was_present = len(remaining) != len(self._waiters)
            self._waiters = remaining

            if not was_present:
                # WHY: release() may have already popped this waiter and handed it
                # a permit. Dropping the entry would silently destroy that permit
                # and shrink the pool forever. Forwarding it to the next FIFO
                # waiter, or returning it to the value pool, keeps the count exact.
                while self._waiters:
                    next_loop, next_event = self._waiters.popleft()
                    try:
                        next_loop.call_soon_threadsafe(next_event.set)
                        return
                    except RuntimeError:
                        # WHY: dead loop — this waiter can never take the
                        # token; keep forwarding to the next live one, and if
                        # none remain the token returns to the pool below
                        # (R5 FIX-E).
                        continue
                self._value += 1

    def release(self) -> None:
        """Release a permit, waking the first FIFO waiter if any.

        :raises ValueError: if the semaphore already holds ``max_value``
            permits and no waiter is parked (asyncio.Semaphore parity —
            a silent over-release would corrupt the count forever, R1 FIX-3).
        """
        with self._lock:
            if self._waiters:
                while self._waiters:
                    loop, event = self._waiters.popleft()
                    try:
                        loop.call_soon_threadsafe(event.set)
                        return
                    except RuntimeError:
                        # WHY: dead loop — drop this waiter and keep looking;
                        # if none remain, the token returns to the pool (R5
                        # FIX-E).
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
# CapacityLimiter — dynamic token limiter backed by Semaphore
# ============================================================================


class CapacityLimiter:
    """An async capacity limiter backed by a :class:`Semaphore`.

    The limiter maintains a total token budget (``total_tokens``) that can
    be dynamically resized.  Available and borrowed tokens are computed from
    the underlying semaphore value.  ``total_tokens`` may be a fractional
    float; the underlying semaphore capacity is ``int(total_tokens)``.
    """

    def __init__(self, total_tokens: float) -> None:
        if total_tokens <= 0:
            raise ValueError("total_tokens must be positive")
        self._semaphore = Semaphore(int(total_tokens))
        self._total_tokens = total_tokens
        # WHY: borrowed is tracked explicitly instead of being derived from the
        # semaphore value — shrinking the total below the borrowed count used
        # to make the derived numbers fictional (probe R1-B: claimed borrowed=1
        # while 3 were held).  avail + borrowed == total now holds by
        # construction (R1 FIX-3).
        self._borrowed = 0
        self._total_lock = threading.Lock()

    def __repr__(self) -> str:
        return (
            f"<CapacityLimiter total_tokens={self._total_tokens}, "
            f"available={self.available_tokens}, "
            f"borrowed={self.borrowed_tokens}>"
        )

    @property
    def total_tokens(self) -> float:
        """Total token capacity."""
        with self._total_lock:
            return self._total_tokens

    @total_tokens.setter
    def total_tokens(self, value: float) -> None:
        if value <= 0:
            raise ValueError("total_tokens must be positive")
        with self._total_lock:
            old = self._total_tokens
            self._total_tokens = value
            new_int = int(value)
            old_int = int(old)
            # WHY: max write + token reclaim/grow are ONE atomic region
            # under the semaphore lock — release() checks value against max
            # under that same lock, so it can never observe an intermediate
            # resize state (max lowered, tokens not yet reclaimed) and raise
            # a false over-release ValueError (R5 FIX-I / revision D).
            with self._semaphore._lock:
                self._semaphore._max_value = new_int
                diff = new_int - old_int
                if diff > 0:
                    # WHY: only tokens NOT currently borrowed may become
                    # available — with 3 borrowed, growing 1 → 5 must yield
                    # exactly 2 available tokens, not diff=4 (R5 revision D).
                    target_value = max(0, new_int - self._borrowed)
                    while self._semaphore._value < target_value:
                        if self._semaphore._waiters:
                            loop, event = self._semaphore._waiters.popleft()
                            try:
                                loop.call_soon_threadsafe(event.set)
                            except RuntimeError:
                                # WHY: dead loop — the waiter can never take
                                # the token; the loop retries with the next
                                # one (R5 FIX-E).
                                continue
                        else:
                            self._semaphore._value += 1
                elif diff < 0:
                    # Shrink only reclaims AVAILABLE tokens (value); borrowed ones
                    # are untouched — the cap below is inherent (value cannot go
                    # negative), so shrinking below the borrowed count is safe.
                    to_reduce = -diff
                    for _ in range(to_reduce):
                        if self._semaphore._value > 0:
                            self._semaphore._value -= 1

    @property
    def available_tokens(self) -> float:
        """Currently available tokens."""
        with self._total_lock:
            return self._available_locked()

    @property
    def borrowed_tokens(self) -> float:
        """Currently borrowed tokens."""
        with self._total_lock:
            return self._total_tokens - self._available_locked()

    def snapshot(self) -> tuple[float, float, float]:
        """Return ``(total_tokens, available_tokens, borrowed_tokens)`` atomically.

        The three individual properties each take ``_total_lock`` separately,
        so a concurrent ``total_tokens`` write between two reads can mix values
        computed against *different* totals (e.g. ``avail`` from the new total
        with ``total`` read before the write).  This method reads all three
        under one lock acquisition: the invariant
        ``available + borrowed == total`` then holds by construction, since
        ``borrowed`` is derived from the same locked ``total`` and ``avail``.
        """
        with self._total_lock:
            avail = self._available_locked()
            return (self._total_tokens, avail, self._total_tokens - avail)

    def _available_locked(self) -> float:
        """Compute available tokens while ``_total_lock`` is held.

        May be negative when the total was shrunk below the borrowed count —
        the real state (anyio semantics), never a fictional number.
        """
        return self._total_tokens - self._borrowed

    async def acquire(self) -> None:
        """Acquire one token, blocking if none are available."""
        await self._semaphore.acquire()
        # No await between the successful acquire and the increment — a
        # concurrent resize can never observe a half-registered borrow, and
        # cancellation cannot be delivered in between.
        with self._total_lock:
            self._borrowed += 1

    def release(self) -> None:
        """Release one token.

        :raises ValueError: if released more times than acquired (borrowed
            count is already zero — the over-release case).
        WHY the cap is clamped, not propagated: after the total was shrunk
        below the borrowed count, returning a token is a legal return of an
        over-budget borrow — the token is absorbed (value stays at max)
        instead of raising, so the accounting ``avail + borrowed == total``
        converges back to the real state (R5 revision D).
        """
        with self._total_lock:
            if self._borrowed <= 0:
                raise ValueError("CapacityLimiter released too many times")
            with self._semaphore._lock:
                token_delivered = False
                while self._semaphore._waiters:
                    loop, event = self._semaphore._waiters.popleft()
                    try:
                        loop.call_soon_threadsafe(event.set)
                        token_delivered = True
                        break
                    except RuntimeError:
                        # WHY: dead loop — keep looking for a live waiter;
                        # if none remain the token is absorbed below (R5
                        # FIX-E).
                        continue
                if not token_delivered and self._semaphore._value < self._semaphore._max_value:
                    self._semaphore._value += 1
                # else: over-budget return — absorbed; value stays at max
            self._borrowed -= 1

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
    """A cross-event-loop-safe event with trio-style semantics (no clear).

    An :class:`Event` can be *set* (from any thread / event loop) and
    *waited* on (asynchronously).  Once set, the event stays set forever;
    there is no ``clear()`` method.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._flag = False
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []

    def is_set(self) -> bool:
        """Return ``True`` if the event has been set."""
        with self._lock:
            return self._flag

    def set(self) -> None:
        """Set the event and wake **all** current waiters.

        This method is synchronous and safe to call from any thread.
        Once set, the event cannot be cleared — subsequent ``wait()``
        calls return immediately.
        """
        with self._lock:
            self._flag = True
            waiters = self._waiters
            self._waiters = []

        for loop, event in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                # WHY: a waiter whose loop was closed can never wake — skip
                # it instead of aborting the whole wakeup loop, which would
                # leave every live-loop waiter sleeping forever (R5 FIX-E).
                pass

    async def wait(self) -> None:
        """Wait until the event has been set.

        If the event was already set prior to this call, return
        immediately.  Otherwise, block asynchronously until
        :meth:`set` is called.
        """
        loop = asyncio.get_running_loop()

        with self._lock:
            if self._flag:
                return
            event = asyncio.Event()
            self._waiters.append((loop, event))
        try:
            await event.wait()
        except BaseException:
            with self._lock:
                self._waiters = [(l, e) for (l, e) in self._waiters if e is not event]
            raise


# ============================================================================
# Condition — async condition variable atop gsyncio.Lock
# ============================================================================


class Condition:
    """An async condition variable backed by a cross-thread-safe :class:`Lock`.

    A :class:`Condition` provides ``wait()``, ``notify()``, and
    ``notify_all()`` on top of a :class:`Lock`.  Waiters release the lock
    while blocked and re-acquire it before ``wait()`` returns — even if the
    waiting task is cancelled.

    The internal waiter queue is protected by a separate
    :class:`threading.Lock`, making the condition safe for use across
    multiple event-loop threads.
    """

    def __init__(self, lock: Lock | None = None) -> None:
        self._lock = lock if lock is not None else Lock()
        self._waiters_lock = threading.Lock()
        self._waiters: collections.deque[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = (
            collections.deque()
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

        The **underlying lock must be held** when this method is called.
        It is released while waiting and re-acquired before returning.
        If the waiting task is cancelled, it is removed from the waiter
        queue and the lock is re-acquired under a cancellation shield.

        Usage::

            async with cond:
                while not predicate():
                    await cond.wait()
                # predicate is true and lock is still held here
        """
        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        with self._waiters_lock:
            self._waiters.append((loop, event))
        self._lock.release()
        try:
            await event.wait()
        except BaseException:
            # WHY: notify() may already have popped and woken this waiter —
            # the notification is then lost (W11).  Forward it to the next
            # FIFO waiter so notify(n) semantics are preserved; a cancelled
            # successor forwards again via its own cancel handler.
            was_present = self._discard_waiter(event)
            if not was_present:
                self._forward_notify()
            await self._reacquire_lock()
            raise
        else:
            await self._reacquire_lock()

    def notify(self, n: int = 1) -> None:
        """Wake up to *n* waiters (default: 1).

        This method is synchronous and safe to call from any thread.
        The underlying lock does **not** need to be held — this follows
        trio / threading.Condition semantics where notifying outside
        the lock is valid (and often preferred to avoid the "thundering
        herd" scheduling issue).
        """
        # WHY: notify() must not require the underlying lock. wait() releases the
        # lock before parking, so a lock-gated notify could deadlock, and holding
        # the lock would let producers batch notifications and starve waiters.
        if n <= 0:
            return
        for waiter_loop, event in self._pop_waiters(n):
            try:
                waiter_loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                # WHY: dead loop — this notification would be lost anyway;
                # skip rather than abort notifying the remaining waiters
                # (R5 FIX-E).
                pass

    def notify_all(self) -> None:
        """Wake up **all** current waiters.

        This method is synchronous and safe to call from any thread.
        """
        with self._waiters_lock:
            n = len(self._waiters)
        self.notify(n)

    # -- internals ----------------------------------------------------------

    def _pop_waiters(self, n: int) -> list[tuple[asyncio.AbstractEventLoop, asyncio.Event]]:
        """Pop up to *n* waiters under the internal lock."""
        with self._waiters_lock:
            count = min(n, len(self._waiters))
            popped: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
            for _ in range(count):
                popped.append(self._waiters.popleft())
            return popped

    def _discard_waiter(self, event: asyncio.Event) -> bool:
        """Remove *event* from the waiter deque if still present.

        Returns ``True`` if the entry was still queued (i.e. this waiter was
        cancelled while waiting), ``False`` if notify() had already popped it
        — in which case the notification must be forwarded.
        """
        with self._waiters_lock:
            remaining = collections.deque(entry for entry in self._waiters if entry[1] is not event)
            was_present = len(remaining) != len(self._waiters)
            self._waiters = remaining
            return was_present

    def _forward_notify(self) -> None:
        """Pass a lost notification to the next FIFO waiter (caller holds nothing)."""
        with self._waiters_lock:
            while self._waiters:
                next_loop, next_event = self._waiters.popleft()
                try:
                    next_loop.call_soon_threadsafe(next_event.set)
                    return
                except RuntimeError:
                    # WHY: dead loop — keep forwarding to the next live
                    # waiter; the notification dies only when no live waiter
                    # remains (R5 FIX-E).
                    continue

    async def _reacquire_lock(self) -> None:
        """Re-acquire the underlying lock, retrying through cancellations.

        Canonical ``asyncio.Condition.wait`` pattern (bpo-34094 family): a
        cancellation delivered while re-acquiring must not abort the
        re-acquisition — the caller's eventual ``release()`` would then
        raise on a lock it no longer owns, masking the cancellation.  Each
        swallowed CancelledError leaves ``Lock`` in a clean state (its
        cancel path discards the waiter entry), so retrying is safe; the
        cancellation is re-raised once the lock is held again.
        """
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
    """Simple result object returned by :meth:`Barrier.wait`.

    Attributes:
        parties: Total number of parties in this barrier round.
    """

    __slots__ = ("parties",)

    def __init__(self, parties: int) -> None:
        self.parties = parties

    def __repr__(self) -> str:
        return f"<BarrierWaitResult parties={self.parties}>"


class Barrier:
    """An async barrier that synchronizes *N* parties.

    Each :meth:`wait` call blocks until *parties* tasks have called
    :meth:`wait`, at which point all of them resume simultaneously and
    receive a :class:`BarrierWaitResult`.  The barrier resets
    automatically after each round, so it can be reused.

    Calling :meth:`abort` breaks the current round: all waiting tasks
    raise a :exc:`RuntimeError`.
    """

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be >= 1")
        self._parties = parties
        self._mutex = threading.Lock()
        self._generation = 0
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
        self._aborted = False

    @property
    def parties(self) -> int:
        """Total number of parties required to pass the barrier."""
        return self._parties

    @property
    def n_waiting(self) -> int:
        """Number of tasks currently waiting at the barrier."""
        with self._mutex:
            return len(self._waiters)

    async def wait(self) -> BarrierWaitResult:
        """Wait at the barrier until *parties* tasks have arrived.

        Returns a :class:`BarrierWaitResult` with ``parties`` set to the
        barrier's party count.  A normal return always means the round was
        completed; :meth:`abort` raises instead.

        :raises RuntimeError: if :meth:`abort` is called.
        """
        loop = asyncio.get_running_loop()
        event = asyncio.Event()

        with self._mutex:
            if self._aborted:
                raise RuntimeError("barrier has been aborted")
            n = len(self._waiters) + 1
            if n == self._parties:
                for l, e in self._waiters:
                    try:
                        l.call_soon_threadsafe(e.set)
                    except RuntimeError:
                        # WHY: dead loop — the party is gone; waking the
                        # remaining live parties must not abort (R5 FIX-E).
                        pass
                self._waiters.clear()
                self._generation += 1
                return BarrierWaitResult(parties=self._parties)
            self._waiters.append((loop, event))
            generation = self._generation

        try:
            await event.wait()
        except asyncio.CancelledError:
            with self._mutex:
                # WHY: The generation guard stops a cancelled waiter from deleting
                # an entry that already advanced to the next round. If a full party
                # set woke us, our entry is gone and the generation incremented, so
                # skipping the removal protects the next round's waiters.
                if generation == self._generation:
                    try:
                        self._waiters.remove((loop, event))
                    except ValueError:
                        pass
            raise

        with self._mutex:
            if self._aborted:
                raise RuntimeError("barrier has been aborted")
            return BarrierWaitResult(parties=self._parties)

    def abort(self) -> None:
        """Abort the barrier, raising :exc:`RuntimeError` in all waiting tasks.

        The barrier is permanently broken after an abort.  Subsequent
        calls to :meth:`wait` will raise immediately.
        """
        with self._mutex:
            self._aborted = True
            waiters = self._waiters
            self._waiters = []

        for loop, event in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                # WHY: dead loop — skip rather than abort the abort (R5 FIX-E).
                pass
