"""Shared base class for AsyncChannel and FastChannel."""

import asyncio
import collections
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

from gsyncio.exceptions import ChannelClosedError

_Waiter = tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]]

_CHANNEL_CLOSED_MSG = "Channel is closed"


def _wake_all(
    waiters: collections.deque[_Waiter] | list[_Waiter],
    exc: BaseException | None = None,
    count: int | None = None,
) -> None:
    """Wake pending waiter futures on their owning event loops.

    Each non-done waiter is completed with ``set_result(None)``, or with
    ``set_exception(exc)`` when ``exc`` is given. Waking stops after ``count``
    non-done futures when ``count`` is not ``None``. When ``waiters`` is a
    deque, entries are consumed from it (done entries are discarded) so stale
    futures don't accumulate; a plain list is left intact.
    """
    if isinstance(waiters, collections.deque):
        woken = 0
        # WHY: Consuming from the left as we go means woken entries leave the deque,
        # so stale done futures are dropped and count=1 reliably wakes the oldest
        # live waiter even when cancelled ones sit at the head. Iterating in place
        # would re-process entries already removed by a concurrent discard.
        while waiters and (count is None or woken < count):
            loop, fut = waiters.popleft()
            if fut.done():
                continue
            if exc is not None:
                try:
                    loop.call_soon_threadsafe(fut.set_exception, exc)
                except asyncio.InvalidStateError:
                    pass
            else:
                try:
                    loop.call_soon_threadsafe(fut.set_result, None)
                except asyncio.InvalidStateError:
                    pass
            woken += 1
        return
    for loop, fut in waiters:
        if not fut.done():
            if exc is not None:
                try:
                    loop.call_soon_threadsafe(fut.set_exception, exc)
                except asyncio.InvalidStateError:
                    pass
            else:
                try:
                    loop.call_soon_threadsafe(fut.set_result, None)
                except asyncio.InvalidStateError:
                    pass


def _discard_waiter(waiters: collections.deque[_Waiter], fut: asyncio.Future[Any]) -> None:
    """Remove the waiter entry for ``fut`` from ``waiters`` (if still present)."""
    remaining = collections.deque(w for w in waiters if w[1] is not fut)
    waiters.clear()
    waiters.extend(remaining)


class _BaseChannel:  # pyright: ignore[reportUnusedClass]
    """Base class providing shared waiter deque management for channels."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._getters: collections.deque[_Waiter] = collections.deque()
        self._putters: collections.deque[_Waiter] = collections.deque()
        # WHY: select_channel needs "channel became non-empty" signals that do
        # NOT consume the item (only the select winner may consume).  Notifier
        # events are one-shot: _wakeup_notifiers pops them, so a woken notifier
        # must re-register (its caller holds the new event).
        self._notifiers: collections.deque[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = (
            collections.deque()
        )

    def _wakeup_next(self, waiters: collections.deque[_Waiter]) -> None:
        """Wake the first non-done future in the waiter deque."""
        _wake_all(waiters, count=1)

    def _wakeup_notifiers(self) -> None:
        """Wake every registered notifier event (caller must hold ``_lock``)."""
        while self._notifiers:
            loop, event = self._notifiers.popleft()
            loop.call_soon_threadsafe(event.set)

    def _register_notifier(
        self, loop: asyncio.AbstractEventLoop
    ) -> asyncio.Event | None:
        """Register a non-consuming readiness notifier.

        Returns ``None`` when the channel already holds an item (the caller
        should try_recv immediately) — checking under ``_lock`` closes the
        lost-wakeup window: a send that lands between the qsize check and the
        registration would otherwise wake nobody.
        """
        with self._lock:
            if self.qsize() > 0:
                return None
            event = asyncio.Event()
            self._notifiers.append((loop, event))
            return event

    def _discard_notifier(
        self, loop: asyncio.AbstractEventLoop, event: asyncio.Event
    ) -> None:
        """Remove a notifier registration (cancelled select reader)."""
        with self._lock:
            remaining = collections.deque(
                entry for entry in self._notifiers if entry[1] is not event
            )
            self._notifiers.clear()
            self._notifiers.extend(remaining)

    async def recv(self, timeout: float | None = None) -> Any:
        """Receive an item from the channel.

        If the channel is empty, this method suspends until an item arrives.

        :param timeout:
            Optional timeout in seconds to wait.
        :type timeout: float or None

        :returns: The received item.

        :raises ChannelClosedError:
            If the channel is closed and empty.
        :raises TimeoutError:
            If the operation times out.

        """
        if timeout is not None:
            return await asyncio.wait_for(self._recv_impl(), timeout=timeout)
        return await self._recv_impl()

    async def _recv_impl(self) -> Any:
        """Receive an item without timeout handling (implemented by subclasses)."""
        raise NotImplementedError

    def try_send(self, item: Any) -> bool:
        """Non-blocking send. Returns True if sent, False if channel full."""
        raise NotImplementedError

    def try_recv(self) -> Any:
        """Non-blocking recv. Returns item or raises WouldBlock."""
        raise NotImplementedError

    def qsize(self) -> int:
        """Number of items currently buffered."""
        raise NotImplementedError

    def _close_waiters(self) -> None:
        """Wake all pending getters and putters with ChannelClosedError.

        Caller must hold ``self._lock``.
        """
        exc = ChannelClosedError(_CHANNEL_CLOSED_MSG)
        _wake_all(self._getters, exc=exc)
        _wake_all(self._putters, exc=exc)
        # Notifiers wake too: a select reader then re-checks the channel and
        # observes the closed state via try_recv.
        self._wakeup_notifiers()

    def _discard_waiter(
        self, waiters: collections.deque[_Waiter], fut: asyncio.Future[Any]
    ) -> None:
        """Remove the waiter entry for ``fut`` from ``waiters`` (if still present)."""
        _discard_waiter(waiters, fut)

    async def _wait_and_send(self, item: Any, try_fn: Callable[[Any], bool]) -> None:
        """Send ``item``, suspending until a slot frees, using ``try_fn``.

        ``try_fn(item)`` is invoked while ``self._lock`` is held and must
        attempt to enqueue ``item``, returning ``True`` on success or ``False``
        when the channel is full. It may raise ``ChannelClosedError`` (or a
        ``RuntimeError``, which is translated) when the channel is closed.

        The double-check under the lock closes the Lost-Wakeup race: a fast
        send attempt can win against a draining receiver and then park on a
        future nobody will resolve. Re-checking before queueing the future
        means only a genuinely full channel sleeps.
        """
        loop = asyncio.get_running_loop()
        while True:
            try:
                with self._lock:
                    if try_fn(item):
                        self._wakeup_next(self._getters)
                        self._wakeup_notifiers()
                        return
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            # Double-check under lock before queueing future to prevent the
            # Lost-Wakeup race (see docstring above).
            with self._lock:
                try:
                    if try_fn(item):
                        self._wakeup_next(self._getters)
                        self._wakeup_notifiers()
                        return
                except RuntimeError as e:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

                fut: asyncio.Future[Any] = loop.create_future()
                self._putters.append((loop, fut))

            try:
                await fut
            except BaseException:
                with self._lock:
                    self._discard_waiter(self._putters, fut)
                raise

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.recv()
        except ChannelClosedError:
            raise StopAsyncIteration
