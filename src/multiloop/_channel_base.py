"""Shared channel base class and cross-thread waiter queues for Channel."""

from __future__ import annotations

import asyncio
import collections
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

from multiloop.exceptions import ChannelClosedError

_Waiter = tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]]
_CHANNEL_CLOSED_MSG = "Channel is closed"

__all__ = ["_CHANNEL_CLOSED_MSG", "_BaseChannel", "_discard_waiter", "_set_soon", "_wake_all"]


def _set_soon(
    loop: asyncio.AbstractEventLoop,
    fut: asyncio.Future[Any],
    exc: BaseException | None = None,
) -> None:
    """Complete a waiter future on its owning event loop, safely tolerating concurrent cancellation.

    :param loop: The event loop owning ``fut``.
    :param fut: The future to fulfill or fail.
    :param exc: Optional exception to deliver. If None, fulfills with None.
    """

    def _do() -> None:
        try:
            if exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(None)
        except asyncio.InvalidStateError:
            pass

    try:
        loop.call_soon_threadsafe(_do)
    except RuntimeError:
        pass


def _wake_all(
    waiters: (
        collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop]
        | collections.deque[_Waiter]
        | list[_Waiter]
    ),
    exc: BaseException | None = None,
    count: int | None = None,
) -> None:
    """Wake pending waiter futures on their respective owning event loops.

    :param waiters: Queue or dictionary of waiter futures mapped to their event loops.
    :param exc: Optional exception to deliver instead of successful resolution.
    :param count: Maximum number of active waiters to wake (wake all if None).
    """
    if isinstance(waiters, collections.OrderedDict):
        woken = 0
        while waiters and (count is None or woken < count):
            fut, loop = waiters.popitem(last=False)
            if fut.done():
                continue
            _set_soon(loop, fut, exc)
            woken += 1
        return
    if isinstance(waiters, collections.deque):
        woken = 0
        while waiters and (count is None or woken < count):
            loop, fut = waiters.popleft()
            if fut.done():
                continue
            _set_soon(loop, fut, exc)
            woken += 1
        return
    for loop, fut in waiters:
        if not fut.done():
            _set_soon(loop, fut, exc)


def _discard_waiter(
    waiters: (
        collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop]
        | collections.deque[_Waiter]
    ),
    fut: asyncio.Future[Any],
) -> bool:
    """Remove a waiter future from the waiter queue if still present.

    :param waiters: The collection of active waiters.
    :param fut: The future to discard.
    :returns: ``True`` if the future was queued and removed; ``False`` if it had
              already been popped by a concurrent wakeup.
    """
    if isinstance(waiters, collections.OrderedDict):
        return waiters.pop(fut, None) is not None
    remaining = collections.deque(w for w in waiters if w[1] is not fut)
    was_present = len(remaining) != len(waiters)
    waiters.clear()
    waiters.extend(remaining)
    return was_present


class _BaseChannel:
    """Base class providing shared waiter management and synchronization for async channels."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._getters_dict: collections.OrderedDict[
            asyncio.Future[Any], asyncio.AbstractEventLoop
        ] = collections.OrderedDict()
        self._putters_dict: collections.OrderedDict[
            asyncio.Future[Any], asyncio.AbstractEventLoop
        ] = collections.OrderedDict()
        self._notifiers_dict: collections.OrderedDict[
            tuple[asyncio.AbstractEventLoop, asyncio.Event | asyncio.Future[Any]], None
        ] = collections.OrderedDict()

    @property
    def _getters(
        self,
    ) -> collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop]:
        return self._getters_dict

    @property
    def _putters(
        self,
    ) -> collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop]:
        return self._putters_dict

    @property
    def _notifiers(
        self,
    ) -> collections.OrderedDict[
        tuple[asyncio.AbstractEventLoop, asyncio.Event | asyncio.Future[Any]], None
    ]:
        return self._notifiers_dict

    def _wakeup_next(
        self,
        waiters: (
            collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop]
            | collections.deque[_Waiter]
        ),
    ) -> None:
        """Wake the first pending non-done future in the waiter queue.

        :param waiters: The queue or dict of registered waiters.
        """
        _wake_all(waiters, count=1)

    def _wakeup_notifiers(self, all_notifiers: bool = False) -> None:
        """Wake registered select watchers. Caller MUST hold ``_lock``.

        :param all_notifiers: If True (channel closed), wakes all watchers;
                              if False (item arrived), wakes at most one to prevent thundering herd.
        """
        while self._notifiers:
            entry, _ = self._notifiers.popitem(last=False)
            loop, target = entry[0], entry[1]
            if isinstance(target, asyncio.Event):
                try:
                    loop.call_soon_threadsafe(target.set)
                    if not all_notifiers:
                        return
                except RuntimeError:
                    pass
            else:
                if target.done():
                    continue

                def _wake(f: asyncio.Future[Any] = target, ch: Any = self) -> None:
                    if not f.done():
                        f.set_result(ch)
                    else:
                        with ch._lock:
                            ch._wakeup_notifiers(all_notifiers=False)

                try:
                    loop.call_soon_threadsafe(_wake)
                    if not all_notifiers:
                        return
                except RuntimeError:
                    pass

    def _wakeup_select_watchers(self) -> None:
        """Wake every registered select watcher (caller must hold ``_lock``)."""
        self._wakeup_notifiers(all_notifiers=True)

    def _register_select_watcher(
        self, loop: asyncio.AbstractEventLoop, arbiter_fut: asyncio.Future[Any]
    ) -> Any | None:
        """Register a single-arbiter select watcher.

        :param loop: The event loop on which the select arbiter runs.
        :param arbiter_fut: The arbiter future awaiting ready channel signal.
        :returns: A token tuple if successfully registered on an empty channel,
                  or None if the channel already has an item available.
        """
        with self._lock:
            if self.is_closed and self.qsize() == 0:
                return None
            if self.qsize() > 0:
                return None
            token = (loop, arbiter_fut)
            self._notifiers[token] = None
            return token

    def _unregister_select_watcher(self, token: Any) -> None:
        """Unregister a select watcher token.

        :param token: The registration token returned by :meth:`_register_select_watcher`.
        """
        if token is None:
            return
        with self._lock:
            self._notifiers.pop(token, None)

    async def recv(self, timeout: float | None = None) -> Any:
        """Receive an item from the channel, suspending if empty until an item arrives.

        :param timeout: Optional maximum seconds to wait.
        :returns: The received item.
        :raises ChannelClosedError: If the channel is closed and all buffered items have been consumed.
        :raises TimeoutError: If timeout expires before an item is received.
        """
        if timeout is not None:
            return await asyncio.wait_for(self._recv_impl(), timeout=timeout)
        return await self._recv_impl()

    async def _recv_impl(self) -> Any:
        """Receive an item without timeout handling (implemented by subclasses)."""
        raise NotImplementedError

    def try_send(self, item: Any) -> bool:
        """Non-blocking send.

        :param item: The item to enqueue.
        :returns: True if enqueued, False if channel is full.
        """
        raise NotImplementedError

    def try_recv(self) -> Any:
        """Non-blocking receive.

        :returns: The dequeued item.
        :raises WouldBlock: If channel is empty.
        """
        raise NotImplementedError

    def qsize(self) -> int:
        """Number of items currently buffered."""
        raise NotImplementedError

    @property
    def is_closed(self) -> bool:
        """Whether the channel has been closed."""
        raise NotImplementedError

    def _close_waiters(self) -> None:
        """Wake all pending getters, putters, and notifiers with ChannelClosedError.

        Caller must hold ``self._lock``.
        """
        exc = ChannelClosedError(_CHANNEL_CLOSED_MSG)
        _wake_all(self._getters, exc=exc)
        _wake_all(self._putters, exc=exc)
        self._wakeup_notifiers(all_notifiers=True)

    def _discard_waiter(
        self,
        waiters: (
            collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop]
            | collections.deque[_Waiter]
        ),
        fut: asyncio.Future[Any],
    ) -> bool:
        """Remove a waiter future from the queue.

        :param waiters: The collection of active waiters.
        :param fut: The future to remove.
        :returns: True if found and removed, False otherwise.
        """
        return _discard_waiter(waiters, fut)

    async def _wait_and_send(self, item: Any, try_fn: Callable[[Any], bool]) -> None:
        """Send ``item``, suspending until buffer capacity frees up.

        :param item: The item to send.
        :param try_fn: Non-blocking try_send function to invoke.
        """
        loop = asyncio.get_running_loop()
        while True:
            try:
                if try_fn(item):
                    with self._lock:
                        if self._getters:
                            self._wakeup_next(self._getters)
                        else:
                            self._wakeup_notifiers(all_notifiers=False)
                    return
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            with self._lock:
                try:
                    if try_fn(item):
                        if self._getters:
                            self._wakeup_next(self._getters)
                        else:
                            self._wakeup_notifiers(all_notifiers=False)
                        return
                except RuntimeError as e:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

                fut: asyncio.Future[Any] = loop.create_future()
                self._putters[fut] = loop

            try:
                await fut
            except BaseException:
                with self._lock:
                    was_present = self._discard_waiter(self._putters, fut)
                    if not was_present:
                        self._wakeup_next(self._putters)
                raise

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.recv()
        except ChannelClosedError:
            raise StopAsyncIteration from None
