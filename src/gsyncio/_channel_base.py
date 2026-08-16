"""Shared base class for FastChannel (AsyncChannel removed in FIX-8a)."""

import asyncio
import collections
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

from gsyncio.exceptions import ChannelClosedError

_Waiter = tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]]

_CHANNEL_CLOSED_MSG = "Channel is closed"


def _set_soon(
    loop: asyncio.AbstractEventLoop,
    fut: asyncio.Future[Any],
    exc: BaseException | None = None,
) -> None:
    """Complete *fut* on its owning loop, tolerating a raced cancellation.

    WHY: the old guard wrapped call_soon_threadsafe itself, where an
    InvalidStateError (waiter cancelled between pop and delivery) surfaced
    asynchronously in the loop exception handler (W21).  Moving the guard
    inside the scheduled callback contains it.
    """

    def _do() -> None:
        try:
            if exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(None)
        except asyncio.InvalidStateError:
            # The waiter was cancelled after the pop — its cancel handler
            # already cleaned up; nothing to deliver.
            pass

    try:
        loop.call_soon_threadsafe(_do)
    except RuntimeError:
        # WHY: the waiter's loop was closed — it can never be woken.  Skip
        # it instead of aborting the caller's wakeup loop (R5 FIX-E).
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
    """Wake pending waiter futures on their owning event loops.

    Each non-done waiter is completed with ``set_result(None)``, or with
    ``set_exception(exc)`` when ``exc`` is given. Waking stops after ``count``
    non-done futures when ``count`` is not ``None``. When ``waiters`` is an
    OrderedDict or deque, entries are consumed from it (done entries are discarded)
    so stale futures don't accumulate; a plain list is left intact.
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
    """Remove the waiter entry for ``fut`` from ``waiters`` (if still present).

    Returns ``True`` when the entry was still queued, ``False`` when it had
    already been popped by a wakeup — the discrimination the cancellation
    path needs to decide whether the wakeup must be forwarded (R10 P1: a
    popped entry means the data/slot side already changed, and the
    notification would otherwise die with the cancelled waiter).
    """
    if isinstance(waiters, collections.OrderedDict):
        return waiters.pop(fut, None) is not None
    remaining = collections.deque(w for w in waiters if w[1] is not fut)
    was_present = len(remaining) != len(waiters)
    waiters.clear()
    waiters.extend(remaining)
    return was_present


class _BaseChannel:  # pyright: ignore[reportUnusedClass]
    """Base class providing shared waiter deque management for channels."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._getters_dict: collections.OrderedDict[
            asyncio.Future[Any], asyncio.AbstractEventLoop
        ] = collections.OrderedDict()
        self._putters_dict: collections.OrderedDict[
            asyncio.Future[Any], asyncio.AbstractEventLoop
        ] = collections.OrderedDict()
        # WHY: select_channel needs "channel became non-empty" signals that do
        # NOT consume the item (only the select winner may consume).  Notifier
        # events are one-shot: _wakeup_notifiers pops them, so a woken notifier
        # must re-register (its caller holds the new event).
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
        """Wake the first non-done future in the waiter deque/dict."""
        _wake_all(waiters, count=1)

    def _wakeup_notifiers(self, all_notifiers: bool = False) -> None:
        """Wake registered notifiers or select watchers (caller must hold ``_lock``).

        When all_notifiers is False (single item enqueued), wakes at most one valid
        notifier to prevent thundering herd. When True (channel close), wakes all.
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
                        # WHY: in multi-event-loop execution, target.done() was False when
                        # popped under _lock, but another channel in select_channel won the
                        # race before this callback ran on its loop. Forward the wakeup signal
                        # to the next pending notifier so the buffered item is not starved (R10).
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

        Returns a token if successfully registered and empty, or None if the channel
        already has an item (caller should try_recv immediately).
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
        """Unregister a select watcher token."""
        if token is None:
            return
        with self._lock:
            self._notifiers.pop(token, None)

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

    @property
    def is_closed(self) -> bool:
        """Whether the channel is closed (implemented by subclasses)."""
        raise NotImplementedError

    def _close_waiters(self) -> None:
        """Wake all pending getters and putters with ChannelClosedError.

        Caller must hold ``self._lock``.
        """
        exc = ChannelClosedError(_CHANNEL_CLOSED_MSG)
        _wake_all(self._getters, exc=exc)
        _wake_all(self._putters, exc=exc)
        # Notifiers and select watchers wake too (all of them on close)
        self._wakeup_notifiers(all_notifiers=True)

    def _discard_waiter(
        self,
        waiters: (
            collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop]
            | collections.deque[_Waiter]
        ),
        fut: asyncio.Future[Any],
    ) -> bool:
        """Remove the waiter entry for ``fut`` from ``waiters`` (if still present)."""
        return _discard_waiter(waiters, fut)

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
                if try_fn(item):
                    with self._lock:
                        if self._getters:
                            self._wakeup_next(self._getters)
                        else:
                            self._wakeup_notifiers(all_notifiers=False)
                    return
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            # Double-check under lock before queueing future to prevent the
            # Lost-Wakeup race (see docstring above).
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
                        # WHY (R10 P1): a receiver already popped our entry
                        # and handed us the freed slot — but we were
                        # cancelled before retrying the send.  The slot must
                        # not idle: forward the wakeup to the next putter,
                        # which retries its send.  A cancelled successor
                        # forwards again via its own handler (chain
                        # forwarding, same as Lock/Semaphore/Condition).
                        self._wakeup_next(self._putters)
                raise

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.recv()
        except ChannelClosedError:
            raise StopAsyncIteration from None
