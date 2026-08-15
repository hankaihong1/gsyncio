"""Concurrency primitives and synchronization tools."""

import asyncio
import builtins
import collections
import threading
from collections.abc import Callable
from typing import Any, Protocol

from gsyncio._channel_base import (
    _CHANNEL_CLOSED_MSG,
    _BaseChannel,
    _discard_waiter,
    _wake_all,
)
from gsyncio._rust import _try_import_rust_class
from gsyncio.exceptions import ChannelClosedError, TimeoutError, WouldBlock


class _FastChannelProtocol(Protocol):
    """Protocol for the Rust FastChannel class."""

    def __init__(self, maxsize: int) -> None: ...
    def try_send(self, item: Any) -> bool: ...
    def try_recv(self) -> tuple[bool, Any]: ...
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def qsize(self) -> int: ...


class _WaitGroupProtocol(Protocol):
    """Protocol for the Rust RawAsyncWaitGroup class."""

    def __init__(self) -> None: ...
    def add(self, delta: int) -> Any: ...
    def done(self) -> Any: ...
    def register_waiter(self, waiter: Any) -> bool: ...
    def unregister_waiter(self, fut: Any) -> bool: ...


_RustFastChannel: type[_FastChannelProtocol] | None = _try_import_rust_class(
    "gsyncio._gsyncio_core", "FastChannel"
)
_RawAsyncWaitGroup: type[_WaitGroupProtocol] | None = _try_import_rust_class(
    "gsyncio._gsyncio_core", "RawAsyncWaitGroup"
)


class FastChannel(_BaseChannel):
    """High-performance cross-thread safe Channel backed by Rust flume.

    This channel enables lock-free cross-thread queueing backed by Rust `flume`
    with Double-Check Lock protection against lost wakeup signals.

    :param maxsize:
        Maximum number of items the channel can hold. Defaults to 0 (unbounded).
    :type maxsize: int

    """

    def __init__(self, maxsize: int = 0) -> None:
        if _RustFastChannel is None:
            raise RuntimeError("_gsyncio_core Rust extension is not compiled.")
        super().__init__()
        self._maxsize = max(0, int(maxsize))
        self._inner = _RustFastChannel(self._maxsize)

    def close(self) -> None:
        """Close the channel.

        All pending senders and receivers will be woken up and fail with
        :class:`ChannelClosedError`.

        """
        self._inner.close()
        with self._lock:
            self._close_waiters()

    def __repr__(self) -> str:
        return f"<FastChannel is_closed={self.is_closed}>"

    @property
    def is_closed(self) -> bool:
        """Return whether the channel is closed.

        :returns: ``True`` if closed, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        return self._inner.is_closed()

    def try_send(self, item: Any) -> bool:
        """Non-blocking send. Returns True if item was enqueued, False if full.

        :param item: The object to send.

        :returns: ``True`` if the item was sent, ``False`` if the channel is full.
        :rtype: :class:`bool`

        :raises ChannelClosedError: If the channel is closed.

        """
        try:
            result = self._inner.try_send(item)
            if result:
                with self._lock:
                    self._wakeup_next(self._getters)
                    self._wakeup_notifiers()
                return True
            return False
        except RuntimeError as e:
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

    def try_recv(self) -> Any:
        """Non-blocking recv. Returns an item or raises :class:`WouldBlock`.

        :returns: The received item.

        :raises WouldBlock: If the channel is empty.
        :raises ChannelClosedError: If the channel is closed and empty.

        """
        try:
            # WHY: the native boundary returns (has_item, item) — a bare
            # Option<Py<PyAny>> cannot distinguish a None payload from an
            # empty channel (BUG-2).
            has_item, item = self._inner.try_recv()
        except RuntimeError as e:
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e
        if has_item:
            with self._lock:
                self._wakeup_next(self._putters)
            return item
        raise WouldBlock("Channel is empty")

    def qsize(self) -> int:
        """Return the number of items currently buffered in the channel.

        :returns: Number of buffered items.
        :rtype: :class:`int`

        """
        return int(self._inner.qsize())

    @property
    def maxsize(self) -> int:
        """Maximum number of items allowed in the channel (0 means unbounded)."""
        return self._maxsize

    def empty(self) -> bool:
        """Return ``True`` if the channel is currently empty, ``False`` otherwise."""
        return self.qsize() == 0

    def full(self) -> bool:
        """Return ``True`` if the channel is currently full, ``False`` otherwise."""
        if self._maxsize <= 0:
            return False
        return self.qsize() >= self._maxsize

    async def send(self, item: Any) -> None:
        """Send an item into the channel.

        If the channel is full, this method suspends until a slot becomes available.

        :param item:
            The object to send.

        :raises ChannelClosedError:
            If the channel is closed.

        """
        await self._wait_and_send(item, self._inner.try_send)

    async def _recv_impl(self) -> Any:
        loop = asyncio.get_running_loop()
        while True:
            try:
                has_item, item = self._inner.try_recv()
                if has_item:
                    with self._lock:
                        self._wakeup_next(self._putters)
                    return item
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            # Double-check inside lock before queueing future to prevent Lost-Wakeup race
            with self._lock:
                try:
                    has_item, item = self._inner.try_recv()
                    if has_item:
                        self._wakeup_next(self._putters)
                        return item
                except RuntimeError as e:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

                if self.is_closed:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG)
                fut: asyncio.Future[Any] = loop.create_future()
                self._getters.append((loop, fut))

            try:
                await fut
            except BaseException:
                with self._lock:
                    was_present = self._discard_waiter(self._getters, fut)
                    if not was_present:
                        # WHY (R10 P1): a sender already popped our entry and
                        # queued the item — but we were cancelled before
                        # consuming it.  Forward the wakeup so the buffered
                        # item reaches the next getter.
                        self._wakeup_next(self._getters)
                raise


_UNSET: Any = object()


async def select_channel(
    *channels: Any,
    timeout: float | None = None,
    default: Any = _UNSET,
    _deadline: float | None = None,
) -> Any:
    """Select the first ready channel from multiple channel instances.

    :param channels:
        One or more :class:`FastChannel` instances to poll.

    :param timeout:
        Optional maximum time in seconds to wait for a channel to become ready.
    :type timeout: float or None

    :param default:
        Optional sentinel value. When provided, ``select_channel`` returns
        immediately (non-blocking): it tries each channel with
        :meth:`~FastChannel.try_recv` and returns ``(channel, value)`` for
        the first ready channel, or ``default`` if no channel is ready.

    :returns:
        When ``default`` is not provided: ``(selected_channel, received_value)``.
        When ``default`` is provided and a channel is ready:
        ``(selected_channel, received_value)``.
        When ``default`` is provided and no channel is ready: ``default``.
    :rtype: :class:`tuple` or any

    :raises ValueError:
        If no channels are provided.
    :raises TimeoutError:
        If the timeout expires before any channel becomes ready.

    """
    if not channels:
        raise ValueError("select_channel requires at least one channel")

    loop = asyncio.get_running_loop()
    if timeout is not None and _deadline is None:
        _deadline = loop.time() + timeout

    while True:
        # Phase 1: Fast Probe (try_recv)
        for ch in channels:
            try:
                val = ch.try_recv()
                return ch, val
            except (ChannelClosedError, WouldBlock):
                continue

        if default is not _UNSET:
            return default

        if all(ch.is_closed and ch.qsize() == 0 for ch in channels):
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG)

        remaining_timeout: float | None = None
        if _deadline is not None:
            remaining_timeout = max(0.0, _deadline - loop.time())
            if remaining_timeout <= 0:
                raise TimeoutError("select_channel timed out")
        elif timeout is not None and timeout <= 0:
            raise TimeoutError("select_channel timed out")

        # Phase 2: Single-Future Multi-Registration Arbiter
        arbiter_fut: asyncio.Future[Any] = loop.create_future()
        registered_channels: list[tuple[Any, Any]] = []

        try:
            for ch in channels:
                token = ch._register_select_watcher(loop, arbiter_fut)
                if token is not None:
                    registered_channels.append((ch, token))
                elif not arbiter_fut.done():
                    try:
                        return ch, ch.try_recv()
                    except (ChannelClosedError, WouldBlock):
                        pass

            if all(ch.is_closed and ch.qsize() == 0 for ch in channels):
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG)

            if remaining_timeout is not None:
                ready_ch = await asyncio.wait_for(arbiter_fut, timeout=remaining_timeout)
            else:
                ready_ch = await arbiter_fut

            try:
                return ready_ch, ready_ch.try_recv()
            except (WouldBlock, ChannelClosedError):
                # Item consumed by concurrent reader or closed, retry arbitration iteratively
                continue
        except (builtins.TimeoutError, TimeoutError):
            raise TimeoutError("select_channel timed out") from None
        finally:
            for ch, token in registered_channels:
                ch._unregister_select_watcher(token)


class AsyncWaitGroup:
    """Cross-thread WaitGroup for coordinating multiple asynchronous tasks.

    Allows one or more tasks to wait until a set of operations being performed
    in other tasks completes (Go `sync.WaitGroup` style).

    """

    def __init__(self) -> None:
        if _RawAsyncWaitGroup is None:
            raise RuntimeError("_gsyncio_core Rust extension is not compiled.")
        self._inner = _RawAsyncWaitGroup()

    def __repr__(self) -> str:
        return "<AsyncWaitGroup>"

    def add(self, delta: int = 1) -> None:
        """Adjust the internal counter by ``delta`` (Go ``sync.WaitGroup`` semantics).

        Negative deltas are allowed but must not drive the counter below zero.

        :param delta:
            Amount to add to the counter. Defaults to 1.
        :type delta: int

        :raises RuntimeError:
            If the counter would go negative.

        """
        waiters = self._inner.add(delta)
        if waiters:
            _wake_all(waiters)

    def done(self) -> None:
        """Decrement the counter by 1. Wakes waiters if counter reaches 0."""
        waiters = self._inner.done()
        if waiters:
            _wake_all(waiters)

    def track(self, coro: Any) -> Any:
        """Wrap a coroutine or callable, incrementing the counter immediately
        (happens-before) and decrementing it when execution finishes (in ``finally``).

        :param coro: A coroutine object, Future, or callable to wrap and track.
        :returns: An awaitable that decrements the counter upon completion.
        """
        self.add(1)

        async def _wrapped() -> Any:
            try:
                if asyncio.iscoroutine(coro) or asyncio.isfuture(coro):
                    return await coro
                elif callable(coro):
                    res = coro()
                    if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                        return await res
                    return res
                return coro
            finally:
                self.done()

        return _wrapped()

    async def wait(self) -> None:
        """Suspend execution until the WaitGroup counter becomes 0.

        .. note::
            If the waiting task is cancelled, its entry is removed from the
            Rust waiter list immediately (R5 FIX-D) — a cancelled wait never
            accumulates stale entries.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        already_done = self._inner.register_waiter((loop, fut))
        if already_done:
            return
        try:
            await fut
        except BaseException:
            # WHY: an entry left behind by a cancelled wait would sit in the
            # Rust waiter list until the counter next reaches zero —
            # unbounded growth on long-lived groups.  The unregister is a
            # no-op when done() already handed the entry over (its waker
            # skips done futures), so there is no lost-wakeup (R5 FIX-D).
            self._inner.unregister_waiter(fut)
            raise


class AsyncOnce:
    """Thread-safe single execution primitive across multiple event loops.

    Ensures that a given initialization function is executed exactly once
    regardless of how many threads or coroutines attempt to execute it
    concurrently (Go `sync.Once` style).

    """

    def __init__(self) -> None:
        self._thread_lock = threading.Lock()
        self._done = False
        self._result: Any = None
        self._exc: BaseException | None = None
        self._waiters: collections.deque[tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]]] = (
            collections.deque()
        )
        self._executing = False

    def __repr__(self) -> str:
        return f"<AsyncOnce done={self._done}>"

    async def do(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute `fn` if and only if it has never been executed before.

        .. warning::
            Do not call :meth:`do` (directly or indirectly) from inside
            ``fn`` — the leader task waits for its own completion, which is
            a deadlock (the same limitation as Go's ``sync.Once``; R3-FIX-22
            probe).  Spawn a separate task if ``fn`` needs the once result.

        :param fn:
            The function or coroutine function to execute.
        :type fn: callable

        :returns: The result returned by `fn`.

        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] | None = None

        with self._thread_lock:
            if self._done:
                if self._exc:
                    raise self._exc
                return self._result

            if not self._executing:
                self._executing = True
                is_leader = True
            else:
                is_leader = False

            if not is_leader:
                fut = loop.create_future()
                self._waiters.append((loop, fut))

        if not is_leader:
            assert fut is not None  # assigned above when is_leader is False
            try:
                await fut
            finally:
                with self._thread_lock:
                    _discard_waiter(self._waiters, fut)
            if self._exc:
                raise self._exc
            return self._result

        # Leader executes fn
        try:
            res = fn(*args, **kwargs)
            if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                res = await res
            self._result = res
        except BaseException as e:
            if isinstance(e, asyncio.CancelledError):
                # R7-D: a cancelled leader is not a function failure — convert
                # the CancelledError at the boundary into a stable exception.
                # Storing the CE in _exc would make later unrelated callers
                # raise it (this file, :474), and a user-level CancelledError
                # marks its task as cancelled (probe R7-BD).  The leader itself
                # still re-raises the original CE; followers and new callers
                # get a RuntimeError.  Note: `RuntimeError(...) from e` is a
                # SyntaxError in assignment context (`from` is only valid in
                # raise statements) — keep the chain via __cause__.
                self._exc = RuntimeError("AsyncOnce execution was cancelled")
                self._exc.__cause__ = e
            else:
                self._exc = e
            raise
        finally:
            with self._thread_lock:
                self._done = True
                _wake_all(self._waiters)

        return self._result
