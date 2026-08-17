"""Concurrency primitives and synchronization tools."""

import asyncio
import builtins
import collections
import threading
import time
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator
from contextlib import asynccontextmanager
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


class _RawAsyncChannelProtocol(Protocol):
    """Protocol for the Rust RawAsyncChannel class."""

    def __init__(
        self,
        maxsize: int = ...,
        wake_fn: Any = ...,
        select_wake_fn: Any = ...,
    ) -> None: ...
    def close(self) -> None: ...
    def is_closed(self) -> bool: ...
    def qsize(self) -> int: ...
    @property
    def maxsize(self) -> int: ...
    def empty(self) -> bool: ...
    def full(self) -> bool: ...
    def try_send(self, item: Any) -> bool: ...
    def try_recv(self) -> tuple[bool, Any]: ...
    def register_getter(self, loop: Any, fut: Any) -> tuple[bool, Any]: ...
    def unregister_getter(self, fut: Any) -> bool: ...
    def register_putter(self, loop: Any, fut: Any) -> bool: ...
    def unregister_putter(self, fut: Any) -> bool: ...
    def register_select_watcher(self, loop: Any, arbiter_fut: Any, channel_token: Any) -> bool: ...
    def unregister_select_watcher(self, fut: Any) -> bool: ...
    def forward_select_wakeup(self) -> None: ...


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
_RustRawAsyncChannel: type[_RawAsyncChannelProtocol] | None = _try_import_rust_class(
    "gsyncio._gsyncio_core", "RawAsyncChannel"
)
_RawAsyncWaitGroup: type[_WaitGroupProtocol] | None = _try_import_rust_class(
    "gsyncio._gsyncio_core", "RawAsyncWaitGroup"
)


def _wake_fut(fut: asyncio.Future[Any], exc: BaseException | None = None) -> None:
    """Complete *fut* on its owning loop, tolerating raced cancellations."""
    try:
        if exc is not None:
            if not isinstance(exc, ChannelClosedError) and "closed" in str(exc).lower():
                exc = ChannelClosedError(_CHANNEL_CLOSED_MSG)
            fut.set_exception(exc)
        else:
            fut.set_result(None)
    except (asyncio.InvalidStateError, RuntimeError):
        pass


def _select_wake(arbiter_fut: asyncio.Future[Any], ch: Any) -> None:
    """Wake select arbiter future, or forward wakeup if already fulfilled."""
    try:
        if not arbiter_fut.done():
            arbiter_fut.set_result(ch)
        else:
            if hasattr(ch, "_forward_select_wakeup"):
                ch._forward_select_wakeup()
    except (asyncio.InvalidStateError, RuntimeError):
        pass


class _LenShim:
    """Helper enabling len() inspection on Rust-backed waiter queues."""

    def __init__(self, len_fn: Callable[[], int]) -> None:
        self._len_fn = len_fn

    def __len__(self) -> int:
        return int(self._len_fn())


class FastChannel(_BaseChannel):
    """High-performance cross-thread safe Channel backed by Rust RawAsyncChannel.

    This channel eliminates Python-level lock contention by delegating buffer
    queueing, waiting state machines, double-checked locks, and cross-thread wakeups
    directly to the Rust core runtime.

    :param maxsize:
        Maximum number of items the channel can hold. Defaults to 0 (unbounded).
    :type maxsize: int

    """

    def __init__(self, maxsize: int = 0) -> None:
        self._maxsize = max(0, int(maxsize))
        if _RustRawAsyncChannel is not None:
            self._inner: Any = _RustRawAsyncChannel(self._maxsize, _wake_fut, _select_wake)
            self._use_raw = True
        elif _RustFastChannel is not None:
            super().__init__()
            self._inner = _RustFastChannel(self._maxsize)
            self._use_raw = False
        else:
            raise RuntimeError("_gsyncio_core Rust extension is not compiled.")

    def close(self) -> None:
        """Close the channel.

        All pending senders and receivers will be woken up and fail with
        :class:`ChannelClosedError`.

        """
        if self._use_raw:
            self._inner.close()
        else:
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
        return bool(self._inner.is_closed())

    def try_send(self, item: Any) -> bool:
        """Non-blocking send. Returns True if item was enqueued, False if full.

        :param item: The object to send.

        :returns: ``True`` if the item was sent, ``False`` if the channel is full.
        :rtype: :class:`bool`

        :raises ChannelClosedError: If the channel is closed.

        """
        if self._use_raw:
            try:
                return bool(self._inner.try_send(item))
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e
        try:
            result = self._inner.try_send(item)
            if result:
                with self._lock:
                    if self._getters:
                        self._wakeup_next(self._getters)
                    else:
                        self._wakeup_notifiers(all_notifiers=False)
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
        if self._use_raw:
            try:
                has_item, item = self._inner.try_recv()
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e
            if has_item:
                return item
            raise WouldBlock("Channel is empty")
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
        if self._use_raw:
            return bool(self._inner.empty())
        return self.qsize() == 0

    def full(self) -> bool:
        """Return ``True`` if the channel is currently full, ``False`` otherwise."""
        if self._use_raw:
            return bool(self._inner.full())
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
        if self._use_raw:
            loop = asyncio.get_running_loop()
            while True:
                try:
                    if self._inner.try_send(item):
                        return
                except RuntimeError as e:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

                fut: asyncio.Future[Any] = loop.create_future()
                try:
                    if self._inner.register_putter(loop, fut) and self._inner.try_send(item):
                        return
                except RuntimeError as e:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

                try:
                    await fut
                except BaseException:
                    self._inner.unregister_putter(fut)
                    raise
        else:
            await self._wait_and_send(item, self._inner.try_send)

    async def _recv_impl(self) -> Any:
        if self._use_raw:
            loop = asyncio.get_running_loop()
            while True:
                try:
                    has_item, item = self._inner.try_recv()
                    if has_item:
                        return item
                except RuntimeError as e:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

                fut: asyncio.Future[Any] = loop.create_future()
                try:
                    has_item, item = self._inner.register_getter(loop, fut)
                    if has_item:
                        return item
                except RuntimeError as e:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

                try:
                    await fut
                except BaseException:
                    self._inner.unregister_getter(fut)
                    raise
        else:
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
                    fut = loop.create_future()
                    self._getters[fut] = loop

                try:
                    await fut
                except BaseException:
                    with self._lock:
                        was_present = self._discard_waiter(self._getters, fut)
                        if not was_present:
                            self._wakeup_next(self._getters)
                    raise

    def _register_select_watcher(
        self, loop: asyncio.AbstractEventLoop, arbiter_fut: asyncio.Future[Any]
    ) -> Any | None:
        if self._use_raw:
            try:
                registered = self._inner.register_select_watcher(loop, arbiter_fut, self)
                if registered:
                    return arbiter_fut
                return None
            except RuntimeError:
                return None
        return super()._register_select_watcher(loop, arbiter_fut)

    def _unregister_select_watcher(self, token: Any) -> None:
        if self._use_raw:
            if token is not None:
                self._inner.unregister_select_watcher(token)
        else:
            super()._unregister_select_watcher(token)

    def _forward_select_wakeup(self) -> None:
        if self._use_raw:
            self._inner.forward_select_wakeup()
        else:
            with self._lock:
                self._wakeup_notifiers(all_notifiers=False)

    @property
    def _getters(self) -> Any:
        if self._use_raw:
            return _LenShim(self._inner.getters_len)
        return super()._getters

    @property
    def _putters(self) -> Any:
        if self._use_raw:
            return _LenShim(self._inner.putters_len)
        return super()._putters

    @property
    def _notifiers(self) -> Any:
        if self._use_raw:
            return _LenShim(self._inner.notifiers_len)
        return super()._notifiers


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

    num_channels = len(channels)
    while True:
        # Phase 1: Fast Probe (try_recv) with uniform lock-free pseudo-random start
        start_idx = (time.perf_counter_ns() ^ id(loop)) % num_channels if num_channels > 1 else 0
        for i in range(num_channels):
            ch = channels[(start_idx + i) % num_channels]
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
            start_reg = (
                (time.perf_counter_ns() ^ id(loop)) % num_channels if num_channels > 1 else 0
            )
            for i in range(num_channels):
                ch = channels[(start_reg + i) % num_channels]
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


class _TrackedCoroutine(Coroutine[Any, Any, Any]):
    """Coroutine wrapper for AsyncWaitGroup.track that guards against counter leaks."""

    __slots__ = ("_gen", "_stepped", "_target", "_wg")

    def __init__(self, target: Any, wg: AsyncWaitGroup) -> None:
        self._target = target
        self._wg = wg
        self._stepped = False
        self._gen: Generator[Any, None, Any] | None = None

    def _ensure_gen(self) -> Generator[Any, None, Any]:
        if self._gen is None:
            self._gen = self._run().__await__()
        return self._gen

    async def _run(self) -> Any:
        try:
            if asyncio.iscoroutine(self._target) or asyncio.isfuture(self._target):
                return await self._target
            elif callable(self._target):
                res = self._target()
                if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                    return await res
                return res
            return self._target
        finally:
            self._wg.done()

    def send(self, value: Any) -> Any:
        self._stepped = True
        return self._ensure_gen().send(value)

    def throw(self, *args: Any, **kwargs: Any) -> Any:
        self._stepped = True
        return self._ensure_gen().throw(*args, **kwargs)

    def close(self) -> None:
        if not self._stepped:
            self._stepped = True
            try:
                self._wg.done()
            except Exception:
                pass
        if self._gen is not None:
            self._gen.close()
        if asyncio.iscoroutine(self._target):
            self._target.close()

    def __await__(self) -> Generator[Any, None, Any]:
        return self._ensure_gen()

    def __del__(self) -> None:
        if not self._stepped:
            self._stepped = True
            try:
                self._wg.done()
            except Exception:
                pass
            if self._gen is not None:
                self._gen.close()
            if hasattr(self, "_target") and asyncio.iscoroutine(self._target):
                self._target.close()


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

    @asynccontextmanager
    async def holding(self) -> AsyncGenerator[None]:
        """Asynchronous context manager that increments counter on enter and decrements on exit.

        Guarantees structured RAII tracking without counter leaks::

            async with wg.holding():
                await do_work()
        """
        self.add(1)
        try:
            yield
        finally:
            self.done()

    def wrap(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap a callable to increment the counter upon invocation and decrement on finish.

        Usage with :meth:`TaskGroup.start_soon`::

            tg.start_soon(wg.wrap(worker), "arg1")
        """

        async def _wrapped(*args: Any, **kwargs: Any) -> Any:
            self.add(1)
            try:
                res = fn(*args, **kwargs)
                if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                    return await res
                return res
            finally:
                self.done()

        return _wrapped

    def track(self, coro: Any) -> Any:
        """Wrap a coroutine or callable, incrementing the counter immediately
        (happens-before) and decrementing it when execution finishes (in ``finally``).

        If the returned coroutine is discarded or closed without ever being awaited,
        the internal counter is safely decremented to prevent counter leak.

        :param coro: A coroutine object, Future, or callable to wrap and track.
        :returns: An awaitable that decrements the counter upon completion.
        """
        self.add(1)
        return _TrackedCoroutine(coro, self)

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
        self._waiters: collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop] = (
            collections.OrderedDict()
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
                self._waiters[fut] = loop

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
