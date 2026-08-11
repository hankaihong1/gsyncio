"""Concurrency primitives and synchronization tools."""

import asyncio
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
from gsyncio._taskgroup import TaskGroup
from gsyncio.exceptions import ChannelClosedError, TimeoutError, WouldBlock


class _FastChannelProtocol(Protocol):
    """Protocol for the Rust FastChannel class."""

    def __init__(self, maxsize: int) -> None: ...
    def try_send(self, item: Any) -> bool: ...
    def try_recv(self) -> Any: ...
    def is_closed(self) -> bool: ...
    def close(self) -> None: ...
    def qsize(self) -> int: ...


class _WaitGroupProtocol(Protocol):
    """Protocol for the Rust RawAsyncWaitGroup class."""

    def __init__(self) -> None: ...
    def add(self, delta: int) -> None: ...
    def done(self) -> Any: ...
    def register_waiter(self, waiter: Any) -> bool: ...


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
        self._inner = _RustFastChannel(maxsize)

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
                    self._discard_waiter(self._getters, fut)
                raise


_UNSET: Any = object()


async def select_channel(
    *channels: Any, timeout: float | None = None, default: Any = _UNSET
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

    if default is not _UNSET:
        for ch in channels:
            try:
                return ch, ch.try_recv()
            except (ChannelClosedError, WouldBlock):
                continue
        return default

    result: list[tuple[Any, Any]] = []
    _tg: TaskGroup | None = None

    async def _notify_one(ch: Any, ready: list[Any]) -> None:
        """Report channel readiness WITHOUT consuming — only the winner consumes.

        Loops on spurious/raced wakeups (a concurrent consumer may steal the
        item between the send and this task running).
        """
        while True:
            loop = asyncio.get_running_loop()
            try:
                event = ch._register_notifier(loop)
            except ChannelClosedError:
                # WHY: a closed-and-empty channel can never become ready.
                # Retire this notifier silently — _select decides, once the
                # group unwinds, whether an open channel remains to wait on
                # or every channel is closed and select must raise (R3
                # FIX-19).  Raising here would abort the whole select even
                # when another channel is still usable.
                return
            if event is None:
                break  # already non-empty — report ready
            try:
                await event.wait()
            except asyncio.CancelledError:
                ch._discard_notifier(loop, event)
                raise
            break
        ready.append(ch)
        # WHY: TaskGroup only exits early when a child fails. Raising CancelledError
        # is the soft failure that cancels the sibling notifiers and unwinds the
        # group the moment the first channel reports; a normal return would make
        # the group wait for every channel to produce data.
        raise asyncio.CancelledError()

    async def _select() -> None:
        nonlocal _tg
        while not result:
            ready: list[Any] = []
            try:
                async with TaskGroup() as tg:
                    _tg = tg
                    for ch in channels:
                        tg.start_soon(_notify_one, ch, ready)
            except BaseExceptionGroup as eg:
                for exc in eg.exceptions:
                    if not isinstance(exc, asyncio.CancelledError):
                        raise
            except asyncio.CancelledError:
                cur = asyncio.current_task()
                if cur is not None and cur.cancelling() > 0:
                    raise  # the select task itself was cancelled — exit
                # Soft-failure signal from the first reporting notifier —
                # unwind and re-arbitrate.
                pass
            finally:
                _tg = None

            # Arbitration: only the winner consumes.  The notifiers never
            # consumed, so items on non-winner channels stay buffered and can
            # be received later (BUG-4).
            for ch in ready:
                try:
                    val = ch.try_recv()
                    result.append((ch, val))
                    return
                except (ChannelClosedError, WouldBlock):
                    # A concurrent consumer stole the item after readiness was
                    # reported — try the next ready channel.
                    continue
            # Every reported channel was stolen — re-register and wait again.
            # A *normal* group exit (no exception) means every notifier
            # retired because its channel was closed-and-empty at
            # registration time.  If ANY channel is still open, its notifier
            # is still parked and the loop keeps waiting for it — so reaching
            # this point with nothing ready and every channel closed means
            # select would spin forever: surface it (R3 FIX-19).
            if all(ch.is_closed and ch.qsize() == 0 for ch in channels):
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG)
            await asyncio.sleep(0)

    select_task = asyncio.create_task(_select())

    if timeout is None:
        await select_task
        return result[0]

    done, _pending = await asyncio.wait(
        [select_task],
        timeout=timeout,
    )

    if done:
        await select_task
        return result[0]

    if _tg is not None:
        _tg.cancel_all()

    select_task.cancel()
    try:
        await select_task
    except (BaseExceptionGroup, asyncio.CancelledError):
        pass

    if result:
        # WHY: the winner was decided in the same instant the timeout fired —
        # the item was already consumed by the arbitration, so surface it
        # instead of losing it (W4).
        return result[0]

    raise TimeoutError("select_channel timed out")


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
        self._inner.add(delta)

    def done(self) -> None:
        """Decrement the counter by 1. Wakes waiters if counter reaches 0."""
        waiters = self._inner.done()
        if waiters:
            _wake_all(waiters)

    async def wait(self) -> None:
        """Suspend execution until the WaitGroup counter becomes 0.

        .. note::
            If the waiting task is cancelled, its entry stays in the Rust
            waiter list until the counter next reaches 0 (FIX-7): the entry
            holds a cancelled future and is simply skipped at wakeup, so
            cancellation is safe — but a group that never reaches 0 keeps
            the (inert) entry, and a repeated cancelled wait accumulates
            entries until the next ``done()`` to zero drains them.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        already_done = self._inner.register_waiter((loop, fut))
        if already_done:
            return
        await fut


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
            self._exc = e
            raise
        finally:
            with self._thread_lock:
                self._done = True
                _wake_all(self._waiters)

        return self._result
