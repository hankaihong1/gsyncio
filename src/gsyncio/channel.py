"""Pure Python asynchronous channel implementation."""

import asyncio
import collections
from typing import Any

from gsyncio._channel_base import _CHANNEL_CLOSED_MSG, _BaseChannel
from gsyncio._channel_wrappers import ReceiveChannel, SendChannel
from gsyncio.exceptions import ChannelClosedError, WouldBlock


class AsyncChannel(_BaseChannel):
    """Pure-Python cross-thread async channel.

    This channel provides bounded or unbounded queueing between coroutines
    running across different OS threads and event loops.

    :param maxsize:
        Maximum number of items the channel can hold. Defaults to 0 (unbounded).
    :type maxsize: int

    """

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__()
        self.maxsize = maxsize
        self._queue: collections.deque[Any] = collections.deque()
        self._closed = False

    def __repr__(self) -> str:
        return f"<AsyncChannel maxsize={self.maxsize} is_closed={self.is_closed}>"

    @property
    def is_closed(self) -> bool:
        """Return whether the channel is closed.

        :returns: ``True`` if closed, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        with self._lock:
            return self._closed

    def try_send(self, item: Any) -> bool:
        """Non-blocking send. Returns True if item was enqueued, False if full.

        :param item: The object to send.

        :returns: ``True`` if the item was sent, ``False`` if the channel is full.
        :rtype: :class:`bool`

        :raises ChannelClosedError: If the channel is closed.

        """
        with self._lock:
            if self._closed:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG)
            if self.maxsize > 0 and len(self._queue) >= self.maxsize:
                return False
            self._queue.append(item)
            self._wakeup_next(self._getters)
            return True

    def try_recv(self) -> Any:
        """Non-blocking recv. Returns an item or raises :class:`WouldBlock`.

        :returns: The received item.

        :raises WouldBlock: If the channel is empty.
        :raises ChannelClosedError: If the channel is closed and empty.

        """
        with self._lock:
            if self._closed and not self._queue:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG)
            if not self._queue:
                raise WouldBlock("Channel is empty")
            item = self._queue.popleft()
            self._wakeup_next(self._putters)
            return item

    def qsize(self) -> int:
        """Return the number of items currently buffered in the channel.

        :returns: Number of buffered items.
        :rtype: :class:`int`

        """
        with self._lock:
            return len(self._queue)

    def split(self) -> tuple[SendChannel, ReceiveChannel]:
        """Split the channel into send-only and receive-only halves.

        :returns: A tuple of ``(SendChannel, ReceiveChannel)`` that share the
                  same underlying channel.
        :rtype: :class:`tuple`

        """
        return SendChannel(self), ReceiveChannel(self)

    def close(self) -> None:
        """Close the channel.

        Wakes up all pending senders and receivers with :class:`ChannelClosedError`.

        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._close_waiters()

    async def send(self, item: Any) -> None:
        """Send an item into the channel.

        If the channel is full, this method suspends until a slot opens.

        :param item:
            The object to send.

        :raises ChannelClosedError:
            If the channel is closed.

        """
        await self._wait_and_send(item, self._try_send)

    def _try_send(self, item: Any) -> bool:
        """Enqueue ``item`` while the lock is held; True if queued, False if full.

        Raises :class:`ChannelClosedError` when the channel is closed. Used by
        :meth:`send` (via :meth:`_BaseChannel._wait_and_send`) as its
        ``try_fn``.
        """
        if self._closed:
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG)
        if self.maxsize > 0 and len(self._queue) >= self.maxsize:
            return False
        self._queue.append(item)
        return True

    async def _recv_impl(self) -> Any:
        loop = asyncio.get_running_loop()
        while True:
            with self._lock:
                if self._queue:
                    item = self._queue.popleft()
                    self._wakeup_next(self._putters)
                    return item
                if self._closed:
                    raise ChannelClosedError(_CHANNEL_CLOSED_MSG)
                fut: asyncio.Future[Any] = loop.create_future()
                self._getters.append((loop, fut))

            try:
                await fut
            except BaseException:
                with self._lock:
                    self._discard_waiter(self._getters, fut)
                raise
