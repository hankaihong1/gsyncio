"""Read-only / write-only channel halves for split channels."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any


class SendChannel:
    """Write-only half of a split channel.

    Provides only send operations; the underlying channel is closed when
    :meth:`close` is called on the send side.
    """

    def __init__(self, channel: Any) -> None:
        self._ch = channel

    def __repr__(self) -> str:
        return f"<SendChannel channel={self._ch!r}>"

    async def send(self, item: Any) -> None:
        """Send an item into the channel (async, may block)."""
        await self._ch.send(item)

    def try_send(self, item: Any) -> bool:
        """Non-blocking send. Returns True if sent, False if channel full."""
        return bool(self._ch.try_send(item))

    def close(self) -> None:
        """Close the channel (send side only)."""
        self._ch.close()


class ReceiveChannel:
    """Read-only half of a split channel.

    Provides only recv operations; :meth:`close` is a no-op — the send side
    is responsible for closing the underlying channel.
    """

    def __init__(self, channel: Any) -> None:
        self._ch = channel

    def __repr__(self) -> str:
        return f"<ReceiveChannel channel={self._ch!r}>"

    async def recv(self, timeout: float | None = None) -> Any:
        """Receive an item from the channel (async, may block)."""
        return await self._ch.recv(timeout=timeout)

    def try_recv(self) -> Any:
        """Non-blocking recv. Returns an item or raises WouldBlock."""
        return self._ch.try_recv()

    def qsize(self) -> int:
        """Number of items currently buffered in the channel."""
        return int(self._ch.qsize())

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        return await self._ch.__anext__()

    def close(self) -> None:
        """No-op — the send side is responsible for closing."""
        pass
