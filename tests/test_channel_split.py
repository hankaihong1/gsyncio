"""Tests for channel split (SendChannel/ReceiveChannel) and non-blocking select_channel."""

import pytest

from gsyncio import (
    AsyncChannel,
    FastChannel,
    ReceiveChannel,
    SendChannel,
    select_channel,
)
from gsyncio.exceptions import ChannelClosedError

# ── FastChannel split ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fast_channel_split_send_recv():
    """Split FastChannel: send on SendChannel, recv on ReceiveChannel."""
    ch = FastChannel()
    sender, receiver = ch.split()
    assert isinstance(sender, SendChannel)
    assert isinstance(receiver, ReceiveChannel)

    await sender.send("hello")
    val = await receiver.recv()
    assert val == "hello"


@pytest.mark.asyncio
async def test_async_channel_split_send_recv():
    """Split AsyncChannel: send on SendChannel, recv on ReceiveChannel."""
    ch = AsyncChannel()
    sender, receiver = ch.split()
    assert isinstance(sender, SendChannel)
    assert isinstance(receiver, ReceiveChannel)

    await sender.send("world")
    val = await receiver.recv()
    assert val == "world"


@pytest.mark.asyncio
async def test_split_close_send_side():
    """Send items, close SendChannel, drain remaining, then ChannelClosedError."""
    ch = FastChannel(maxsize=10)
    sender, receiver = ch.split()

    await sender.send(1)
    await sender.send(2)
    await sender.send(3)
    sender.close()

    # Drain remaining items
    assert await receiver.recv() == 1
    assert await receiver.recv() == 2
    assert await receiver.recv() == 3

    # Now the channel is closed and empty
    with pytest.raises(ChannelClosedError):
        await receiver.recv()


@pytest.mark.asyncio
async def test_split_receive_channel_close_is_noop():
    """ReceiveChannel.close() should be a no-op — channel stays open."""
    ch = FastChannel()
    sender, receiver = ch.split()

    await sender.send("still open after receiver close")
    receiver.close()  # no-op

    val = await receiver.recv()
    assert val == "still open after receiver close"


@pytest.mark.asyncio
async def test_select_channel_default_empty():
    """select_channel with default on empty channels returns default immediately."""
    ch1 = FastChannel()
    ch2 = FastChannel()

    result = await select_channel(ch1, ch2, default="nothing")
    assert result == "nothing"


@pytest.mark.asyncio
async def test_select_channel_default_some_ready():
    """select_channel with default — one channel has data, returns it."""
    ch1 = FastChannel()
    ch2 = FastChannel()

    ch2.try_send("ready")

    selected, val = await select_channel(ch1, ch2, default="nothing")
    assert selected is ch2
    assert val == "ready"


@pytest.mark.asyncio
async def test_select_channel_default_closed_channel():
    """select_channel with default skips closed channels gracefully."""
    ch1 = FastChannel()
    ch2 = FastChannel()
    ch1.close()

    result = await select_channel(ch1, ch2, default="fallback")
    assert result == "fallback"
