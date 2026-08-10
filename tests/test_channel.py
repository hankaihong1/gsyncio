import asyncio
import threading

import pytest

from gsyncio import (
    EventLoopThreadPool,
    TimeoutError,
    open_channel,
    select_channel,
)
from gsyncio.exceptions import ChannelClosedError, GsyncioError, WouldBlock
from gsyncio.primitives import FastChannel
from gsyncio.testing import wait_all_tasks_blocked


@pytest.mark.asyncio
async def test_channel_valid_basic_send_recv():
    """Valid 1: Basic send and receive"""
    ch = FastChannel()
    await ch.send("hello")
    res = await ch.recv()
    assert res == "hello"


@pytest.mark.asyncio
async def test_channel_valid_multiple_items():
    """Valid 2: Sequential send and receive of multiple items"""
    ch = FastChannel()
    items = [1, 2, 3, "test", {"key": "val"}]
    for item in items:
        await ch.send(item)

    received = []
    for _ in range(len(items)):
        received.append(await ch.recv())
    assert received == items


@pytest.mark.asyncio
async def test_channel_valid_concurrent_producers_consumers():
    """Valid 3: Multi-coroutine concurrent send/receive data integrity"""
    ch = FastChannel()
    count = 50

    async def producer(n):
        await ch.send(n)

    async def consumer():
        return await ch.recv()

    producers = [producer(i) for i in range(count)]
    consumers = [consumer() for _ in range(count)]

    _, results = await asyncio.gather(asyncio.gather(*producers), asyncio.gather(*consumers))
    assert sorted(results) == list(range(count))


@pytest.mark.asyncio
async def test_channel_boundary_none_and_empty_payloads():
    """Boundary 1: None / empty bytes / empty data structure passing"""
    ch = FastChannel()
    payloads = [None, b"", {}, [], 0, False]
    for p in payloads:
        await ch.send(p)
        res = await ch.recv()
        assert res is p


@pytest.mark.asyncio
async def test_channel_boundary_capacity_limit():
    """Boundary 2: Capacity-limited (capacity=1) send and blocking unlock"""
    ch = FastChannel(maxsize=1)
    await ch.send("first")

    # The second send should suspend, waiting for consumption
    send_task = asyncio.create_task(ch.send("second"))
    await wait_all_tasks_blocked()
    assert not send_task.done()

    val1 = await ch.recv()
    assert val1 == "first"

    # After receiving, send_task should be able to complete
    await asyncio.wait_for(send_task, timeout=1.0)
    val2 = await ch.recv()
    assert val2 == "second"


@pytest.mark.asyncio
async def test_channel_boundary_recv_blocks_until_send():
    """Boundary 3: recv suspends on empty channel, unblocks after send"""
    ch = FastChannel()

    recv_task = asyncio.create_task(ch.recv())
    await asyncio.sleep(0.01)
    assert not recv_task.done()

    await ch.send("unblock")
    val = await asyncio.wait_for(recv_task, timeout=1.0)
    assert val == "unblock"


@pytest.mark.asyncio
async def test_channel_error_send_to_closed_channel():
    """Error 1: Sending data to a closed channel raises ChannelClosedError"""
    ch = FastChannel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        await ch.send("fail")


@pytest.mark.asyncio
async def test_channel_error_recv_from_closed_empty_channel():
    """Error 2: Receiving from a closed and empty channel raises ChannelClosedError"""
    ch = FastChannel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        await ch.recv()


@pytest.mark.asyncio
async def test_channel_error_recv_timeout():
    """Error 3: Timed recv raises asyncio.TimeoutError"""
    ch = FastChannel()
    with pytest.raises(asyncio.TimeoutError):
        await ch.recv(timeout=0.02)


def test_wouldblock_is_gsyncio_error():
    """WouldBlock should be a subclass of GsyncioError."""
    assert isinstance(WouldBlock(), GsyncioError)


def test_fast_channel_try_send_recv_roundtrip():
    """try_send + try_recv on FastChannel should round-trip a value."""
    ch = FastChannel()
    assert ch.try_send("hello")
    assert ch.try_recv() == "hello"


def test_fast_channel_try_recv_empty_raises_wouldblock():
    """try_recv on an empty FastChannel should raise WouldBlock."""
    ch = FastChannel()
    with pytest.raises(WouldBlock):
        ch.try_recv()


def test_fast_channel_qsize():
    """qsize() should reflect the number of buffered items."""
    ch = FastChannel()
    assert ch.qsize() == 0
    ch.try_send(1)
    assert ch.qsize() == 1
    ch.try_send(2)
    assert ch.qsize() == 2
    ch.try_recv()
    assert ch.qsize() == 1
    ch.try_recv()
    assert ch.qsize() == 0


def test_fast_channel_try_send_full_bounded():
    """try_send on a full bounded FastChannel should return False."""
    ch = FastChannel(maxsize=1)
    assert ch.try_send("first")
    assert not ch.try_send("second")


def test_fast_channel_try_recv_closed_raises():
    """try_recv on a closed empty FastChannel should raise ChannelClosedError."""
    ch = FastChannel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        ch.try_recv()


def test_fast_channel_try_send_closed_raises():
    """try_send on a closed FastChannel should raise ChannelClosedError."""
    ch = FastChannel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        ch.try_send("nope")


def test_async_channel_try_send_recv_roundtrip():
    """try_send + try_recv on FastChannel should round-trip a value."""
    ch = FastChannel()
    assert ch.try_send("world")
    assert ch.try_recv() == "world"


def test_async_channel_try_recv_empty_raises_wouldblock():
    """try_recv on an empty FastChannel should raise WouldBlock."""
    ch = FastChannel()
    with pytest.raises(WouldBlock):
        ch.try_recv()


def test_async_channel_qsize():
    """qsize() should reflect the number of buffered items."""
    ch = FastChannel()
    assert ch.qsize() == 0
    ch.try_send("a")
    assert ch.qsize() == 1
    ch.try_send("b")
    assert ch.qsize() == 2
    ch.try_recv()
    assert ch.qsize() == 1


def test_async_channel_try_send_full_bounded():
    """try_send on a full bounded FastChannel should return False."""
    ch = FastChannel(maxsize=1)
    assert ch.try_send("first")
    assert not ch.try_send("second")


def test_async_channel_try_recv_closed_raises():
    """try_recv on a closed empty FastChannel should raise ChannelClosedError."""
    ch = FastChannel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        ch.try_recv()


def test_async_channel_try_send_closed_raises():
    """try_send on a closed FastChannel should raise ChannelClosedError."""
    ch = FastChannel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        ch.try_send("nope")


@pytest.mark.asyncio
async def test_fast_channel_basic_send_recv():
    """Test Rust-based FastChannel basic send/receive"""
    ch = FastChannel()
    await ch.send("rust_fast_data")
    val = await ch.recv()
    assert val == "rust_fast_data"


@pytest.mark.asyncio
async def test_fast_channel_capacity_and_block():
    """Test FastChannel capacity-based blocking"""
    ch = FastChannel(maxsize=1)
    await ch.send(100)

    send_task = asyncio.create_task(ch.send(200))
    await wait_all_tasks_blocked()
    assert not send_task.done()

    val1 = await ch.recv()
    assert val1 == 100
    await asyncio.wait_for(send_task, timeout=1.0)
    val2 = await ch.recv()
    assert val2 == 200


@pytest.mark.asyncio
async def test_fast_channel_closed_error():
    """Test FastChannel raises exception on close"""
    ch = FastChannel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        await ch.send("error")


@pytest.mark.asyncio
async def test_select_channel_no_channels():
    """select_channel raises ValueError when no channels are provided (line 211)."""
    from gsyncio.primitives import select_channel

    with pytest.raises(ValueError, match="at least one channel"):
        await select_channel()


@pytest.mark.asyncio
async def test_channel_close_with_blocked_waiters():
    """Close FastChannel while sender and receiver are blocked — covers
    _channel_base.py _close_waiters putters path (46-48, branch 41->39)
    and primitives.py BaseException cleanup (124-129, 175, 182-186)."""
    # Blocked sender: fill bounded channel, start sender, close
    send_ch = FastChannel(maxsize=1)
    await send_ch.send("fill")

    async def blocked_sender():
        await send_ch.send("blocked")

    sender_task = asyncio.create_task(blocked_sender())
    await wait_all_tasks_blocked()
    send_ch.close()
    with pytest.raises(ChannelClosedError):
        await sender_task

    # Blocked receiver: start receiver on empty channel, close
    recv_ch = FastChannel()

    async def blocked_receiver():
        return await recv_ch.recv()

    recv_task = asyncio.create_task(blocked_receiver())
    await wait_all_tasks_blocked()
    recv_ch.close()
    with pytest.raises(ChannelClosedError):
        await recv_task


@pytest.mark.asyncio
async def test_open_channel_basic():
    """open_channel returns a FastChannel instance."""
    ch = await open_channel()
    assert isinstance(ch, FastChannel)


@pytest.mark.asyncio
async def test_open_channel_close_behavior():
    """Closing an open_channel raises ChannelClosedError on send/recv."""
    ch = await open_channel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        await ch.send("x")
    with pytest.raises(ChannelClosedError):
        await ch.recv()


@pytest.mark.asyncio
async def test_async_channel_timeout_and_errors():
    """Test FastChannel timeout and close edge paths"""
    ch = FastChannel(maxsize=1)
    await ch.send("item1")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ch.send("item2"), timeout=0.05)

    ch.close()
    ch.close()  # Second close is idempotent
    assert ch.is_closed

    with pytest.raises(ChannelClosedError):
        await ch.send("item3")

    # Retrieve existing data
    val = await ch.recv()
    assert val == "item1"

    with pytest.raises(ChannelClosedError):
        await ch.recv()


@pytest.mark.asyncio
async def test_fast_channel_timeout_and_errors():
    """Test FastChannel timeout and edge case error paths"""
    ch = FastChannel(maxsize=1)
    with pytest.raises((TimeoutError, asyncio.TimeoutError)):
        await ch.recv(timeout=0.05)

    await ch.send("item1")
    ch.close()
    assert ch.is_closed

    with pytest.raises(ChannelClosedError):
        await ch.send("item2")


@pytest.mark.asyncio
async def test_vulnerability_fast_channel_no_busy_wait():
    """Vulnerability 1: FastChannel should not busy-wait with sleep when no data is available, and should be properly woken up"""
    ch = FastChannel()

    async def consumer():
        return await ch.recv()

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.02)
    assert not task.done()

    await ch.send("unblocked_event")
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result == "unblocked_event"


@pytest.mark.asyncio
async def test_race_high_concurrency_channel_send_recv():
    """Race 2: Validate data integrity and zero race-condition data loss for 100 coroutines concurrently sending/receiving on Channel under a multi-threaded event loop pool"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        ch = FastChannel(maxsize=10)
        total_items = 200
        received_items = []
        lock = threading.Lock()

        async def producer(val):
            await ch.send(val)

        async def consumer():
            val = await ch.recv()
            with lock:
                received_items.append(val)

        # Dispatch 200 producers and 200 consumers running and waiting simultaneously across threads
        producers = [pool.submit(producer, i) for i in range(total_items)]
        consumers = [pool.submit(consumer) for _ in range(total_items)]

        await asyncio.gather(*producers, *consumers)

        assert len(received_items) == total_items
        assert sorted(received_items) == list(range(total_items))


@pytest.mark.asyncio
async def test_channel_async_for_iteration():
    """1. Go parity range ch: Validate async for item in ch loop consumption and graceful exit on close"""
    ch = FastChannel()

    async def producer():
        for i in range(3):
            await ch.send(i)
            await asyncio.sleep(0.01)
        ch.close()

    asyncio.create_task(producer())

    received = []
    async for item in ch:
        received.append(item)

    assert received == [0, 1, 2]


@pytest.mark.asyncio
async def test_fast_channel_async_for_iteration():
    """1b. Validate FastChannel async for loop consumption"""
    ch = FastChannel()

    async def producer():
        for item in ["a", "b", "c"]:
            await ch.send(item)
            await asyncio.sleep(0.01)
        ch.close()

    asyncio.create_task(producer())

    received = []
    async for item in ch:
        received.append(item)

    assert received == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_select_channel_multi_branch():
    """2. Go parity select: Validate select_channel listening on multiple channels simultaneously"""
    ch1 = FastChannel()
    ch2 = FastChannel()

    async def send_to_ch2():
        await asyncio.sleep(0.02)
        await ch2.send("from_ch2")

    asyncio.create_task(send_to_ch2())

    selected_ch, val = await select_channel(ch1, ch2, timeout=1.0)
    assert selected_ch is ch2
    assert val == "from_ch2"


# ---------------------------------------------------------------------------
# FIX-2 regression tests (BUG-2: PyO3 Option<Py<PyAny>> boundary loses None
# payloads) — 2026-08-10 audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fastchannel_none_payload() -> None:
    """BUG-2: FastChannel must transport None payloads end to end.

    Before the fix `send(None)` consumed the item but `recv()` saw the
    channel as empty and registered a waiter that nobody would wake —
    a permanent hang.
    """
    ch = FastChannel()
    await ch.send(None)
    # wait_for guards against the historical permanent hang.
    assert await asyncio.wait_for(ch.recv(), timeout=1.0) is None

    # try_send/try_recv must distinguish "None payload" from "empty".
    assert ch.try_send(None) is True
    assert ch.try_recv() is None  # None payload, NOT WouldBlock

    # The channel is now truly empty: WouldBlock must be raised.
    with pytest.raises(WouldBlock):
        ch.try_recv()


@pytest.mark.asyncio
async def test_fastchannel_none_after_regular_items() -> None:
    """None must not disturb the FIFO order of surrounding items."""
    ch = FastChannel()
    await ch.send(1)
    await ch.send(None)
    await ch.send(2)
    assert await ch.recv() == 1
    assert await ch.recv() is None
    assert await ch.recv() == 2


def test_pop_work_rejects_none() -> None:
    """FIX-2 hardening: pushing a None task into the native pool is a type error."""
    import gsyncio._gsyncio_core as core

    pool = core.NativeWorkerPool(2)
    try:
        with pytest.raises(TypeError):
            pool.push_global(None)
        with pytest.raises(TypeError):
            pool.push_local(0, None)
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# FIX-4 regression tests (BUG-4: select_channel must only consume the winner)
# — 2026-08-10 audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_no_item_loss_simultaneous() -> None:
    """BUG-4: both channels ready at once — the unselected item stays buffered."""
    ch1 = FastChannel()
    ch2 = FastChannel()
    await ch1.send("a")
    await ch2.send("b")

    selected_ch, val = await select_channel(ch1, ch2, timeout=1.0)

    # The loser's item must still be receivable; the winner's was consumed.
    if selected_ch is ch1:
        assert val == "a"
        assert await asyncio.wait_for(ch2.recv(), timeout=1.0) == "b"
    else:
        assert val == "b"
        assert await asyncio.wait_for(ch1.recv(), timeout=1.0) == "a"


@pytest.mark.asyncio
async def test_select_timeout_no_loss() -> None:
    """W4: an item sent at the timeout boundary must never vanish."""
    async def delayed_send(ch: FastChannel, delay: float, value: str) -> None:
        await asyncio.sleep(delay)
        await ch.send(value)

    for i in range(100):
        ch = FastChannel()
        sender = asyncio.create_task(delayed_send(ch, 0.005, f"v{i}"))
        consumed = 0
        try:
            await select_channel(ch, timeout=0.01)
            consumed = 1
        except TimeoutError:
            pass
        await sender
        # Accounting: exactly one item was sent — it was either consumed by
        # the select winner or is still buffered.  Zero means it was dropped.
        assert ch.qsize() + consumed == 1, (
            f"round {i}: item lost (qsize={ch.qsize()}, consumed={consumed})"
        )
