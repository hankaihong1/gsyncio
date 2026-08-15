import asyncio
import threading
import time

import pytest

from gsyncio import (
    EventLoopThreadPool,
    TimeoutError,
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


# ── U5 FIX-19: select_channel closed-channel semantics ─────────────────────


@pytest.mark.asyncio
async def test_select_all_closed_raises() -> None:
    """R3-FIX-19 (probe R3-B): selecting on channels that are closed AND empty
    must raise ChannelClosedError instead of hanging forever (pre-fix: the
    notifier registered successfully and nobody ever woke it)."""
    ch1 = FastChannel()
    ch2 = FastChannel()
    ch1.close()
    ch2.close()
    with pytest.raises(ChannelClosedError):
        await asyncio.wait_for(select_channel(ch1, ch2), timeout=0.5)


@pytest.mark.asyncio
async def test_select_closed_with_data_returns_data() -> None:
    """FIX-19: a closed channel that still holds buffered items reports ready
    and yields its data — closing must not destroy buffered values."""
    ch1 = FastChannel()
    ch2 = FastChannel()
    await ch1.send("value")
    ch1.close()
    ch2.close()
    ch, val = await asyncio.wait_for(select_channel(ch1, ch2), timeout=1.0)
    assert (ch, val) == (ch1, "value")


@pytest.mark.asyncio
async def test_select_one_closed_one_open_waits() -> None:
    """FIX-19: with one closed-empty channel and one open channel, select must
    ignore the closed one and wait for the open channel (no spurious raise)."""
    ch1 = FastChannel()
    ch2 = FastChannel()
    ch1.close()
    task = asyncio.create_task(select_channel(ch1, ch2))
    await asyncio.sleep(0.05)
    assert not task.done()
    await ch2.send("data")
    ch, val = await asyncio.wait_for(task, timeout=1.0)
    assert (ch, val) == (ch2, "data")


@pytest.mark.asyncio
async def test_select_close_race() -> None:
    """FIX-19: close racing with select must never hang — either the buffered
    item wins or the select raises ChannelClosedError."""
    for _ in range(20):
        ch1 = FastChannel()
        ch2 = FastChannel()
        await ch1.send("x")
        task = asyncio.create_task(select_channel(ch1, ch2))
        await asyncio.sleep(0)
        ch1.close()
        ch2.close()
        try:
            ch, val = await asyncio.wait_for(task, timeout=1.0)
            assert ch in (ch1, ch2)
            assert val == "x"
        except ChannelClosedError:
            pass


@pytest.mark.asyncio
async def test_putter_cancel_forwards_wakeup_to_next_putter() -> None:
    """R10 P1: a cancelled putter whose entry was already popped must forward
    the wakeup — the freed slot would otherwise idle while later putters
    sleep forever (pre-fix: the second putter hangs)."""
    ch = FastChannel(1)
    await ch.send("a")  # fill the buffer

    puts_done: list[str] = []
    a_cancelled = asyncio.Event()

    async def put_b() -> None:
        try:
            await ch.send("b")
            puts_done.append("b")
        except asyncio.CancelledError:
            a_cancelled.set()
            raise

    async def put_c() -> None:
        await ch.send("c")
        puts_done.append("c")

    a = asyncio.create_task(put_b())
    await asyncio.sleep(0)  # A registers in _putters
    c = asyncio.create_task(put_c())
    await asyncio.sleep(0)  # C registers in _putters
    assert len(ch._putters) == 2

    # Consumer takes the buffered item; this pops putter A's entry and
    # queues fut.set_result for A.
    assert ch.try_recv() == "a"

    # Cancel A synchronously, before the set_result callback runs — the
    # wakeup is then consumed by a cancelled putter (lost-wakeup window).
    a.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert a_cancelled.is_set()

    # The freed slot must reach C: it eventually sends "c".
    await asyncio.wait_for(c, timeout=1.0)
    assert puts_done == ["c"]


@pytest.mark.asyncio
async def test_getter_cancel_forwards_wakeup_to_next_getter() -> None:
    """R10 P1: a cancelled getter whose entry was already popped must forward
    the wakeup — the buffered item would otherwise sit unconsumed while
    later getters sleep forever (pre-fix: the second getter hangs)."""
    ch = FastChannel(0)  # unbounded
    recv_done: list[str] = []

    async def recv_a() -> None:
        try:
            recv_done.append(await ch.recv())
        except asyncio.CancelledError:
            raise

    async def recv_b() -> None:
        recv_done.append(await ch.recv())

    a = asyncio.create_task(recv_a())
    await asyncio.sleep(0)  # A registers in _getters
    b = asyncio.create_task(recv_b())
    await asyncio.sleep(0)  # B registers in _getters
    assert len(ch._getters) == 2

    # Sender puts an item; this pops getter A's entry and queues
    # set_result for A.
    assert ch.try_send("x") is True

    # Cancel A synchronously — the wakeup is consumed by a cancelled getter.
    a.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # The buffered item must still reach B.
    await asyncio.wait_for(b, timeout=1.0)
    assert recv_done == ["x"]


def test_fastchannel_empty_full_maxsize() -> None:
    """FastChannel should correctly report empty(), full(), and maxsize."""
    ch_unbounded = FastChannel(0)
    assert ch_unbounded.maxsize == 0
    assert ch_unbounded.empty() is True
    assert ch_unbounded.full() is False

    ch_unbounded.try_send(1)
    assert ch_unbounded.empty() is False
    assert ch_unbounded.full() is False

    ch_bounded = FastChannel(2)
    assert ch_bounded.maxsize == 2
    assert ch_bounded.empty() is True
    assert ch_bounded.full() is False

    ch_bounded.try_send("a")
    assert ch_bounded.empty() is False
    assert ch_bounded.full() is False

    ch_bounded.try_send("b")
    assert ch_bounded.empty() is False
    assert ch_bounded.full() is True


# ---------------------------------------------------------------------------
# Concurrency audit & semantic refactoring tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_channel_timeout_zero_no_false_timeout() -> None:
    """select_channel with ready data and timeout=0 must return ready channel immediately."""
    ch1 = FastChannel()
    ch2 = FastChannel()

    ch1.try_send("hello")

    # timeout=0 must immediately probe ch1 without false TimeoutError
    ch, val = await select_channel(ch1, ch2, timeout=0)
    assert ch is ch1
    assert val == "hello"


@pytest.mark.asyncio
async def test_select_channel_empty_timeout_zero() -> None:
    """select_channel on empty channels with timeout=0 raises TimeoutError."""
    ch1 = FastChannel()
    ch2 = FastChannel()

    with pytest.raises(TimeoutError):
        await select_channel(ch1, ch2, timeout=0)


@pytest.mark.asyncio
async def test_select_channel_deadline_contention_retry() -> None:
    """select_channel contention retry must deduct elapsed time against deadline."""
    ch1 = FastChannel()
    ch2 = FastChannel()

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        await select_channel(ch1, ch2, timeout=0.1)
    elapsed = time.monotonic() - start
    assert 0.08 <= elapsed < 0.5, f"Timeout took {elapsed}s, expected ~0.1s"


@pytest.mark.asyncio
async def test_select_channel_contention_iterative() -> None:
    """select_channel with contended consumers must not raise RecursionError."""
    ch1 = FastChannel()
    ch2 = FastChannel()

    async def producer() -> None:
        for i in range(50):
            await ch1.send(i)
            await ch2.send(i)

    async def consumer() -> list[int]:
        consumed: list[int] = []
        for _ in range(50):
            try:
                _, val = await select_channel(ch1, ch2, timeout=0.5)
                consumed.append(val)
            except TimeoutError:
                break
        return consumed

    prod_task = asyncio.create_task(producer())
    c1 = asyncio.create_task(consumer())
    c2 = asyncio.create_task(consumer())

    await prod_task
    r1, r2 = await asyncio.gather(c1, c2)
    assert len(r1) + len(r2) == 100


@pytest.mark.asyncio
async def test_select_channel_timeout_error_tuple_catch() -> None:
    """select_channel properly converts asyncio.TimeoutError and passes through custom exceptions."""
    ch1 = FastChannel()
    ch2 = FastChannel()

    # Normal timeout raises gsyncio.exceptions.TimeoutError
    with pytest.raises(TimeoutError, match="select_channel timed out"):
        await select_channel(ch1, ch2, timeout=0.05)
