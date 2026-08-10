"""Regression tests for known concurrency bugs — all MUST FAIL against current unfixed code.

These tests assert the EXPECTED (fixed) behavior for each bug.
Against current unfixed code they will FAIL, proving the bugs exist.
After the fixes are applied, these same tests will PASS.
"""

import asyncio
import threading

import pytest

pytest.importorskip("gsyncio")

import gsyncio
from gsyncio.pool import EventLoopThreadPool
from gsyncio.primitives import AsyncWaitGroup, FastChannel

# ── Bug A: Hang when Rust extension is missing ───────────────────────────


@pytest.mark.asyncio
async def test_regression_bug_a_fallback_error():
    """Bug A: EventLoopThreadPool hangs forever when _gsyncio_core is missing.

    When the Rust native extension is unavailable, NativeWorkerPool is set to
    None (pool.py:13-22). The _worker_dispatcher (pool.py:168-170) immediately
    breaks its dispatch loop, but worker threads keep running idle.
    submit() (pool.py:443-444) skips pushing tasks when _native_pool is None,
    so the returned Future is never resolved → infinite hang.

    Expected (fixed): either raise a clear RuntimeError on init/start, or
    fall back to a pure-Python dispatcher so tasks complete without hanging.

    This test FAILS against current code because the pool hangs instead of
    completing or raising an error.
    """
    import gsyncio.pool

    # Simulate missing Rust extension
    gsyncio.pool.NativeWorkerPool = None

    pool = EventLoopThreadPool(num_threads=1)
    try:
        with pytest.raises(RuntimeError, match="not installed"):
            await pool.start()
    finally:
        await pool.close()
        # Restore NativeWorkerPool for other tests
        try:
            from gsyncio._gsyncio_core import NativeWorkerPool as _restore  # noqa: N813
        except ImportError:
            _restore = None
        gsyncio.pool.NativeWorkerPool = _restore


# ── Bug B: Channel waiter future leak on timeout/cancellation ────────────


@pytest.mark.asyncio
async def test_regression_bug_b_channel_leak_send():
    """Bug B (send): FastChannel must not leak waiter futures in _putters.

    Historical root cause: channel send cleanup used ``except Exception``,
    which does NOT catch ``asyncio.CancelledError`` (BaseException in 3.8+).
    When a blocked send is cancelled by asyncio.wait_for(timeout), the
    CancelledError escaped past the cleanup and the waiter future was never
    removed from _putters.  The shared _BaseChannel._wait_and_send now uses
    ``except BaseException`` — this test pins that behavior on FastChannel.

    Expected (fixed): after timeout/cancellation, _putters should be empty.
    """
    ch = FastChannel(maxsize=1)
    await ch.send("first")  # Fill the bounded channel

    # Block on a send that can never complete (channel full, no receiver).
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ch.send("second"), timeout=0.01)

    assert len(ch._putters) == 0, (
        f"Bug B (send): channel leaked {len(ch._putters)} waiter(s) in _putters "
        "— CancelledError escaped except Exception cleanup"
    )


@pytest.mark.asyncio
async def test_regression_bug_b_channel_leak_recv():
    """Bug B (recv): FastChannel must not leak getter futures in _getters.

    Same root cause as the send leak — ``except Exception`` missed
    CancelledError when a blocked recv is cancelled by wait_for.

    Expected (fixed): after timeout/cancellation, _getters should be empty.
    """
    ch = FastChannel()  # Empty channel — recv() will block.

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ch.recv(), timeout=0.01)

    assert len(ch._getters) == 0, (
        f"Bug B (recv): channel leaked {len(ch._getters)} waiter(s) in _getters "
        "— CancelledError escaped except Exception cleanup"
    )


# ── Bug C: RuntimeError when submitting from a non-asyncio thread ─────────


@pytest.mark.asyncio
async def test_regression_bug_c_non_asyncio_submit():
    """Bug C: pool.submit() raises informative RuntimeError from a raw thread.

    In pool.py:403-410, submit() calls ``asyncio.Future()`` when no running
    event loop is detected. In Python 3.10+, ``asyncio.Future()`` raises
    ``RuntimeError("There is no current event loop")`` from a non-asyncio
    thread.

    Expected (fixed): submit() raises an informative RuntimeError:
    "submit() must be called from a thread with a running asyncio event loop"
    This test PASSES because the fix replaces the cryptic ``asyncio.Future()``
    error with a clear, actionable message.
    """
    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()

    errors: list[Exception] = []

    def call_submit() -> None:
        try:
            pool.submit(asyncio.sleep, 0.01)
        except RuntimeError as e:
            errors.append(e)

    t = threading.Thread(target=call_submit)
    t.start()
    t.join()

    assert len(errors) == 1, f"Bug C: expected 1 RuntimeError, got {len(errors)} error(s)"
    assert str(errors[0]) == (
        "submit() must be called from a thread with a running asyncio event loop"
    )

    await pool.close()


# ── Bug E: WaitGroup counter underflow ────────────────────────────────────


def test_regression_bug_e_waitgroup_underflow():
    """Bug E: AsyncWaitGroup.done() silently underflows counter to usize::MAX.

    In lib.rs:224-233, RawAsyncWaitGroup.done() calls ``fetch_sub(1)`` without
    checking whether counter is zero. When counter == 0, unsigned integer
    underflow wraps to ``usize::MAX`` with no error.

    Expected (fixed): done() should raise RuntimeError/ValueError when
    counter is already zero (overdone).
    This test FAILS against current code because no error is raised.
    """
    wg = AsyncWaitGroup()
    with pytest.raises((RuntimeError, ValueError), match="counter"):
        wg.done()  # Should prevent underflow by raising an error


# ── Bug D: select_channel timeout leaks waiter futures ─────────────────────


@pytest.mark.asyncio
async def test_regression_bug_d_select_channel_timeout_leak():
    """Bug D: select_channel leaks waiter futures in channel _getters after timeout.

    Historical root cause: select_channel's _read_one tasks blocked on
    ch.recv(); on timeout the pending tasks were cancelled but without an
    ``except BaseException`` cleanup the CancelledError escaped and waiter
    futures remained in _getters.  Pinned here on FastChannel.

    Expected (fixed): after select_channel timeout, all channels' _getters
    should be empty (cancelled waiters cleaned up by except BaseException).
    """
    ch1 = FastChannel()
    ch2 = FastChannel()

    with pytest.raises(gsyncio.TimeoutError):
        await gsyncio.select_channel(ch1, ch2, timeout=0.01)

    # Yield to the event loop so cancelled tasks run their cleanup
    await asyncio.sleep(0)

    assert len(ch1._getters) == 0, (
        f"Bug D (select_channel): ch1 leaked {len(ch1._getters)} waiter(s) "
        "in _getters — CancelledError escaped cleanup"
    )
    assert len(ch2._getters) == 0, (
        f"Bug D (select_channel): ch2 leaked {len(ch2._getters)} waiter(s) "
        "in _getters — CancelledError escaped cleanup"
    )
