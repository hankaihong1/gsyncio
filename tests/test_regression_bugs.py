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
from gsyncio.channel import AsyncChannel
from gsyncio.pool import EventLoopThreadPool
from gsyncio.primitives import AsyncWaitGroup

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
    """Bug B (send): AsyncChannel leaks waiter futures in _putters.

    In channel.py:107-114, send() cleanup uses ``except Exception`` which
    does NOT catch ``asyncio.CancelledError`` (BaseException in Python 3.8+).
    When a blocked send is cancelled by asyncio.wait_for(timeout), the
    CancelledError escapes past the cleanup and the waiter future is never
    removed from _putters.

    Expected (fixed): after timeout/cancellation, _putters should be empty
    (waiter cleaned up).  This test FAILS against current code because the
    leaked future remains in _putters.
    """
    ch = AsyncChannel(maxsize=1)
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
    """Bug B (recv): AsyncChannel leaks getter futures in _getters.

    Same root cause as send leak (channel.py:151-157) — ``except Exception``
    misses CancelledError when a blocked recv is cancelled by wait_for.

    Expected (fixed): after timeout/cancellation, _getters should be empty.
    This test FAILS against current code because the leaked future remains.
    """
    ch = AsyncChannel()  # Empty channel — recv() will block.

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

    In primitives.py:217-230, select_channel creates _read_one tasks that block
    on ch.recv(). When the timeout expires, pending tasks are cancelled but
    without the ``except BaseException`` fix in channel.py:124, the
    CancelledError escapes cleanup and waiter futures remain in _getters.

    Expected (fixed): after select_channel timeout, all channels' _getters
    should be empty (cancelled waiters cleaned up by except BaseException).
    This test FAILS against the old code that uses ``except Exception``.
    """
    ch1 = AsyncChannel()
    ch2 = AsyncChannel()

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
