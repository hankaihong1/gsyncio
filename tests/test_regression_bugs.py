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


# ── R2 FIX-9: Lock re-entrancy + cancellation ownership theft ─────────────


@pytest.mark.asyncio
async def test_lock_reentrant_acquire_raises():
    """R2-FIX-9 (probe R2-A2): same-task re-acquire raises RuntimeError
    (asyncio.Lock parity) instead of silently queueing into a self-deadlock.

    Pre-fix: the inner acquire parks forever (deadlock); when an external
    timeout breaks it, the outer holder loses ownership (see the next test).
    """
    lock = gsyncio.Lock()
    async with lock:
        with pytest.raises(RuntimeError):
            async with lock:
                pass


@pytest.mark.asyncio
async def test_lock_reentrant_cancel_no_ownership_theft():
    """R2-FIX-9 (probe R2-A1): a cancelled re-entrant acquire must NOT hand
    the lock to a queued waiter while the outer holder is still inside its
    critical section — that violates mutual exclusion.

    Pre-fix log (probe): T2 entered its critical section while T1 was still
    inside, and T1's outer __aexit__ then raised "does not own the lock".
    """
    lock = gsyncio.Lock()
    t1_in_cs = {"v": False}

    async def t1():
        try:
            async with lock:
                t1_in_cs["v"] = True
                try:
                    async with asyncio.timeout(0.2):
                        async with lock:  # re-entrant acquire — must raise
                            pass
                except (TimeoutError, RuntimeError):
                    pass
                await asyncio.sleep(0.3)  # keep the outer critical section
                t1_in_cs["v"] = False
        except RuntimeError:
            pass  # pre-fix: outer __aexit__ loses ownership

    async def t2():
        async with lock:
            # Entering here means T1 has fully exited its critical section.
            assert t1_in_cs["v"] is False

    t1_task = asyncio.create_task(t1())
    await asyncio.sleep(0.05)  # T1 holds the lock and is inside the re-acquire
    t2_task = asyncio.create_task(t2())
    results = await asyncio.wait_for(
        asyncio.gather(t1_task, t2_task, return_exceptions=True), timeout=5
    )
    assert not any(isinstance(r, BaseException) for r in results), results


# ── U3 FIX-1 + FIX-10: AsyncRWMutex release shielding + nesting ────────────


@pytest.mark.asyncio
async def test_rwmutex_cancel_cleanup_leak():
    """R1-FIX-1 contract: a cancelled *holder* must complete its release path
    (readers back to 0, a queued writer admitted).  The finally block re-
    acquires the inner Lock, and that re-acquire must never be interrupted by
    a pending cancellation (R1 probe A observed readers stuck at 1 and the
    writer hanging forever).  On 3.14 the single-shot delivery semantics make
    the plain single-cancel path safe; the shield guards the residual-count
    paths (user shield restore leaving _must_cancel set).  This test pins the
    observable contract.
    """
    rw = gsyncio.AsyncRWMutex()
    entered = asyncio.Event()

    async def holder() -> None:
        async with rw.reader():
            entered.set()
            await asyncio.Event().wait()

    h = asyncio.create_task(holder())
    await entered.wait()
    w_cm = rw.writer()
    w = asyncio.create_task(w_cm.__aenter__())
    await asyncio.sleep(0.01)  # w queued (writer priority)

    h.cancel()
    with pytest.raises(asyncio.CancelledError):
        await h

    # Holder's release completed: readers back to 0, writer admitted.
    async with asyncio.timeout(1.0):
        await w
    await w_cm.__aexit__(None, None, None)
    assert rw._readers == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_rwmutex_nesting_rejected():
    """R2-FIX-10: reader→writer / writer→reader / writer→writer nesting
    raises RuntimeError instead of silently self-deadlocking; reader→reader
    re-entry stays legal."""
    rw = gsyncio.AsyncRWMutex()

    with pytest.raises(RuntimeError):
        async with rw.reader():
            async with asyncio.timeout(0.5):
                async with rw.writer():
                    pass  # pragma: no cover — pre-fix hangs until timeout

    with pytest.raises(RuntimeError):
        async with rw.writer():
            async with asyncio.timeout(0.5):
                async with rw.reader():
                    pass  # pragma: no cover

    with pytest.raises(RuntimeError):
        async with rw.writer():
            async with asyncio.timeout(0.5):
                async with rw.writer():
                    pass  # pragma: no cover

    # Re-entrant reads are allowed (shared lock).
    async with rw.reader(), rw.reader():
        pass


@pytest.mark.asyncio
async def test_rwmutex_double_reader_writer_still_rejected():
    """R5 修订 A: reader depth counting — after the *first* nested reader
    exits (still inside the second), writer() must still be rejected.  A
    plain set would drop the registration on the first exit and let the
    writer hang (pre-fix behavior: no detection at all → hangs)."""
    rw = gsyncio.AsyncRWMutex()
    async with rw.reader():
        async with rw.reader():
            pass  # first exit: depth 2 → 1, task still registered
        with pytest.raises(RuntimeError):
            async with asyncio.timeout(0.5):
                async with rw.writer():
                    pass  # pragma: no cover


@pytest.mark.asyncio
async def test_rwmutex_cancelled_writer_preserves_holder_state():
    """U3 contract: a *queued* writer cancelled while another writer holds
    the lock must not touch the holder's state.  Structurally the acquire
    phase throws before the outer ``try: yield`` is entered, so only the
    inner finally (pending_writers decrement) runs — this test pins that
    contract (and the explicit ``acquired`` guard in the release path) so a
    future restructure cannot let a cancelled queued writer flip _writer to
    False and admit readers while the holder is still inside."""
    rw = gsyncio.AsyncRWMutex()
    async with rw.writer():
        w2_entered = asyncio.Event()

        async def queued_writer() -> None:
            async with rw.writer():
                w2_entered.set()
                await asyncio.Event().wait()

        w2 = asyncio.create_task(queued_writer())
        await asyncio.sleep(0.01)  # w2 queued
        w2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await w2
        r = asyncio.create_task(rw.reader().__aenter__())
        await asyncio.sleep(0.01)
        # Holder state intact → reader stays blocked.
        assert not r.done()
        r.cancel()
        with pytest.raises(asyncio.CancelledError):
            await r
    # After the holder exits, reads work normally.
    async with asyncio.timeout(1.0):
        async with rw.reader():
            pass

