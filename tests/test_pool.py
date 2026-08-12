import asyncio
import threading

import pytest

from gsyncio import (
    AsyncContext,
    AsyncOnce,
    AsyncRWMutex,
    AsyncWaitGroup,
    ChannelClosedError,
    FastChannel,
    GsyncioError,
    create_pool,
    run_in_pool,
)
from gsyncio.exceptions import ThreadPoolClosedError
from gsyncio.pool import EventLoopThreadPool, PoolOptions
from gsyncio.testing import wait_all_tasks_blocked


@pytest.mark.asyncio
async def test_pool_valid_basic_submit():
    """Valid 1: Basic coroutine dispatch and return value"""

    async def add(a, b):
        return a + b

    async with EventLoopThreadPool(num_threads=2) as pool:
        fut = pool.submit(add, 3, 5)
        res = await fut
        assert res == 8


@pytest.mark.asyncio
async def test_pool_valid_round_robin_thread_distribution():
    """Valid 2: Round-Robin thread distribution verification"""

    async def get_thread_id():
        return threading.get_ident()

    num_threads = 4
    async with EventLoopThreadPool(num_threads=num_threads) as pool:
        # Explicitly test multi-thread distribution and work-stealing
        futs = [pool.submit(get_thread_id, pin_to=i % num_threads) for i in range(num_threads * 2)]
        tids = [await f for f in futs]

        unique_threads = set(tids)
        assert len(unique_threads) == num_threads
        assert threading.get_ident() not in unique_threads


@pytest.mark.asyncio
async def test_pool_valid_context_manager_lifecycle():
    """Valid 3: Context manager lifecycle"""
    pool = EventLoopThreadPool(num_threads=2)
    assert not pool.is_running
    async with pool:
        assert pool.is_running
    assert not pool.is_running


@pytest.mark.asyncio
async def test_pool_boundary_single_thread():
    """Boundary 1: num_threads=1 boundary handling"""

    async def echo(val):
        return val

    async with EventLoopThreadPool(num_threads=1) as pool:
        fut = pool.submit(echo, "single")
        assert await fut == "single"


@pytest.mark.asyncio
async def test_pool_boundary_high_concurrency_submit():
    """Boundary 2: High-concurrency coroutine dispatch"""

    async def square(x):
        await asyncio.sleep(0.001)
        return x * x

    count = 100
    async with EventLoopThreadPool(num_threads=4) as pool:
        futs = [pool.submit(square, i) for i in range(count)]
        results = await asyncio.gather(*futs)
        assert results == [i * i for i in range(count)]


@pytest.mark.asyncio
async def test_pool_boundary_async_io_inside_coro():
    """Boundary 3: Dispatched coroutine internally suspends via asyncio.sleep"""

    async def delayed_val(v):
        await asyncio.sleep(0.01)
        return v * 2

    async with EventLoopThreadPool(num_threads=2) as pool:
        res = await pool.submit(delayed_val, 21)
        assert res == 42


@pytest.mark.asyncio
async def test_pool_error_coro_exception_propagation():
    """Error 1: Exception from cross-thread coroutine is correctly propagated and caught"""

    async def failing_coro():
        raise ValueError("custom error inside thread loop")

    async with EventLoopThreadPool(num_threads=2) as pool:
        fut = pool.submit(failing_coro)
        with pytest.raises(ValueError, match="custom error inside thread loop"):
            await fut


@pytest.mark.asyncio
async def test_pool_error_submit_after_close():
    """Error 2: Submitting task after closing the thread pool raises RuntimeError"""

    async def dummy():
        return 1

    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()
    await pool.close()

    with pytest.raises(RuntimeError, match="ThreadPool is not running"):
        pool.submit(dummy)


@pytest.mark.asyncio
async def test_pool_error_invalid_num_threads():
    """Error 3: Invalid thread count initialization raises ValueError"""
    with pytest.raises(ValueError, match="num_threads must not be negative"):
        EventLoopThreadPool(num_threads=-1)


@pytest.mark.asyncio
async def test_pool_race_close_vs_submit():
    """Error 4: Concurrent close vs submit race condition must not leak raw RuntimeError (must be ThreadPoolClosedError)"""

    async def dummy():
        return 1

    pool = EventLoopThreadPool(num_threads=4)
    await pool.start()

    leaked: list[RuntimeError] = []
    stop = threading.Event()

    async def submitter():
        while not stop.is_set():
            try:
                pool.submit(dummy)
            except ThreadPoolClosedError:
                pass
            except RuntimeError as exc:  # noqa: PERF203
                leaked.append(exc)
                return
            await asyncio.sleep(0)

    task = asyncio.create_task(submitter())
    await pool.close()
    stop.set()
    await task

    assert not leaked, f"submit() leaked raw RuntimeError under close race condition: {leaked}"


@pytest.mark.asyncio
async def test_create_pool_auto_starts():
    """create_pool returns a pool that is already running."""
    pool = await create_pool(num_threads=2)
    try:
        assert pool.is_running
        assert pool.num_threads == 2
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_create_pool_custom_num_threads():
    """create_pool with explicit num_threads creates correct number of workers."""
    pool = await create_pool(num_threads=4)
    try:
        assert pool.num_threads == 4
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_create_pool_context_manager():
    """create_pool works as an async context manager (auto-close on exit)."""
    pool = await create_pool(num_threads=2)
    async with pool:
        assert pool.is_running
        result = await pool.submit(asyncio.sleep, 0.001)
        assert result is None
    assert not pool.is_running


@pytest.mark.asyncio
async def test_pool_options_default_values():
    """PoolOptions uses module-level defaults."""
    opts = PoolOptions()
    assert opts.num_threads == 0  # 0 = auto-detect
    assert opts.loop_factory is None


@pytest.mark.asyncio
async def test_pool_options_num_threads_override():
    """PoolOptions accepts a custom num_threads value."""
    opts = PoolOptions(num_threads=8)
    assert opts.num_threads == 8


@pytest.mark.asyncio
async def test_pool_options_loop_factory_override():
    """PoolOptions accepts a custom loop_factory callable."""
    opts = PoolOptions(loop_factory=asyncio.new_event_loop)
    assert opts.loop_factory is asyncio.new_event_loop


@pytest.mark.asyncio
async def test_pool_metrics_and_pull_model_scheduling():
    """Test thread pool metrics retrieval and normal task scheduling under Pull Model"""
    async with EventLoopThreadPool(num_threads=2) as pool:
        # 1. Verify get_metrics
        metrics = pool.get_metrics()
        assert metrics["thread_count"] == 2
        assert len(metrics["completed_tasks"]) == 2
        assert metrics["is_running"] is True

        # 2. Normal task submission
        async def work():
            return 123

        assert await pool.submit(work) == 123

        # 3. Verify concurrent Pull Model
        futs = [pool.submit(work) for _ in range(20)]
        results = await asyncio.gather(*futs)
        assert results == [123] * 20


@pytest.mark.asyncio
async def test_work_stealing_and_loop_pinning():
    """Verify Pull Model work stealing and explicit submit(pin_to=...) binding capability"""
    async with EventLoopThreadPool(num_threads=2) as pool:
        # 1. Verify Work-Stealing global shared queue dispatch and execution
        futs = [pool.submit(asyncio.sleep, 0.01) for _ in range(10)]
        await asyncio.gather(*futs)

        # 2. Verify explicit target Worker index binding
        fut0 = pool.submit(asyncio.sleep, 0.01, pin_to=0)
        fut1 = pool.submit(asyncio.sleep, 0.01, pin_to=1)

        await asyncio.gather(fut0, fut1)

        # 3. Verify out-of-range index raises ValueError
        with pytest.raises(ValueError, match="out of range"):
            pool.submit(print, "invalid", pin_to=99)

        # 4. Verify explicit AbstractEventLoop instance binding
        target_loop = pool._get_loop(1)
        fut_target = pool.submit(asyncio.sleep, 0.01, pin_to=target_loop)
        await fut_target

        metrics = pool.get_metrics()
        assert "completed_tasks" in metrics
        assert metrics["thread_count"] == 2


@pytest.mark.asyncio
async def test_pool_close_cancels_submitted_slow_tasks():
    """Close pool while slow tasks are running — verify futures are cancelled."""

    async def slow_task(delay: float) -> str:
        await asyncio.sleep(delay)
        return "done"

    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()

    f1 = pool.submit(slow_task, 10.0)
    f2 = pool.submit(slow_task, 10.0)

    # Give the tasks time to be pulled by the workers.
    await wait_all_tasks_blocked()

    # Close drains for up to ~5s, then stops the worker loops — the still
    # running 10s tasks are cancelled and their futures resolve.
    await pool.close()

    # Let the call_soon_threadsafe result deliveries land on this loop.
    await wait_all_tasks_blocked()

    assert f1.done() and isinstance(f1.exception(), asyncio.CancelledError)
    assert f2.done() and isinstance(f2.exception(), asyncio.CancelledError)


# ---------------------------------------------------------------------------
# FIX-7 regression tests (BUG-7 restart, TS-1 _get_loop after close) — 2026-08-10
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restart_after_close_raises() -> None:
    """BUG-7: start() after close() must fail loudly instead of silently
    accepting tasks that can never run."""
    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()
    await pool.close()

    with pytest.raises(RuntimeError, match="restart"):
        await pool.start()


@pytest.mark.asyncio
async def test_get_loop_after_close_raises_runtime_error() -> None:
    """TS-1: _get_loop after close must raise RuntimeError, not IndexError
    from a bare unlocked read of the cleared _loops list."""
    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()
    assert pool._get_loop(1) is not None
    await pool.close()

    with pytest.raises(RuntimeError, match="not running"):
        pool._get_loop(0)


@pytest.mark.asyncio
async def test_pool_start_idempotent():
    """pool.start() is a no-op when already running (line 250)."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        await pool.start()  # pool is already running; should return immediately.


@pytest.mark.asyncio
async def test_pool_invalid_loop_targets():
    """_resolve_target_worker raises ValueError for unmanaged loops and out-of-range indices,
    and TypeError for invalid loop types (lines 360-362, 368-372)."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        # Unmanaged AbstractEventLoop (the test's own loop)
        with pytest.raises(ValueError, match="not managed"):
            pool.submit(asyncio.sleep, 0, pin_to=asyncio.get_running_loop())

        # Out-of-range worker index
        with pytest.raises(ValueError, match="out of range"):
            pool.submit(asyncio.sleep, 0, pin_to=99)

        # Invalid type for pin_to parameter
        with pytest.raises(TypeError, match="must be an AbstractEventLoop"):
            pool.submit(asyncio.sleep, 0, pin_to="invalid")


@pytest.mark.asyncio
async def test_pool_submit_plain_values():
    """submit() handles plain (non-coroutine, non-callable) values and
    callables returning plain values (line 435, branch 432->437)."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        # Plain value → covers line 435 (else branch of _execute_task)
        result = await pool.submit(42)
        assert result == 42

        # Callable returning plain value → covers branch 432->437
        def plain_func():
            return 99

        result = await pool.submit(plain_func)
        assert result == 99


@pytest.mark.asyncio
async def test_pool_default_threads():
    """pool.py line 94: num_threads=None triggers os.cpu_count() or 4 fallback."""
    async with EventLoopThreadPool() as pool:
        assert pool.num_threads > 0
        result = await pool.submit(asyncio.sleep, 0.001)
        assert result is None


@pytest.mark.asyncio
async def test_run_in_pool_basic_coroutine():
    """run_in_pool executes a coroutine function and returns its result."""

    async def add(a: int, b: int) -> int:
        return a + b

    result = await run_in_pool(add, 3, 5)
    assert result == 8


@pytest.mark.asyncio
async def test_run_in_pool_with_args():
    """run_in_pool passes positional and keyword args to the coroutine."""

    async def echo(a: int, b: int, *, c: int = 0) -> tuple[int, int, int]:
        return (a, b, c)

    result = await run_in_pool(echo, 1, 2, c=3)
    assert result == (1, 2, 3)


@pytest.mark.asyncio
async def test_run_in_pool_exception_propagation():
    """run_in_pool propagates exceptions from the coroutine to the caller."""

    async def failing() -> None:
        raise ValueError("test error inside run_in_pool")

    with pytest.raises(ValueError, match="test error inside run_in_pool"):
        await run_in_pool(failing)


@pytest.mark.asyncio
async def test_resolve_target_worker_lock_concurrent():
    """_resolve_target_worker holds self._lock during resolution.

    Concurrent access from multiple threads must not crash or produce
    inconsistent loop->index mappings.
    """
    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()

    errors: list[Exception] = []
    results: list[int] = []

    def resolve_from_thread(idx: int) -> None:
        try:
            # Resolve a valid worker index — must not crash under lock.
            info = pool._resolve_target_worker(idx)
            if info is not None:
                results.append(info[0])
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=resolve_from_thread, args=(i % 2,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Unexpected errors: {errors}"
    # All resolutions should have succeeded (pool is running, indices valid).
    assert len(results) == 10

    await pool.close()


@pytest.mark.asyncio
async def test_notify_all_workers_toctou():
    """_notify_all_workers is safe when called concurrently with pool close.

    The method captures loops/events under lock then calls
    call_soon_threadsafe outside — must not raise AttributeError or
    crash when close() clears the lists between capture and use.
    """
    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()

    errors: list[Exception] = []

    def hammer_notify() -> None:
        try:
            for _ in range(50):
                pool._notify_all_workers()
        except Exception as exc:
            errors.append(exc)

    t = threading.Thread(target=hammer_notify)
    t.start()

    # Close during hammering — exercises TOCTOU between list copy and loop access.
    await pool.close()
    t.join(timeout=5)

    assert len(errors) == 0, f"TOCTOU errors: {errors}"


@pytest.mark.asyncio
async def test_repr_implementations():
    """Verify friendly debug diagnostic output of __repr__ for core classes"""
    async with EventLoopThreadPool(num_threads=2) as pool:
        repr_pool = repr(pool)
        assert "EventLoopThreadPool" in repr_pool
        assert "threads=2" in repr_pool

    ch = FastChannel()
    assert "FastChannel" in repr(ch)

    ctx = AsyncContext()
    assert "AsyncContext" in repr(ctx)

    wg = AsyncWaitGroup()
    assert "AsyncWaitGroup" in repr(wg)

    once = AsyncOnce()
    assert "AsyncOnce" in repr(once)

    rw = AsyncRWMutex()
    assert "AsyncRWMutex" in repr(rw)


@pytest.mark.asyncio
async def test_pool_closed_submission():
    """Test exception on ThreadPool submission after close"""
    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()
    await pool.close()

    assert not pool.is_running

    with pytest.raises(ThreadPoolClosedError):
        pool.submit(print, "hello")


@pytest.mark.asyncio
async def test_exception_hierarchy():
    """Verify engineering standard 1: all gsyncio custom exceptions inherit from GsyncioError"""
    assert issubclass(ChannelClosedError, GsyncioError)
    assert issubclass(ThreadPoolClosedError, GsyncioError)


@pytest.mark.asyncio
async def test_pool_closed_error_type():
    """Verify engineering standard 1b: submitting on a closed pool raises ThreadPoolClosedError"""
    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()
    metrics = pool.get_metrics()
    assert "completed_tasks" in metrics
    assert "active_tasks" in metrics
    await pool.close()

    async def dummy():
        pass

    with pytest.raises(ThreadPoolClosedError):
        pool.submit(dummy)


@pytest.mark.asyncio
async def test_pool_closed_error_type_native_push_path():
    """Verify engineering standard 1b: when native pool is closed (close race window), submit raises ThreadPoolClosedError, not leaking raw RuntimeError"""
    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()

    # Simulate close race window: _running is still True, but native pool already closed, push fails
    assert pool._native_pool is not None
    pool._native_pool.close()

    async def dummy():
        pass

    with pytest.raises(ThreadPoolClosedError, match="ThreadPool is closed"):
        pool.submit(dummy)
    with pytest.raises(ThreadPoolClosedError, match="ThreadPool is closed"):
        pool.submit(dummy, pin_to=0)

    await pool.close()


@pytest.mark.asyncio
async def test_vulnerability_submit_coroutine_object():
    """Vulnerability 2: Submit with an already-instantiated coroutine object should run normally instead of raising TypeError"""
    async with EventLoopThreadPool(num_threads=2) as pool:

        async def sample_coro(val):
            await asyncio.sleep(0.01)
            return val * 10

        # Instantiate coroutine object
        coro_obj = sample_coro(5)

        # Directly submit coroutine object
        fut = pool.submit(coro_obj)
        res = await fut
        assert res == 50


@pytest.mark.asyncio
async def test_context_futures_cleaned():
    """R3-FIX-21 (probe R3-D): completed AsyncContext submissions must be
    dropped from _futures — pre-fix 500/500 stayed behind, so cancel() kept
    walking stale futures forever."""
    from gsyncio import AsyncContext

    ctx = AsyncContext()
    async with EventLoopThreadPool(num_threads=2) as pool:

        async def quick() -> int:
            return 7

        for _ in range(200):
            fut = ctx.submit(pool, quick)
            await asyncio.wait_for(fut, timeout=5.0)
    assert len(ctx._futures) == 0  # type: ignore[attr-defined]


# ── U6 FIX-13 + FIX-24: abort completes futures + contextvars propagation ──


@pytest.mark.asyncio
async def test_abort_completes_all_futures():
    """R2-FIX-13 (probe F13b): abort() must complete every outstanding
    future — pre-fix 2911/5000 hung forever because queued-but-unexecuted
    tasks were discarded with nobody completing their futures."""
    async with EventLoopThreadPool(num_threads=2) as pool:

        async def slow() -> int:
            await asyncio.sleep(10)
            return 1

        futs = [pool.submit(slow) for _ in range(500)]
        await asyncio.sleep(0.05)  # let some tasks start executing
        await pool.abort()
        done, pending = await asyncio.wait(futs, timeout=5.0)
        assert len(pending) == 0, f"{len(pending)} futures still pending after abort"
        for fut in done:
            try:
                exc = fut.exception()
            except asyncio.CancelledError:
                continue  # future was cancelled outright — legitimate
            if exc is not None:
                # Two legitimate abort outcomes: tasks that never ran get
                # ThreadPoolClosedError from abort(); tasks that were being
                # executed get CancelledError from the worker-thread
                # shutdown cancel (_worker's finally) — both mean "aborted".
                assert isinstance(exc, (ThreadPoolClosedError, asyncio.CancelledError)), exc


@pytest.mark.asyncio
async def test_abort_delivery_no_invalid_state(capsys):
    """R2-FIX-13 revision B: abort racing a worker's own delivery must not
    surface InvalidStateError noise (the guard lives inside the scheduled
    callback, like _channel_base._set_soon)."""
    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()
    try:

        async def quick() -> int:
            return 42

        futs = [pool.submit(quick) for _ in range(200)]
        await asyncio.sleep(0.01)  # some complete, some still queued
        await pool.abort()
        await asyncio.wait(futs, timeout=5.0)
    finally:
        await pool.close()
    err = capsys.readouterr().err
    assert "InvalidStateError" not in err


@pytest.mark.asyncio
async def test_submit_propagates_contextvars():
    """R4-FIX-24 (probe R4-A): the caller's contextvars must reach the worker
    task — pre-fix the worker read 'missing' instead of the caller's value."""
    import contextvars

    var = contextvars.ContextVar("audit_var", default="missing")
    seen: dict[str, str] = {}

    async def read_var() -> None:
        seen["value"] = var.get()

    async with EventLoopThreadPool(num_threads=1) as pool:
        token = var.set("caller-value")
        try:
            await asyncio.wait_for(pool.submit(read_var), timeout=5.0)
        finally:
            var.reset(token)
    assert seen.get("value") == "caller-value"


@pytest.mark.asyncio
async def test_submit_no_ctx_backward_compat():
    """FIX-24: without a caller-set ContextVar the default path is unchanged
    (the worker sees the variable's default, not a corrupted context)."""
    import contextvars

    var = contextvars.ContextVar("audit_var2", default="missing")

    async def read_var() -> str:
        return var.get()

    async with EventLoopThreadPool(num_threads=1) as pool:
        res = await asyncio.wait_for(pool.submit(read_var), timeout=5.0)
    assert res == "missing"


# ---------------------------------------------------------------------------
# FIX-A / FIX-B / FIX-H (R5 audit): future-completion contracts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_cancel_scope_completes_future():
    """FIX-A: ``pool.submit(coro, cancel_scope=s)`` + ``s.cancel()`` must
    complete the caller's future with CancelledError — pre-fix the future
    stayed pending forever (worker returned silently on the scope-cancel
    branch)."""
    from gsyncio._cancel import CancelScope

    async with EventLoopThreadPool(num_threads=2) as pool:
        scope = CancelScope()

        async def work() -> int:
            return 42

        fut = pool.submit(work, cancel_scope=scope)
        scope.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(fut, timeout=2.0)


@pytest.mark.asyncio
async def test_pool_close_completes_all_burst_futures():
    """FIX-B contract lock: burst submit + immediate ``close()`` must
    complete every future.  Pre-fix a task popped by the drain but never
    stepped (loop.stop raced create_task) was cancelled at its outermost
    await and its future stayed pending forever; close() now also completes
    any leftover ``_outstanding`` futures as a safety net."""
    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()

    async def work(i: int) -> int:
        await asyncio.sleep(0)
        return i

    futs = [pool.submit(work, i) for i in range(40)]
    await asyncio.wait_for(pool.close(), timeout=5.0)
    pending = [f for f in futs if not f.done()]
    for f in pending:
        f.cancel()
    assert not pending, f"{len(pending)} futures left pending after close()"
    with pool._lock:
        assert not pool._outstanding, "close() left futures in _outstanding"


@pytest.mark.asyncio
async def test_pool_submit_push_failure_discards_future():
    """FIX-H: a failed native push must not leave the future registered in
    ``_outstanding`` (repeated failures would grow the set forever)."""
    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()

    class _FailingPool:
        """Duck-typed native pool whose push always fails; the idle paths
        (pop_work / is_closed) stay benign so live workers keep running."""

        def push_global(self, task: object) -> None:  # noqa: ARG002
            raise RuntimeError("boom")

        def push_local(self, index: int, task: object) -> None:  # noqa: ARG002
            raise RuntimeError("boom")

        def pop_work(self, index: int) -> None:  # noqa: ARG002
            return None

        def is_closed(self) -> bool:
            return False

        def close(self) -> None:
            pass

        def set_metrics(self, metrics: object) -> None:  # noqa: ARG002
            pass

    try:
        pool._native_pool = _FailingPool()  # type: ignore[assignment]
        with pytest.raises(RuntimeError, match="boom"):
            pool.submit(lambda: 1)
        # WHY: snapshot under the lock, assert OUTSIDE it — pytest's
        # assertion rewriting reprs the failed expression, and repr(pool)
        # takes the same non-reentrant lock (self-deadlock on failure).
        with pool._lock:
            outstanding = bool(pool._outstanding)
        assert not outstanding
    finally:
        await pool.close()
