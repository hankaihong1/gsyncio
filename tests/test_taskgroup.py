"""Tests for gsyncio TaskGroup (structured concurrency)."""

import asyncio
from typing import Any

import pytest

from gsyncio._taskgroup import TaskGroup, TaskHandle, TaskStatus, _TaskStatus


async def _worker(value: Any) -> Any:
    await asyncio.sleep(0)
    return value


async def _worker_slow(value: Any, delay: float = 0.1) -> Any:
    await asyncio.sleep(delay)
    return value


async def _worker_fail(msg: str) -> None:
    await asyncio.sleep(0)
    raise ValueError(msg)


async def _worker_fail_slow(msg: str, delay: float = 0.1) -> None:
    await asyncio.sleep(delay)
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Test 1: all children succeed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_succeed() -> None:
    """Spawn 3 tasks — every one returns cleanly, TaskGroup exits without error."""
    async with TaskGroup() as tg:
        h1 = tg.start_soon(_worker, 1)
        h2 = tg.start_soon(_worker, 2)
        h3 = tg.start_soon(_worker, 3)

    assert await h1 == 1
    assert await h2 == 2
    assert await h3 == 3
    assert h1.status == _TaskStatus.FINISHED
    assert h2.status == _TaskStatus.FINISHED
    assert h3.status == _TaskStatus.FINISHED


# ---------------------------------------------------------------------------
# Test 2: one failure cancels siblings, single exception re-raised
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_fails_cancels_others() -> None:
    """Task 0 raises ValueError; tasks 1 & 2 are cancelled by the scope.

    Only the single ValueError is re-raised (not wrapped in a group).
    """
    h1: TaskHandle | None = None
    h2: TaskHandle | None = None

    with pytest.raises(ValueError, match="boom"):
        async with TaskGroup() as tg:
            h1 = tg.start_soon(_worker_fail, "boom")  # fails fast
            h2 = tg.start_soon(_worker_slow, "slow")  # gets cancelled

    # h1 has the ValueError.
    assert h1 is not None
    assert h1.status == _TaskStatus.FINISHED
    assert isinstance(h1.exception, ValueError)

    # h2 should have been cancelled by the sibling-cancel.
    assert h2 is not None
    assert h2.status == _TaskStatus.FINISHED
    assert isinstance(h2.exception, asyncio.CancelledError)


# ---------------------------------------------------------------------------
# Test 3: multiple independent failures → BaseExceptionGroup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_fail_aggregate() -> None:
    """Two tasks raise different exceptions concurrently.

    Both exceptions appear in a BaseExceptionGroup.
    """
    with pytest.raises(BaseExceptionGroup, match="taskgroup crashed") as exc_info:
        async with TaskGroup() as tg:
            tg.start_soon(_worker_fail, "e1")  # both fire ~simultaneously
            tg.start_soon(_worker_fail, "e2")

    eg = exc_info.value
    exceptions = eg.exceptions
    assert len(exceptions) == 2
    msgs = {str(e) for e in exceptions}
    assert msgs == {"e1", "e2"}


# ---------------------------------------------------------------------------
# Test 4: TaskGroup.start() blocks until task_status.started()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_with_task_status() -> None:
    """``tg.start()`` does not return before the spawned task calls
    ``task_status.started()``."""

    started_flag: bool = False

    async def child(ts: TaskStatus) -> str:
        nonlocal started_flag
        # Simulate expensive initialisation.
        await asyncio.sleep(0.05)
        ts.started()
        started_flag = True
        await asyncio.sleep(0.02)
        return "done"

    async with TaskGroup() as tg:
        handle = await tg.start(child)
        # By the time start() returns, started() has been called.
        assert started_flag is True
        assert handle.status == _TaskStatus.STARTED

    assert handle.status == _TaskStatus.FINISHED
    assert handle.result == "done"


# ---------------------------------------------------------------------------
# Test 5: CancelledError from sibling-cancel is filtered
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_children_filtered() -> None:
    """When the scope cancels siblings, their CancelledError does NOT appear
    in the exception group (only the originating error does)."""

    h_slow: TaskHandle | None = None

    with pytest.raises(ValueError, match="original"):
        async with TaskGroup() as tg:
            h_slow = tg.start_soon(_worker_slow, "ok", 0.1)  # will get cancelled
            tg.start_soon(_worker_fail, "original")  # triggers sibling-cancel

    # The slow task was cancelled by sibling-cancel — that CancelledError
    # is filtered and should NOT be in the raised exception.
    assert h_slow is not None
    assert isinstance(h_slow.exception, asyncio.CancelledError)


# ---------------------------------------------------------------------------
# Test 6: TaskHandle.result / .exception raise RuntimeError when unfinished
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_properties_raise_before_done() -> None:
    """Accessing .result or .exception before the task finishes raises
    RuntimeError."""
    handle: TaskHandle | None = None

    async with TaskGroup() as tg:
        handle = tg.start_soon(_worker_slow, 42, 0.05)
        with pytest.raises(RuntimeError, match="not finished"):
            _ = handle.result
        with pytest.raises(RuntimeError, match="not finished"):
            _ = handle.exception

    assert handle.result == 42
    assert handle.exception is None


# ---------------------------------------------------------------------------
# TaskGroup shield + start crash (Wave 2 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_taskgroup_start_child_crash_cleans_up():
    """TaskGroup.start() cancels siblings when the spawned child crashes after init.

    Post-fix: when a child started via tg.start() crashes after calling
    task_status.started(), the sibling-cancel mechanism in _wait_children
    cancels remaining children and propagates the exception.
    """
    cleanup_called = False

    async def child_crashes_after_init(ts: TaskStatus) -> None:
        ts.started()
        await asyncio.sleep(0.02)
        raise ValueError("post-init crash")

    async def innocent_child() -> int:
        nonlocal cleanup_called
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cleanup_called = True
            raise
        return 0

    with pytest.raises(ValueError, match="post-init crash"):
        async with TaskGroup() as tg:
            tg.start_soon(innocent_child)
            await tg.start(child_crashes_after_init)

    assert cleanup_called, "sibling child was not cancelled after start child crash"
