"""Testing utilities for gsyncio."""

import asyncio


async def wait_all_tasks_blocked(cushion: float = 0.0) -> None:
    """Block until all other runnable tasks on the calling event loop are parked.

    Single-loop only. For multi-loop synchronization, use a shared atomic counter.
    """
    # Schedule a no-op and yield control. After the no-op runs,
    # all previously-scheduled tasks have had a chance to run and park.
    loop = asyncio.get_running_loop()
    event = asyncio.Event()
    loop.call_soon(event.set)
    await event.wait()
    if cushion > 0:
        await asyncio.sleep(cushion)


class _CheckpointContext:
    def __init__(self) -> None:
        self._entered = False

    async def __aenter__(self) -> None:
        self._entered = True

    async def __aexit__(self, *args: object) -> None:
        if not self._entered:
            raise AssertionError("assert_checkpoints block did not yield")


def assert_checkpoints() -> _CheckpointContext:
    return _CheckpointContext()


def rust_available() -> bool:
    """Return True if the Rust _gsyncio_core extension is loaded."""
    try:
        import gsyncio._gsyncio_core  # noqa: F401 # pyright: ignore[reportUnusedImport]
    except ImportError:
        return False
    return True
