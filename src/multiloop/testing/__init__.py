"""Testing and verification utilities for multiloop."""

from __future__ import annotations

import asyncio

__all__ = ["rust_available", "wait_all_tasks_blocked"]


async def wait_all_tasks_blocked(cushion: float = 0.0) -> None:
    """Block until all currently runnable tasks on the calling event loop have parked.

    Schedules a probe callback on the event loop and awaits its completion, ensuring
    all preceding runnable coroutines have yielded control.
    """
    loop = asyncio.get_running_loop()
    event = asyncio.Event()
    loop.call_soon(event.set)
    await event.wait()
    if cushion > 0:
        await asyncio.sleep(cushion)


def rust_available() -> bool:
    """Return True if the native Rust ``_multiloop_core`` extension is compiled and importable."""
    try:
        import multiloop._multiloop_core  # noqa: F401 # pyright: ignore[reportUnusedImport]
    except ImportError:
        return False
    return True
