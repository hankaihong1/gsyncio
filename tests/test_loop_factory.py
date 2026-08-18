import asyncio

import pytest

from multiloop import EventLoopThreadPool


@pytest.mark.asyncio
async def test_custom_loop_factory():
    """Verify EventLoopThreadPool supports an explicitly-passed loop_factory."""
    custom_created = 0

    def custom_factory() -> asyncio.AbstractEventLoop:
        nonlocal custom_created
        custom_created += 1
        return asyncio.new_event_loop()

    async with EventLoopThreadPool(num_threads=2, loop_factory=custom_factory) as pool:
        assert custom_created == 2

        async def hello():
            return "custom-ready"

        res = await pool.submit(hello)
        assert res == "custom-ready"
