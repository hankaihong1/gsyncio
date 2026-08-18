"""Example 3: structured concurrency — TaskGroup reaps all children,
fail_after/move_on_after bound the runtime.

Run: uv run python examples/03_taskgroup_timeout.py
"""

import asyncio

import multiloop


async def fetch(name: str, delay: float) -> str:
    await asyncio.sleep(delay)  # simulate a network request
    return f"{name}: ok"


async def main() -> None:
    # 1. TaskGroup: all children are guaranteed finished when the block exits
    async with multiloop.TaskGroup() as tg:
        h1 = tg.start_soon(fetch, "fast", 0.01)
        h2 = tg.start_soon(fetch, "slow", 0.03)
    print("TaskGroup:", await h1, "|", await h2)

    # 2. fail_after: overall deadline, raises TimeoutError on expiry
    try:
        async with multiloop.fail_after(0.05):
            await fetch("too-slow", 10)  # never completes
    except multiloop.TimeoutError:
        print("fail_after: timed out (TimeoutError)")

    # 3. move_on_after: silently skips on timeout, no exception
    async with multiloop.move_on_after(0.05) as scope:
        await fetch("also-slow", 10)
    print("move_on_after: skipped, cancelled_caught =", scope.cancelled_caught)

    # 4. CancelScope: manual cancellation + shield protecting cleanup code
    scope = multiloop.CancelScope()

    async def cancellable() -> None:
        async with scope:
            try:
                await asyncio.sleep(10)
            finally:
                # even when cancelled, the cleanup runs to completion
                # (shield semantics apply internally)
                print("  cleanup code finished")

    task = asyncio.create_task(cancellable())
    await asyncio.sleep(0.05)
    scope.cancel()  # trigger the cancellation manually
    try:
        await task
    except asyncio.CancelledError:
        print("CancelScope: task cancelled")


if __name__ == "__main__":
    asyncio.run(main())
