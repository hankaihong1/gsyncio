"""Example 2: group synchronization — AsyncWaitGroup waits for all,
AsyncOnce executes exactly once.

Run: uv run python examples/02_waitgroup_once.py
"""

import asyncio

import gsyncio


async def worker(name: str, wg: gsyncio.AsyncWaitGroup) -> None:
    try:
        await asyncio.sleep(0.02)
        print(f"  worker {name} done")
    finally:
        wg.done()  # decrement the counter regardless of success or failure


async def main() -> None:
    # 1. WaitGroup: add() counts, done() decrements, wait() blocks until zero
    wg = gsyncio.AsyncWaitGroup()
    async with gsyncio.EventLoopThreadPool(num_threads=4) as pool:
        for i in range(5):
            wg.add()
            pool.submit(worker, f"task-{i}", wg)
        await wg.wait()
    print("all workers finished")

    # 2. AsyncOnce: concurrent callers still run init exactly once, sharing
    #    the result
    once = gsyncio.AsyncOnce()

    async def init() -> str:
        await asyncio.sleep(0.02)  # simulate an expensive initialization
        return "initialized"

    # Three concurrent callers, but only one actually executes init
    results = await asyncio.gather(once.do(init), once.do(init), once.do(init))
    print("AsyncOnce results:", results)  # all three are "initialized"

    # 3. Exceptions are cached too: the first failure is re-raised to every
    #    later caller
    once_fail = gsyncio.AsyncOnce()

    def boom() -> None:
        raise ValueError("init failed")

    for i in range(2):
        try:
            await once_fail.do(boom)
        except ValueError as e:
            print(f"call {i + 1} got the same exception: {e}")


if __name__ == "__main__":
    asyncio.run(main())
