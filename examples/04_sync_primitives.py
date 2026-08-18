"""Example 4: synchronization primitives — Lock / Semaphore / Event /
Condition / Barrier.

Run: uv run python examples/04_sync_primitives.py
"""

import asyncio

import multiloop


async def main() -> None:
    # 1. Lock: mutual exclusion (FIFO-fair, safe across event loops/threads)
    lock = multiloop.Lock()
    shared = 0

    async def increment() -> None:
        nonlocal shared
        async with lock:
            v = shared
            await asyncio.sleep(0)  # yield to widen the race window
            shared = v + 1

    await asyncio.gather(*(increment() for _ in range(10)))
    print("shared under Lock =", shared)  # 10 (would be < 10 without the lock)

    # 2. Semaphore: caps concurrency (at most 2 tasks run here simultaneously)
    sem = multiloop.Semaphore(2)
    running = 0
    peak = 0

    async def limited() -> None:
        nonlocal running, peak
        async with sem:
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.01)
            running -= 1

    await asyncio.gather(*(limited() for _ in range(6)))
    print("Semaphore peak concurrency =", peak)  # 2

    # 3. Event: one-shot broadcast (sticky — once set, never cleared)
    event = multiloop.Event()

    async def waiter(name: str) -> None:
        await event.wait()
        print(f"  {name} got the event")

    tasks = [asyncio.create_task(waiter(f"w{i}")) for i in range(3)]
    await asyncio.sleep(0.01)
    event.set()  # wake all waiters
    await asyncio.gather(*tasks)

    # 4. Condition: wait on a predicate (wait() releases the lock, notify()
    #    does not require holding it)
    cond = multiloop.Condition()
    data: list[int] = []

    async def consumer() -> None:
        async with cond:
            while not data:  # standard spurious-wakeup-safe pattern
                await cond.wait()
            print(f"Condition consumed: {data.pop()}")

    async def producer() -> None:
        await asyncio.sleep(0.02)
        data.append(42)
        cond.notify()  # no lock required

    await asyncio.gather(consumer(), producer())

    # 5. Barrier: N parties must arrive before all pass together (auto-reset
    #    every round)
    barrier = multiloop.Barrier(3)

    async def party(name: str) -> None:
        result = await barrier.wait()
        print(f"  {name} passed (parties={result.parties})")

    await asyncio.gather(*(party(f"p{i}") for i in range(3)))


if __name__ == "__main__":
    asyncio.run(main())
