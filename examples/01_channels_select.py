"""Example 1: Go-style channels — send/recv, graceful iteration, select multiplexing.

Run: uv run python examples/01_channels_select.py
"""

import asyncio

import multiloop


async def main() -> None:
    # 1. Basic send/recv (safe across tasks)
    ch = multiloop.Channel()
    await ch.send("hello")
    print("recv:", await ch.recv())  # hello

    # 2. Graceful iteration via async for: the producer calls close() when
    #    done and iteration terminates automatically
    ch2 = multiloop.Channel()

    async def producer() -> None:
        for i in range(3):
            await ch2.send(i)
        ch2.close()  # signal "no more items"

    asyncio.create_task(producer())
    print("iteration:", [item async for item in ch2])  # [0, 1, 2]

    # 3. select_channel: wait for the first ready channel (Go select style)
    ch3, ch4 = multiloop.Channel(), multiloop.Channel()

    async def slow_sender() -> None:
        await asyncio.sleep(0.05)
        await ch3.send("from ch3")

    async def fast_sender() -> None:
        await asyncio.sleep(0.01)
        await ch4.send("from ch4")

    asyncio.create_task(slow_sender())
    asyncio.create_task(fast_sender())
    selected_ch, val = await multiloop.select_channel(ch3, ch4)
    print("select winner:", selected_ch is ch4, val)  # True from ch4

    # 4. Non-blocking polling: with default=, returns the default immediately
    #    when no channel has data
    ch5 = multiloop.Channel()
    print("non-blocking:", await multiloop.select_channel(ch5, default="nothing ready"))


if __name__ == "__main__":
    asyncio.run(main())
