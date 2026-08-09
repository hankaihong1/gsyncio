"""示例 1：Go 风格通道 —— 发送/接收、优雅迭代、select 多路复用。

运行: uv run python examples/01_channels_select.py
"""

import asyncio

import gsyncio


async def main() -> None:
    # 1. 基础 send/recv（跨任务安全）
    ch = gsyncio.FastChannel()
    await ch.send("hello")
    print("recv:", await ch.recv())  # hello

    # 2. async for 优雅迭代：生产者发完调用 close()，迭代自动终止
    ch2 = gsyncio.FastChannel()

    async def producer() -> None:
        for i in range(3):
            await ch2.send(i)
        ch2.close()  # 发送完毕信号

    asyncio.create_task(producer())
    print("迭代:", [item async for item in ch2])  # [0, 1, 2]

    # 3. select_channel：等待最先就绪的通道（Go select 风格）
    ch3, ch4 = gsyncio.FastChannel(), gsyncio.FastChannel()

    async def slow_sender() -> None:
        await asyncio.sleep(0.05)
        await ch3.send("from ch3")

    async def fast_sender() -> None:
        await asyncio.sleep(0.01)
        await ch4.send("from ch4")

    asyncio.create_task(slow_sender())
    asyncio.create_task(fast_sender())
    selected_ch, val = await gsyncio.select_channel(ch3, ch4)
    print("select 胜出:", selected_ch is ch4, val)  # True from ch4

    # 4. 非阻塞轮询：default= 参数，没有数据立即返回默认值
    ch5 = gsyncio.FastChannel()
    print("非阻塞:", await gsyncio.select_channel(ch5, default="nothing ready"))


if __name__ == "__main__":
    asyncio.run(main())
