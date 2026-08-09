"""示例 2：组同步 —— AsyncWaitGroup 等全部完成，AsyncOnce 只执行一次。

运行: uv run python examples/02_waitgroup_once.py
"""

import asyncio

import gsyncio


async def worker(name: str, wg: gsyncio.AsyncWaitGroup) -> None:
    try:
        await asyncio.sleep(0.02)
        print(f"  worker {name} 完成")
    finally:
        wg.done()  # 无论成败都递减计数


async def main() -> None:
    # 1. WaitGroup：add 计数，done 递减，wait 等到归零
    wg = gsyncio.AsyncWaitGroup()
    async with gsyncio.EventLoopThreadPool(num_threads=4) as pool:
        for i in range(5):
            wg.add()
            pool.submit(worker, f"task-{i}", wg)
        await wg.wait()
    print("所有 worker 已结束")

    # 2. AsyncOnce：并发调用也只执行一次，结果共享
    once = gsyncio.AsyncOnce()

    async def init() -> str:
        await asyncio.sleep(0.02)  # 模拟昂贵的初始化
        return "initialized"

    # 三个并发调用者，只有一个真正执行 init
    results = await asyncio.gather(once.do(init), once.do(init), once.do(init))
    print("AsyncOnce 结果:", results)  # 三个都是 "initialized"

    # 3. 异常也会缓存：第一次抛错，后续调用者重抛同一个异常
    once_fail = gsyncio.AsyncOnce()

    def boom() -> None:
        raise ValueError("init failed")

    for i in range(2):
        try:
            await once_fail.do(boom)
        except ValueError as e:
            print(f"第 {i + 1} 次调用也拿到同一异常: {e}")


if __name__ == "__main__":
    asyncio.run(main())
