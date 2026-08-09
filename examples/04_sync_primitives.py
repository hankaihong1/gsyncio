"""示例 4：同步原语 —— Lock / Semaphore / Event / Condition / Barrier。

运行: uv run python examples/04_sync_primitives.py
"""

import asyncio

import gsyncio


async def main() -> None:
    # 1. Lock：互斥访问（FIFO 公平，跨事件循环/线程安全）
    lock = gsyncio.Lock()
    shared = 0

    async def increment() -> None:
        nonlocal shared
        async with lock:
            v = shared
            await asyncio.sleep(0)  # 让出控制权，制造竞争窗口
            shared = v + 1

    await asyncio.gather(*(increment() for _ in range(10)))
    print("Lock 保护下 shared =", shared)  # 10（无锁会小于 10）

    # 2. Semaphore：限制并发数（这里同时最多 2 个任务在跑）
    sem = gsyncio.Semaphore(2)
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
    print("Semaphore 峰值并发 =", peak)  # 2

    # 3. Event：一次性广播（sticky，set 后永不清除）
    event = gsyncio.Event()

    async def waiter(name: str) -> None:
        await event.wait()
        print(f"  {name} 收到事件")

    tasks = [asyncio.create_task(waiter(f"w{i}")) for i in range(3)]
    await asyncio.sleep(0.01)
    event.set()  # 唤醒所有等待者
    await asyncio.gather(*tasks)

    # 4. Condition：等待条件（wait 释放锁，notify 不需要持锁）
    cond = gsyncio.Condition()
    data: list[int] = []

    async def consumer() -> None:
        async with cond:
            while not data:  # 标准防虚假唤醒写法
                await cond.wait()
            print(f"Condition 消费: {data.pop()}")

    async def producer() -> None:
        await asyncio.sleep(0.02)
        data.append(42)
        cond.notify()  # 无需持锁

    await asyncio.gather(consumer(), producer())

    # 5. Barrier：N 个任务到齐才一起放行（每轮自动重置）
    barrier = gsyncio.Barrier(3)

    async def party(name: str) -> None:
        result = await barrier.wait()
        print(f"  {name} 过关 (fulfilled={result.fulfilled})")

    await asyncio.gather(*(party(f"p{i}") for i in range(3)))


if __name__ == "__main__":
    asyncio.run(main())
