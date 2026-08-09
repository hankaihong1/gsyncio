"""示例 0：线程池基础 —— 提交任务、指定 worker、批量提交、健康指标。

运行: uv run python examples/00_pool_basics.py
"""

import asyncio

import gsyncio


async def heavy_task(x: int) -> int:
    """模拟一个耗时任务：睡 10ms 后返回翻倍结果。"""
    await asyncio.sleep(0.01)
    return x * 2


async def main() -> None:
    # 池是异步上下文管理器：进入时启动 worker 线程，退出时优雅关闭。
    async with gsyncio.EventLoopThreadPool(num_threads=4) as pool:
        # 1. 提交单个任务（全局队列 + 工作窃取）
        fut1 = pool.submit(heavy_task, 21)
        print("单个任务:", await fut1)  # 42

        # 2. 钉到指定 worker（loop=0），确定性路由
        fut2 = pool.submit(heavy_task, 21, loop=0)
        print("钉到 worker 0:", await fut2)  # 42

        # 3. 批量提交：start_soon 注册，离开 with 块时全部完成
        async with pool.submit_group() as group:
            results = [group.start_soon(heavy_task, i) for i in range(4)]
        print("批量结果:", [r.result() for r in results])  # [0, 2, 4, 6]

        # 4. 池健康指标（每个 worker 的活跃/完成计数）
        metrics = pool.get_metrics()
        print("metrics 键:", sorted(metrics.keys()))


if __name__ == "__main__":
    asyncio.run(main())
