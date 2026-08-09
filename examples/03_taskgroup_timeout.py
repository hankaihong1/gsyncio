"""示例 3：结构化并发 —— TaskGroup 子任务必回收，fail_after/move_on_after 限时。

运行: uv run python examples/03_taskgroup_timeout.py
"""

import asyncio

import gsyncio


async def fetch(name: str, delay: float) -> str:
    await asyncio.sleep(delay)  # 模拟网络请求
    return f"{name}: ok"


async def main() -> None:
    # 1. TaskGroup：离开 async with 时所有子任务必定已结束
    async with gsyncio.TaskGroup() as tg:
        h1 = tg.start_soon(fetch, "fast", 0.01)
        h2 = tg.start_soon(fetch, "slow", 0.03)
    print("TaskGroup:", await h1, "|", await h2)

    # 2. fail_after：整体限时，超时抛 TimeoutError
    try:
        async with gsyncio.fail_after(0.05):
            await fetch("too-slow", 10)  # 永远完不成
    except gsyncio.TimeoutError:
        print("fail_after: 已超时 (TimeoutError)")

    # 3. move_on_after：超时静默跳过，不抛异常
    async with gsyncio.move_on_after(0.05) as scope:
        await fetch("also-slow", 10)
    print("move_on_after: 已跳过, cancelled_caught =", scope.cancelled_caught)

    # 4. CancelScope：手动取消 + shield 保护清理代码
    scope = gsyncio.CancelScope()

    async def cancellable() -> None:
        async with scope:
            try:
                await asyncio.sleep(10)
            finally:
                # 即使被取消，清理代码也会执行完（shield 语义在内部生效）
                print("  清理代码执行完毕")

    task = asyncio.create_task(cancellable())
    await asyncio.sleep(0.05)
    scope.cancel()  # 手动触发取消
    try:
        await task
    except asyncio.CancelledError:
        print("CancelScope: 任务已取消")


if __name__ == "__main__":
    asyncio.run(main())
