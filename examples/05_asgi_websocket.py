"""Example 05: ASGI 3.0 Lifespan & WebSocket Echo Server.

Demonstrates running an ASGI application with lifespan lifecycle management
and full-duplex WebSocket bidirectional echo communication over multiloop.
"""

import asyncio
from typing import Any

from multiloop import EventLoopThreadPool, MultiloopASGIWorker


async def asgi_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                print("  [Lifespan] Application initialized.")
                await send({"type": "lifespan.startup.complete"})
            elif msg["type"] == "lifespan.shutdown":
                print("  [Lifespan] Application shutting down.")
                await send({"type": "lifespan.shutdown.complete"})
                break
        return

    if scope["type"] == "websocket":
        await receive()  # websocket.connect
        await send({"type": "websocket.accept"})
        print("  [WebSocket] Connection accepted.")

        while True:
            msg = await receive()
            if msg["type"] == "websocket.disconnect":
                print("  [WebSocket] Connection disconnected.")
                break
            if msg["type"] == "websocket.receive":
                text = msg.get("text")
                if text:
                    await send({"type": "websocket.send", "text": f"Echo: {text}"})
        return

    if scope["type"] == "http":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"Hello from multiloop ASGI!",
            }
        )


async def main() -> None:
    print("=== Example 05: ASGI 3.0 Lifespan & WebSocket Server ===")
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(asgi_app, pool=pool, port=0) as worker,
    ):
        print(f"Server listening on http://127.0.0.1:{worker.port}")
        # Brief wait to verify running state
        await asyncio.sleep(0.1)
    print("=== Completed cleanly ===")


if __name__ == "__main__":
    asyncio.run(main())
