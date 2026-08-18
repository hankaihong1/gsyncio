"""Example 06: Synchronous WSGI Application (PEP 3333).

Demonstrates running a synchronous WSGI application (such as Flask or Django)
on multiloop's multi-event-loop thread pool with lock-free response streaming.
"""

import asyncio
from typing import Any

from multiloop import EventLoopThreadPool, MultiloopWSGIWorker


def wsgi_app(environ: dict[str, Any], start_response: Any) -> list[bytes]:
    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET")

    status = "200 OK"
    headers = [("content-type", "text/plain; charset=utf-8")]
    start_response(status, headers)

    body = f"WSGI Response: {method} {path} running on multiloop thread pool!\n"
    return [body.encode("utf-8")]


async def main() -> None:
    print("=== Example 06: WSGI Application Worker ===")
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(wsgi_app, pool=pool, port=0) as worker,
    ):
        print(f"WSGI Server listening on http://127.0.0.1:{worker.port}")
        await asyncio.sleep(0.1)
    print("=== Completed cleanly ===")


if __name__ == "__main__":
    asyncio.run(main())
