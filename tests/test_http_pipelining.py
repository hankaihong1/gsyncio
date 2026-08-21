"""Tests for HTTP/1.1 Pipelining and serial request queueing."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from multiloop.asgi import MultiloopASGIWorker
from multiloop.pool import EventLoopThreadPool


@pytest.mark.asyncio
async def test_asgi_http_pipelining_serial_execution() -> None:
    """Verify multiple pipelined HTTP requests on a single connection execute sequentially."""
    order_processed: list[int] = []

    async def pipelined_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        path = scope.get("path", "/")
        req_id = int(path.lstrip("/req_"))
        order_processed.append(req_id)
        # Small artificial delay to verify no interleaving
        await asyncio.sleep(0.01)
        resp_body = f"response_{req_id}".encode("latin1")
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(resp_body)).encode("latin1"))],
            }
        )
        await send({"type": "http.response.body", "body": resp_body, "more_body": False})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(pipelined_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)

        num_requests = 10
        # Pipeline all 10 requests at once into the TCP socket
        pipelined_payload = bytearray()
        for i in range(num_requests):
            req_str = f"GET /req_{i} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
            pipelined_payload.extend(req_str.encode("latin1"))

        writer.write(pipelined_payload)
        await writer.drain()

        for i in range(num_requests):
            status_line = await asyncio.wait_for(reader.readline(), timeout=3.0)
            assert b"200 OK" in status_line
            content_len = 0
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=3.0)
                if line.lower().startswith(b"content-length:"):
                    content_len = int(line.split(b":")[1].strip())
                if not line or line == b"\r\n":
                    break
            body = await asyncio.wait_for(reader.readexactly(content_len), timeout=3.0)
            assert body == f"response_{i}".encode("latin1")

        assert order_processed == list(range(num_requests))

        writer.close()
        await writer.wait_closed()
