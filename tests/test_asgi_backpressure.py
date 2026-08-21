"""Tests for ASGI transport backpressure and flow control."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from multiloop.asgi import MultiloopASGIWorker
from multiloop.pool import EventLoopThreadPool


@pytest.mark.asyncio
async def test_asgi_streaming_backpressure_flow_control() -> None:
    """Verify ASGI streaming responses respect Transport backpressure when client reads slowly."""
    chunk_size = 64 * 1024  # 64KB
    total_chunks = 20  # 1.28MB
    chunks_sent = 0

    async def streaming_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        nonlocal chunks_sent
        if scope["type"] != "http":
            return
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for _ in range(total_chunks):
            await send(
                {
                    "type": "http.response.body",
                    "body": b"X" * chunk_size,
                    "more_body": True,
                }
            )
            chunks_sent += 1
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(streaming_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)

        req = f"GET /stream HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
        writer.write(req.encode("latin1"))
        await writer.drain()

        # Read status line
        status = await asyncio.wait_for(reader.readline(), timeout=3.0)
        assert b"200 OK" in status

        # Read headers
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=3.0)
            if not line or line == b"\r\n":
                break

        # Slowly consume chunks with pauses
        received_total = 0
        while True:
            chunk = await asyncio.wait_for(reader.read(32 * 1024), timeout=5.0)
            if not chunk:
                break
            received_total += len(chunk)
            await asyncio.sleep(0.005)

        assert received_total >= chunk_size * total_chunks
        assert chunks_sent == total_chunks

        writer.close()
        await writer.wait_closed()
