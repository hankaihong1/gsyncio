"""Tests for WSGI 1.0.1 (PEP 3333) worker adapter."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from multiloop.pool import EventLoopThreadPool
from multiloop.wsgi import MultiloopWSGIWorker


def sample_wsgi_app(environ: dict[str, Any], start_response: Any) -> list[bytes]:
    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET")

    if path == "/hello":
        status = "200 OK"
        headers = [("content-type", "text/plain"), ("x-custom-header", "multiloop-wsgi")]
        start_response(status, headers)
        return [b"Hello from WSGI!"]

    if path == "/echo" and method == "POST":
        input_stream = environ.get("wsgi.input")
        body = input_stream.read() if input_stream else b""
        status = "200 OK"
        headers = [("content-type", "application/octet-stream")]
        start_response(status, headers)
        return [b"echo:" + body]

    if path == "/stream":
        status = "200 OK"
        headers = [("content-type", "text/plain")]
        start_response(status, headers)
        return [b"chunk1-", b"chunk2-", b"chunk3"]

    if path == "/error":
        raise ValueError("App error in WSGI")

    status = "404 Not Found"
    headers = [("content-type", "text/plain")]
    start_response(status, headers)
    return [b"Not Found"]


@pytest.mark.asyncio
async def test_wsgi_worker_basic_get() -> None:
    """Verify standard GET request and headers on WSGI worker."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(sample_wsgi_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        assert port > 0

        async with httpx.AsyncClient() as client:
            resp = await client.get(f"http://127.0.0.1:{port}/hello")
            assert resp.status_code == 200
            assert resp.text == "Hello from WSGI!"
            assert resp.headers.get("x-custom-header") == "multiloop-wsgi"

            resp_404 = await client.get(f"http://127.0.0.1:{port}/unknown")
            assert resp_404.status_code == 404
            assert resp_404.text == "Not Found"


@pytest.mark.asyncio
async def test_wsgi_worker_post_body() -> None:
    """Verify POST request body in wsgi.input."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(sample_wsgi_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"http://127.0.0.1:{port}/echo", content=b"ping_payload")
            assert resp.status_code == 200
            assert resp.content == b"echo:ping_payload"


@pytest.mark.asyncio
async def test_wsgi_worker_streaming_chunks() -> None:
    """Verify multi-chunk iterable responses from WSGI app."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(sample_wsgi_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"http://127.0.0.1:{port}/stream")
            assert resp.status_code == 200
            assert resp.text == "chunk1-chunk2-chunk3"


@pytest.mark.asyncio
async def test_wsgi_worker_exception_handling() -> None:
    """Verify 500 status code returned when WSGI app raises exception."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(sample_wsgi_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"http://127.0.0.1:{port}/error")
            assert resp.status_code == 500
            assert "Internal Server Error" in resp.text


@pytest.mark.asyncio
async def test_wsgi_worker_concurrent_clients() -> None:
    """Verify concurrent requests across multiple worker threads."""
    async with (
        EventLoopThreadPool(num_threads=4) as pool,
        MultiloopWSGIWorker(sample_wsgi_app, pool=pool, port=0) as worker,
    ):
        port = worker.port

        async def make_req(idx: int) -> int:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"http://127.0.0.1:{port}/hello")
                return res.status_code

        tasks = [make_req(i) for i in range(20)]
        results = await asyncio.gather(*tasks)
        assert all(r == 200 for r in results)


@pytest.mark.asyncio
async def test_wsgi_worker_payload_too_large() -> None:
    """Verify that requests exceeding max_request_body return 413 Payload Too Large."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(sample_wsgi_app, pool=pool, port=0, max_request_body=1024) as worker,
    ):
        port = worker.port
        async with httpx.AsyncClient() as client:
            large_content = b"x" * 2048
            resp = await client.post(f"http://127.0.0.1:{port}/echo", content=large_content)
            assert resp.status_code == 413


@pytest.mark.asyncio
async def test_wsgi_worker_client_disconnect_no_deadlock() -> None:
    """Verify that client disconnect during streaming does not deadlock worker threads."""

    def infinite_stream_app(environ: dict[str, Any], start_response: Any) -> Any:
        start_response("200 OK", [("content-type", "text/plain")])

        def gen() -> Any:
            for _ in range(500):
                yield b"chunk_data_block_"

        return gen()

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(infinite_stream_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n".encode("latin1"))
        await writer.drain()

        # Read only status and first chunk, then abruptly close connection
        await reader.readline()
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_wsgi_chunked_upload_and_stream_reader() -> None:
    """Verify WSGI correctly decodes client Transfer-Encoding: chunked uploads with SyncStreamReader."""

    def chunked_receiver_app(environ: dict[str, Any], start_response: Any) -> list[bytes]:
        stream = environ["wsgi.input"]
        # Test readline & read
        line1 = stream.readline()
        rest = stream.read()
        assert environ["HTTP_X_CUSTOM_HEADER"] == "val1, val2"
        resp = b"read:" + line1 + b":" + rest
        start_response(
            "200 OK",
            [("content-type", "text/plain"), ("content-length", str(len(resp)))],
        )
        return [resp]

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopWSGIWorker(chunked_receiver_app, pool=pool, host="0.0.0.0", port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        req = (
            f"POST /upload HTTP/1.1\r\n"
            f"Host: testserver:{port}\r\n"
            f"Transfer-Encoding: chunked\r\n"
            f"X-Custom-Header: val1\r\n"
            f"X-Custom-Header: val2\r\n\r\n"
            f"7\r\nhello\r\n\r\n"
            f"6\r\nworld!\r\n"
            f"0\r\n\r\n"
        )
        writer.write(req.encode("latin1"))
        await writer.drain()

        status_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
        assert b"200 OK" in status_line
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            if not line or line == b"\r\n":
                break
        res_body = await asyncio.wait_for(
            reader.readexactly(len(b"read:hello\r\n:world!")), timeout=2.0
        )
        assert res_body == b"read:hello\r\n:world!"

        writer.close()
        await writer.wait_closed()
