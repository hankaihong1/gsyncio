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
