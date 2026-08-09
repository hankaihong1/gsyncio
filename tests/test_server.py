import asyncio
import threading

import httpx
import pytest

from gsyncio.asgi import GsyncioASGIWorker
from gsyncio.pool import EventLoopThreadPool
from gsyncio.server import ConnectionPinningServer


@pytest.mark.asyncio
async def test_connection_pinning_server_basic():
    """Verify that all requests on the same TCP connection are pinned to and handled on the same Worker thread loop"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        server = ConnectionPinningServer(pool, host="127.0.0.1", port=0)

        # Client request handler logic: return the current Worker thread ID
        async def handler(reader, writer):
            thread_id = threading.get_ident()
            while True:
                data = await reader.read(100)
                if not data:
                    break
                # Respond to client with the physical thread ID handling this request
                writer.write(f"{thread_id}\n".encode())
                await writer.drain()
            writer.close()
            await writer.wait_closed()

        await server.start(handler)

        port = server.port
        assert port > 0

        # Open connection, send 3 requests
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        handled_threads = []
        for _ in range(3):
            writer.write(b"ping\n")
            await writer.drain()
            line = await reader.readline()
            handled_threads.append(int(line.decode().strip()))

        writer.close()
        await writer.wait_closed()
        await server.close()

        # Verify that all 3 request handlings for this socket connection fall on the same physical thread!
        assert len(handled_threads) == 3
        assert len(set(handled_threads)) == 1


@pytest.mark.asyncio
async def test_server_lifecycle_close():
    """Lifecycle: start() and close() are symmetric and repeatable."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        server = ConnectionPinningServer(pool, host="127.0.0.1", port=0)
        await server.start()
        assert server.is_running

        await server.close()
        assert not server.is_running

        await server.start()
        await server.close()
        assert not server.is_running


# Define a simple compliant ASGI 3.0 async app (simulating FastAPI/Starlette).
# WHY: 模块级辅助函数——供下面两个 GsyncioASGIWorker 测试共用。
async def mock_fastapi_app(scope, receive, send):
    if scope["type"] != "http":
        return

    path = scope.get("path", "/")
    if path == "/json":
        body = b'{"status": "ok", "framework": "FastAPI"}'
        headers = [(b"content-type", b"application/json")]
        status = 200
    elif path == "/echo":
        # Read request Body
        msg = await receive()
        body = msg.get("body", b"")
        headers = [(b"content-type", b"text/plain")]
        status = 200
    else:
        body = b"Not Found"
        headers = [(b"content-type", b"text/plain")]
        status = 404

    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": headers,
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": body,
        }
    )


@pytest.mark.asyncio
async def test_gsyncio_asgi_worker_get_and_post():
    """Test that GsyncioASGIWorker can successfully proxy an ASGI/FastAPI application"""
    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = GsyncioASGIWorker(app=mock_fastapi_app, pool=pool, host="127.0.0.1", port=0)
        await worker.start()
        port = worker.port

        async with httpx.AsyncClient() as client:
            # GET request test
            resp = await client.get(f"http://127.0.0.1:{port}/json")
            assert resp.status_code == 200
            assert resp.json() == {"status": "ok", "framework": "FastAPI"}

            # POST request test
            resp_post = await client.post(f"http://127.0.0.1:{port}/echo", content=b"hello_asgi")
            assert resp_post.status_code == 200
            assert resp_post.text == "hello_asgi"

            # 404 test
            resp_404 = await client.get(f"http://127.0.0.1:{port}/unknown")
            assert resp_404.status_code == 404

        await worker.close()


@pytest.mark.asyncio
async def test_asgi_lifecycle_close():
    """Lifecycle: start() and close() are symmetric and repeatable."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = GsyncioASGIWorker(app=mock_fastapi_app, pool=pool, host="127.0.0.1", port=0)
        await worker.start()
        assert worker.is_running

        await worker.close()
        assert not worker.is_running

        await worker.start()
        await worker.close()
        assert not worker.is_running


@pytest.mark.asyncio
async def test_server_dummy_handler():
    """ConnectionPinningServer uses the internal dummy handler when no handler is
    provided at construction or at start() (line 79)."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        server = ConnectionPinningServer(pool, host="127.0.0.1", port=0)
        # Start without providing a handler — dummy_h is used.
        await server.start()
        port = server.port
        assert port > 0
        # Open and close a connection to exercise the accept → dummy handler path.
        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.close()
        await writer.wait_closed()
        await server.close()


@pytest.mark.asyncio
async def test_asgi_worker_malformed_http():
    """Verify GsyncioASGIWorker fault tolerance for malformed HTTP requests and exceptions"""
    async with EventLoopThreadPool(num_threads=2) as pool:

        async def err_app(scope, receive, send):
            raise RuntimeError("Application Error")

        async with GsyncioASGIWorker(err_app, pool, port=0) as worker:
            assert worker.is_running
            port = worker.port
            reader, writer = await asyncio.open_connection("127.0.0.1", port)

            # Send standard GET request, test 500 fault tolerance
            writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            resp = await reader.read(1024)
            assert b"500 Internal Server Error" in resp
            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_post_request_body():
    """Verify GsyncioASGIWorker reads Content-Length request body"""
    async with EventLoopThreadPool(num_threads=2) as pool:

        async def echo_app(scope, receive, send):
            msg = await receive()
            body = msg.get("body", b"")
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-length", str(len(body)).encode())],
                }
            )
            await send({"type": "http.response.body", "body": body})

        async with GsyncioASGIWorker(echo_app, pool, port=0) as worker:
            reader, writer = await asyncio.open_connection("127.0.0.1", worker.port)
            req = b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 11\r\n\r\nHello World"
            writer.write(req)
            await writer.drain()

            resp = b""
            while b"Hello World" not in resp:
                chunk = await asyncio.wait_for(reader.read(2048), timeout=2.0)
                if not chunk:
                    break
                resp += chunk
            assert b"Hello World" in resp
            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_server_and_asgi_context_manager():
    """Verify engineering standard 3: Server and ASGI Worker both support async with context lifecycle"""
    async with EventLoopThreadPool(num_threads=2) as pool:
        # Test ConnectionPinningServer async with
        async def dummy_handler(reader, writer):
            pass

        async with ConnectionPinningServer(pool, port=0) as server:
            assert server.port > 0

        # Test GsyncioASGIWorker async with
        async def dummy_app(scope, receive, send):
            pass

        async with GsyncioASGIWorker(dummy_app, pool, port=0) as worker:
            assert worker.port > 0


@pytest.mark.asyncio
async def test_vulnerability_server_abrupt_disconnect():
    """Vulnerability 4: Server does not crash when client brutally closes the socket"""
    async with EventLoopThreadPool(num_threads=2) as pool:
        server = ConnectionPinningServer(pool, host="127.0.0.1", port=0)

        async def handler(reader, writer):
            # Try to write data
            writer.write(b"data\n")
            await writer.drain()

        await server.start(handler)
        port = server.port

        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        # Client brutally closes directly, without graceful wait_closed
        writer.close()
        await asyncio.sleep(0.05)

        await server.close()
