"""Tests for ASGI 3.0 Lifespan protocol & RFC 6455 WebSocket support in MultiloopASGIWorker."""

from __future__ import annotations

import asyncio
import struct
from typing import Any

import pytest

from multiloop.asgi import MultiloopASGIWorker
from multiloop.pool import EventLoopThreadPool

# -- Test ASGI Applications -----------------------------------------------

lifespan_state: dict[str, Any] = {"started": False, "stopped": False}


async def lifespan_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                lifespan_state["started"] = True
                await send({"type": "lifespan.startup.complete"})
            elif msg["type"] == "lifespan.shutdown":
                lifespan_state["stopped"] = True
                await send({"type": "lifespan.shutdown.complete"})
                break
        return

    if scope["type"] == "http":
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok", "more_body": False})


async def failing_lifespan_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    if scope["type"] == "lifespan":
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.failed", "message": "DB connect failed"})


async def websocket_echo_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif msg["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                break
        return

    if scope["type"] == "websocket":
        await receive()  # websocket.connect
        await send({"type": "websocket.accept"})

        while True:
            msg = await receive()
            if msg["type"] == "websocket.disconnect":
                break
            if msg["type"] == "websocket.receive":
                if msg.get("text") is not None:
                    await send({"type": "websocket.send", "text": f"echo:{msg['text']}"})
                elif msg.get("bytes") is not None:
                    await send({"type": "websocket.send", "bytes": b"echo:" + msg["bytes"]})


# -- WebSocket Client Helper ----------------------------------------------


async def ws_connect_and_handshake(
    host: str, port: int, path: str = "/ws"
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Perform RFC 6455 client WebSocket handshake."""
    reader, writer = await asyncio.open_connection(host, port)
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    writer.write(req.encode("latin1"))
    await writer.drain()

    # Read handshake response
    resp = b""
    while b"\r\n\r\n" not in resp:
        line = await reader.readline()
        if not line:
            break
        resp += line

    assert b"101 Switching Protocols" in resp
    assert b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" in resp  # Expected SHA1 accept key
    return reader, writer


async def ws_send_text_frame(writer: asyncio.StreamWriter, text: str) -> None:
    """Send client masked text frame."""
    payload = text.encode("utf-8")
    mask_key = b"\x12\x34\x56\x78"
    header = bytearray([0x81, 0x80 | len(payload)])
    header.extend(mask_key)
    masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    writer.write(bytes(header) + masked_payload)
    await writer.drain()


async def ws_send_binary_frame(writer: asyncio.StreamWriter, data: bytes) -> None:
    """Send client masked binary frame."""
    mask_key = b"\x11\x22\x33\x44"
    header = bytearray([0x82, 0x80 | len(data)])
    header.extend(mask_key)
    masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(data))
    writer.write(bytes(header) + masked_payload)
    await writer.drain()


async def ws_read_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    """Read server unmasked frame."""
    head = await reader.readexactly(2)
    opcode = head[0] & 0x0F
    payload_len = head[1] & 0x7F
    if payload_len == 126:
        payload_len = struct.unpack("!H", await reader.readexactly(2))[0]
    elif payload_len == 127:
        payload_len = struct.unpack("!Q", await reader.readexactly(8))[0]
    payload = await reader.readexactly(payload_len) if payload_len > 0 else b""
    return opcode, payload


# -- Tests ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_asgi_lifespan_startup_and_shutdown() -> None:
    """Verify ASGI Lifespan startup and shutdown events execute properly."""
    lifespan_state["started"] = False
    lifespan_state["stopped"] = False

    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = MultiloopASGIWorker(lifespan_app, pool=pool, port=0)
        await worker.start()
        assert lifespan_state["started"] is True
        assert lifespan_state["stopped"] is False

        await worker.close()
        assert lifespan_state["stopped"] is True


@pytest.mark.asyncio
async def test_asgi_lifespan_startup_failure_raises() -> None:
    """Verify ASGI Lifespan startup failure raises RuntimeError and prevents startup."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = MultiloopASGIWorker(failing_lifespan_app, pool=pool, port=0)
        with pytest.raises(RuntimeError, match="DB connect failed"):
            await worker.start()


@pytest.mark.asyncio
async def test_websocket_text_echo() -> None:
    """Verify WebSocket handshake and full-duplex text message echo."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(websocket_echo_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await ws_connect_and_handshake("127.0.0.1", port)

        # Send text frame
        await ws_send_text_frame(writer, "hello_websocket")

        # Receive echo frame
        opcode, data = await ws_read_frame(reader)
        assert opcode == 0x1  # Text
        assert data.decode("utf-8") == "echo:hello_websocket"

        # Send second frame
        await ws_send_text_frame(writer, "second_message")
        opcode2, data2 = await ws_read_frame(reader)
        assert opcode2 == 0x1
        assert data2.decode("utf-8") == "echo:second_message"

        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_websocket_binary_echo() -> None:
    """Verify WebSocket binary frame communication."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(websocket_echo_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await ws_connect_and_handshake("127.0.0.1", port)

        binary_data = bytes([0x01, 0x02, 0x03, 0x04, 0xFF])
        await ws_send_binary_frame(writer, binary_data)

        opcode, data = await ws_read_frame(reader)
        assert opcode == 0x2  # Binary
        assert data == b"echo:" + binary_data

        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_websocket_cross_loop_broadcast() -> None:
    """Verify thread-safe cross-event-loop WebSocket send invocation."""
    send_callable_box: list[Any] = []

    async def broadcast_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "websocket":
            await receive()
            await send({"type": "websocket.accept"})
            send_callable_box.append(send)
            # Keep connection alive until disconnected
            while True:
                msg = await receive()
                if msg["type"] == "websocket.disconnect":
                    break

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(broadcast_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await ws_connect_and_handshake("127.0.0.1", port)

        # Wait for send callable to be captured
        while not send_callable_box:
            await asyncio.sleep(0.01)
        captured_send = send_callable_box[0]

        # Call captured_send from a DIFFERENT worker event loop thread (pin_to=1)
        async def remote_sender() -> None:
            await captured_send({"type": "websocket.send", "text": "broadcast_from_loop_1"})

        fut = pool.submit(remote_sender, pin_to=1)
        await asyncio.wrap_future(fut)

        # Verify client received the broadcast message
        opcode, data = await ws_read_frame(reader)
        assert opcode == 0x1
        assert data.decode("utf-8") == "broadcast_from_loop_1"
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_http11_keepalive_multiple_requests() -> None:
    """Test that a single TCP socket can serve multiple sequential HTTP/1.1 requests (Keep-Alive)."""

    async def echo_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        path = scope.get("path", "/")
        body = f"hello from {path}".encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/plain"),
                    (b"content-length", str(len(body)).encode("latin1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})

    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = MultiloopASGIWorker(app=echo_app, pool=pool, port=0)
        async with worker:
            port = worker.port
            reader, writer = await asyncio.open_connection("127.0.0.1", port)

            for i in range(5):
                req = f"GET /item_{i} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
                writer.write(req.encode("latin1"))
                await writer.drain()

                # Read HTTP response status
                status_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                assert b"200 OK" in status_line

                # Read headers until \r\n
                headers = {}
                while True:
                    line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                    if not line or line == b"\r\n":
                        break
                    k, v = line.decode("latin1").split(":", 1)
                    headers[k.strip().lower()] = v.strip().lower()

                cl = int(headers.get("content-length", "0"))
                body = await asyncio.wait_for(reader.readexactly(cl), timeout=2.0)
                assert body == f"hello from /item_{i}".encode()
                # Connection should NOT be forced closed
                assert headers.get("connection") != "close"

            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_http11_explicit_connection_close() -> None:
    """Test that Connection: close header cleanly terminates the Keep-Alive connection."""

    async def simple_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain"), (b"content-length", b"2")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok", "more_body": False})

    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = MultiloopASGIWorker(app=simple_app, pool=pool, port=0)
        async with worker:
            port = worker.port
            reader, writer = await asyncio.open_connection("127.0.0.1", port)

            req = f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
            writer.write(req.encode("latin1"))
            await writer.drain()

            status_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            assert b"200 OK" in status_line

            headers = {}
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if not line or line == b"\r\n":
                    break
                k, v = line.decode("latin1").split(":", 1)
                headers[k.strip().lower()] = v.strip().lower()

            assert headers.get("connection") == "close"
            body = await asyncio.wait_for(reader.read(1024), timeout=2.0)
            assert body == b"ok"

            # Server must have closed connection (EOF on read)
            eof = await asyncio.wait_for(reader.read(1024), timeout=2.0)
            assert eof == b""

            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_http11_keepalive_post_and_get() -> None:
    """Test sequential POST with body followed by GET on the same Keep-Alive socket."""

    async def api_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        method = scope.get("method", "GET")
        if method == "POST":
            req_body = bytearray()
            while True:
                msg = await receive()
                req_body.extend(msg.get("body", b""))
                if not msg.get("more_body", False):
                    break
            resp = b"received:" + bytes(req_body)
        else:
            resp = b"get_success"

        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/plain"),
                    (b"content-length", str(len(resp)).encode("latin1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": resp, "more_body": False})

    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = MultiloopASGIWorker(app=api_app, pool=pool, port=0)
        async with worker:
            port = worker.port
            reader, writer = await asyncio.open_connection("127.0.0.1", port)

            # Request 1: POST
            post_body = b"sample_payload_123"
            post_req = (
                f"POST /api HTTP/1.1\r\n"
                f"Host: 127.0.0.1:{port}\r\n"
                f"Content-Length: {len(post_body)}\r\n\r\n"
            ).encode("latin1") + post_body
            writer.write(post_req)
            await writer.drain()

            status = await asyncio.wait_for(reader.readline(), timeout=2.0)
            assert b"200 OK" in status
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if not line or line == b"\r\n":
                    break
            resp1 = await asyncio.wait_for(
                reader.readexactly(len(b"received:" + post_body)), timeout=2.0
            )
            assert resp1 == b"received:sample_payload_123"

            # Request 2: GET on the same socket
            get_req = f"GET /api HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n".encode("latin1")
            writer.write(get_req)
            await writer.drain()

            status2 = await asyncio.wait_for(reader.readline(), timeout=2.0)
            assert b"200 OK" in status2
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if not line or line == b"\r\n":
                    break
            resp2 = await asyncio.wait_for(reader.readexactly(len(b"get_success")), timeout=2.0)
            assert resp2 == b"get_success"

            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_lifespan_state_propagation() -> None:
    """Verify that lifespan scope['state'] values are inherited by HTTP and WebSocket scopes."""

    async def stateful_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            while True:
                msg = await receive()
                if msg["type"] == "lifespan.startup":
                    scope["state"]["db_conn"] = "connected_pool_123"
                    await send({"type": "lifespan.startup.complete"})
                elif msg["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    break
            return

        if scope["type"] == "http":
            db = scope.get("state", {}).get("db_conn", "missing")
            body = db.encode("latin1")
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-length", str(len(body)).encode("latin1"))],
                }
            )
            await send({"type": "http.response.body", "body": body, "more_body": False})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(app=stateful_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(f"GET /state HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n".encode("latin1"))
        await writer.drain()

        status = await asyncio.wait_for(reader.readline(), timeout=2.0)
        assert b"200 OK" in status
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            if not line or line == b"\r\n":
                break
        res_body = await asyncio.wait_for(
            reader.readexactly(len(b"connected_pool_123")), timeout=2.0
        )
        assert res_body == b"connected_pool_123"
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_websocket_continuation_frames() -> None:
    """Verify RFC 6455 continuation frame (0x0) fragmented message reassembly."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(websocket_echo_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await ws_connect_and_handshake("127.0.0.1", port)

        # Send fragmented text: Frame 1 (FIN=0, opcode=1, text="hello_"), Frame 2 (FIN=1, opcode=0, text="world")
        part1 = b"hello_"
        mask1 = b"\x12\x34\x56\x78"
        h1 = bytearray([0x01, 0x80 | len(part1)])  # FIN=0, opcode=0x1
        h1.extend(mask1)
        m_part1 = bytes(b ^ mask1[i % 4] for i, b in enumerate(part1))
        writer.write(bytes(h1) + m_part1)
        await writer.drain()

        await asyncio.sleep(0.01)

        part2 = b"world"
        mask2 = b"\x22\x33\x44\x55"
        h2 = bytearray([0x80, 0x80 | len(part2)])  # FIN=1, opcode=0x0 (Continuation)
        h2.extend(mask2)
        m_part2 = bytes(b ^ mask2[i % 4] for i, b in enumerate(part2))
        writer.write(bytes(h2) + m_part2)
        await writer.drain()

        # Read echo response (should be unfragmented "echo:hello_world")
        head = await asyncio.wait_for(reader.readexactly(2), timeout=2.0)
        b1, b2 = head[0], head[1]
        assert (b1 & 0x0F) == 0x1  # Text frame
        length = b2 & 0x7F
        resp_payload = await asyncio.wait_for(reader.readexactly(length), timeout=2.0)
        assert resp_payload == b"echo:hello_world"

        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_http11_split_body_packets() -> None:
    """Verify HTTP/1.1 body split across multiple TCP packets does not cause header reparsing."""

    async def echo_body_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        body = bytearray()
        while True:
            msg = await receive()
            body.extend(msg.get("body", b""))
            if not msg.get("more_body", False):
                break
        body_bytes = bytes(body)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(body_bytes)).encode("latin1"))],
            }
        )
        await send({"type": "http.response.body", "body": body_bytes, "more_body": False})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(app=echo_body_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)

        full_payload = b"A" * 1000 + b"B" * 1000
        header_part = (
            f"POST /split HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{port}\r\n"
            f"Content-Length: {len(full_payload)}\r\n\r\n"
        ).encode("latin1")

        # Send headers only
        writer.write(header_part)
        await writer.drain()
        await asyncio.sleep(0.02)

        # Send first 1000 bytes
        writer.write(full_payload[:1000])
        await writer.drain()
        await asyncio.sleep(0.02)

        # Send remaining 1000 bytes
        writer.write(full_payload[1000:])
        await writer.drain()

        status = await asyncio.wait_for(reader.readline(), timeout=2.0)
        assert b"200 OK" in status
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            if not line or line == b"\r\n":
                break
        res_body = await asyncio.wait_for(reader.readexactly(len(full_payload)), timeout=2.0)
        assert res_body == full_payload

        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_lifespan_off_fast_shutdown() -> None:
    """Verify that lifespan='off' starts up and shuts down immediately without 10s timeout."""
    import time

    async with EventLoopThreadPool(num_threads=2) as pool:
        worker = MultiloopASGIWorker(app=lifespan_app, pool=pool, port=0, lifespan="off")
        start_t = time.monotonic()
        await worker.start()
        await worker.close()
        elapsed = time.monotonic() - start_t
        assert elapsed < 1.0, f"Shutdown took too long with lifespan='off': {elapsed}s"


@pytest.mark.asyncio
async def test_websocket_pre_handshake_rejection_http_403() -> None:
    """Verify that websocket.close before websocket.accept returns HTTP 403 Forbidden."""

    async def reject_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "websocket":
            await receive()
            await send({"type": "websocket.close", "code": 4403})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(reject_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        req = (
            f"GET /ws HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            f"Sec-WebSocket-Version: 13\r\n\r\n"
        )
        writer.write(req.encode("latin1"))
        await writer.drain()

        resp_status = await asyncio.wait_for(reader.readline(), timeout=2.0)
        assert b"403 Forbidden" in resp_status
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_http11_crlf_header_injection_rejection() -> None:
    """Verify that CRLF in response headers is blocked."""

    async def crlf_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"X-Injected", b"val\r\nInjected-Header: evil")],
                }
            )
            await send({"type": "http.response.body", "body": b"ok"})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(crlf_app, pool=pool, port=0) as worker,
    ):
        reader, writer = await asyncio.open_connection("127.0.0.1", worker.port)
        writer.write(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        await writer.drain()

        resp = await reader.read(4096)
        assert b"500 Internal Server Error" in resp
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_http11_conflicting_content_length_400() -> None:
    """Verify that multiple conflicting Content-Length headers trigger 400 Bad Request."""

    async def simple_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(simple_app, pool=pool, port=0) as worker,
    ):
        reader, writer = await asyncio.open_connection("127.0.0.1", worker.port)
        writer.write(
            b"POST / HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Content-Length: 5\r\n"
            b"Content-Length: 10\r\n\r\n"
            b"12345"
        )
        await writer.drain()

        resp = await reader.read(4096)
        assert b"400 Bad Request" in resp
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_asgi_http11_cl_te_smuggling_400() -> None:
    """Verify that simultaneous Content-Length and Transfer-Encoding: chunked trigger 400 Bad Request."""

    async def simple_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(simple_app, pool=pool, port=0) as worker,
    ):
        reader, writer = await asyncio.open_connection("127.0.0.1", worker.port)
        writer.write(
            b"POST / HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Content-Length: 5\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
            b"0\r\n\r\n"
        )
        await writer.drain()

        resp = await reader.read(4096)
        assert b"400 Bad Request" in resp
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_websocket_invalid_version_426() -> None:
    """Verify that Sec-WebSocket-Version != 13 returns 426 Upgrade Required."""

    async def dummy_ws_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        pass

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(dummy_ws_app, pool=pool, port=0) as worker,
    ):
        reader, writer = await asyncio.open_connection("127.0.0.1", worker.port)
        req = (
            f"GET /ws HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{worker.port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            f"Sec-WebSocket-Version: 8\r\n\r\n"
        )
        writer.write(req.encode("latin1"))
        await writer.drain()

        resp = await reader.read(4096)
        assert b"426 Upgrade Required" in resp
        assert b"Sec-WebSocket-Version: 13" in resp
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_websocket_invalid_utf8_close_1007() -> None:
    """Verify that invalid UTF-8 payload in text frame triggers RFC 6455 1007 Close frame."""
    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(websocket_echo_app, pool=pool, port=0) as worker,
    ):
        reader, writer = await ws_connect_and_handshake("127.0.0.1", worker.port)

        # Send masked text frame with invalid UTF-8 bytes: \xff\xfe
        mask = [0x11, 0x22, 0x33, 0x44]
        raw_payload = b"\xff\xfe\xfd"
        masked_payload = bytes(b ^ mask[i % 4] for i, b in enumerate(raw_payload))
        frame = bytearray([0x81, 0x80 | len(raw_payload)]) + bytearray(mask) + masked_payload
        writer.write(frame)
        await writer.drain()

        # Read Close Frame from server (opcode 0x8, code 1007)
        resp_head = await asyncio.wait_for(reader.readexactly(2), timeout=2.0)
        assert resp_head[0] == 0x88  # FIN + Close opcode
        resp_len = resp_head[1] & 0x7F
        close_payload = await reader.readexactly(resp_len)
        close_code = struct.unpack("!H", close_payload[:2])[0]
        assert close_code == 1007

        writer.close()
        await writer.wait_closed()
