"""Tests for ASGI 3.0 Lifespan protocol & RFC 6455 WebSocket support in MultiloopASGIWorker."""

from __future__ import annotations

import asyncio
import struct
from typing import Any

import h2.config
import h2.connection
import h2.events
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
async def test_asgi_http2_request_response() -> None:
    """Verify native HTTP/2.0 request and response handling."""
    received_scope: dict[str, Any] = {}

    async def h2_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            received_scope.update(scope)
            msg = await receive()
            body_in = msg.get("body", b"")
            resp_body = b"http2_echo:" + body_in
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/plain")],
                }
            )
            await send({"type": "http.response.body", "body": resp_body, "more_body": False})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(h2_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        client_conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
        client_conn.initiate_connection()
        writer.write(client_conn.data_to_send())
        await writer.drain()

        # Synchronize initial HTTP/2 connection preface & settings
        raw = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        client_conn.receive_data(raw)
        to_send = client_conn.data_to_send()
        if to_send:
            writer.write(to_send)
            await writer.drain()

        # Send Stream 1
        headers = [
            (":method", "POST"),
            (":authority", "localhost"),
            (":scheme", "http"),
            (":path", "/api/h2_test?user=alice"),
            ("x-custom-h2", "valid"),
        ]
        client_conn.send_headers(stream_id=1, headers=headers)
        client_conn.send_data(stream_id=1, data=b"hello_http2", end_stream=True)
        writer.write(client_conn.data_to_send())
        await writer.drain()

        resp_status = 0
        resp_data = b""
        stream_ended = False

        for _ in range(50):
            raw = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            if not raw:
                break
            events = client_conn.receive_data(raw)
            for event in events:
                if isinstance(event, h2.events.ResponseReceived):
                    resp_status = int(dict(event.headers).get(b":status", b"0"))
                elif isinstance(event, h2.events.DataReceived):
                    resp_data += event.data
                elif isinstance(event, h2.events.StreamEnded):
                    stream_ended = True
            to_send = client_conn.data_to_send()
            if to_send:
                writer.write(to_send)
                await writer.drain()
            if stream_ended:
                break

        writer.close()
        await writer.wait_closed()

        assert resp_status == 200
        assert resp_data == b"http2_echo:hello_http2"

    assert received_scope["http_version"] == "2"
    assert received_scope["path"] == "/api/h2_test"
    assert received_scope["query_string"] == b"user=alice"


@pytest.mark.asyncio
async def test_asgi_http2_concurrent_multiplexed_streams() -> None:
    """Verify concurrent stream multiplexing over a single HTTP/2 TCP connection."""

    async def echo_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            path = scope["path"]
            msg = await receive()
            body_in = msg.get("body", b"")
            resp_body = f"echo:{path}:".encode("latin1") + body_in
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/plain")],
                }
            )
            await send({"type": "http.response.body", "body": resp_body, "more_body": False})

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        MultiloopASGIWorker(echo_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        client_conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
        client_conn.initiate_connection()
        writer.write(client_conn.data_to_send())
        await writer.drain()

        # Synchronize initial HTTP/2 connection preface & settings
        raw = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        client_conn.receive_data(raw)
        to_send = client_conn.data_to_send()
        if to_send:
            writer.write(to_send)
            await writer.drain()

        # Send Stream 1, 3, 5 concurrently over same connection
        for stream_id, name in [(1, "first"), (3, "second"), (5, "third")]:
            headers = [
                (":method", "POST"),
                (":authority", "localhost"),
                (":scheme", "http"),
                (":path", f"/stream_{name}"),
            ]
            client_conn.send_headers(stream_id=stream_id, headers=headers)
            client_conn.send_data(
                stream_id=stream_id, data=f"data_{name}".encode(), end_stream=True
            )

        writer.write(client_conn.data_to_send())
        await writer.drain()

        stream_responses: dict[int, bytearray] = {1: bytearray(), 3: bytearray(), 5: bytearray()}
        ended_streams: set[int] = set()

        for _ in range(100):
            raw = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            if not raw:
                break
            events = client_conn.receive_data(raw)
            for event in events:
                if isinstance(event, h2.events.DataReceived):
                    stream_responses[event.stream_id].extend(event.data)
                elif isinstance(event, h2.events.StreamEnded):
                    ended_streams.add(event.stream_id)
            to_send = client_conn.data_to_send()
            if to_send:
                writer.write(to_send)
                await writer.drain()
            if len(ended_streams) == 3:
                break

        writer.close()
        await writer.wait_closed()

        assert bytes(stream_responses[1]) == b"echo:/stream_first:data_first"
        assert bytes(stream_responses[3]) == b"echo:/stream_second:data_second"
        assert bytes(stream_responses[5]) == b"echo:/stream_third:data_third"


@pytest.mark.asyncio
async def test_asgi_http2_slow_streaming_body_no_loop_freeze() -> None:
    """Verify that slow streaming request bodies do not freeze the event loop."""
    heartbeat_ticks = 0
    stop_heartbeat = asyncio.Event()

    async def heartbeat_coro() -> None:
        nonlocal heartbeat_ticks
        while not stop_heartbeat.is_set():
            await asyncio.sleep(0.01)
            heartbeat_ticks += 1

    async def stream_receiver_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            while True:
                msg = await receive()
                if msg["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif msg["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    break
            return

        if scope["type"] == "http":
            body_parts: list[bytes] = []
            while True:
                msg = await receive()
                body_parts.append(msg.get("body", b""))
                if not msg.get("more_body", False):
                    break
            total_body = b"".join(body_parts)
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
                    "body": f"received:{len(total_body)}".encode(),
                    "more_body": False,
                }
            )

    async with (
        EventLoopThreadPool(num_threads=1) as pool,
        MultiloopASGIWorker(stream_receiver_app, pool=pool, port=0) as worker,
    ):
        port = worker.port
        # Start heartbeat on the pool loop
        pool_loop = pool._loops[0]
        hb_fut = asyncio.run_coroutine_threadsafe(heartbeat_coro(), pool_loop)

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        client_conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
        client_conn.initiate_connection()
        writer.write(client_conn.data_to_send())
        await writer.drain()

        # Receive server connection preface & settings to synchronize connection state
        raw = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        client_conn.receive_data(raw)
        to_send = client_conn.data_to_send()
        if to_send:
            writer.write(to_send)
            await writer.drain()

        # Send headers without ending stream
        client_conn.send_headers(
            stream_id=1,
            headers=[
                (":method", "POST"),
                (":authority", "localhost"),
                (":scheme", "http"),
                (":path", "/slow_upload"),
            ],
            end_stream=False,
        )
        writer.write(client_conn.data_to_send())
        await writer.drain()

        # Simulate slow chunk arrival
        for _ in range(3):
            await asyncio.sleep(0.03)
            client_conn.send_data(stream_id=1, data=b"chunk_data_", end_stream=False)
            writer.write(client_conn.data_to_send())
            await writer.drain()

        # Final chunk ending stream
        await asyncio.sleep(0.03)
        client_conn.send_data(stream_id=1, data=b"final", end_stream=True)
        writer.write(client_conn.data_to_send())
        await writer.drain()

        resp_data = bytearray()
        stream_ended = False
        for _ in range(50):
            raw = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            if not raw:
                break
            events = client_conn.receive_data(raw)
            for event in events:
                if isinstance(event, h2.events.DataReceived):
                    resp_data.extend(event.data)
                elif isinstance(event, h2.events.StreamEnded):
                    stream_ended = True
            to_send = client_conn.data_to_send()
            if to_send:
                writer.write(to_send)
                await writer.drain()
            if stream_ended:
                break

        stop_heartbeat.set()
        await asyncio.wrap_future(hb_fut)
        writer.close()
        await writer.wait_closed()

        assert b"received:38" in resp_data
        # Heartbeat must have ticked multiple times, confirming event loop was NOT blocked
        assert heartbeat_ticks >= 5, (
            f"Heartbeat ticks too low ({heartbeat_ticks}), loop was starved"
        )


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
