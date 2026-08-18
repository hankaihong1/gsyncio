"""ASGI 3.0, WebSocket & FastAPI application worker adapter."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import struct
import types
from collections.abc import Callable
from http import HTTPStatus
from typing import Any, Self, cast

from multiloop._rust import _try_import_rust_class
from multiloop._sync import Event, Lock
from multiloop.pool import EventLoopThreadPool
from multiloop.primitives import Channel
from multiloop.server import ConnectionPinningServer

_FastHttpParser = _try_import_rust_class("multiloop._multiloop_core", "FastHttpParser")
_serve_h2_connection = _try_import_rust_class("multiloop._multiloop_core", "serve_h2_connection")

# Hard ceiling for request bodies to prevent worker OOM
_MAX_REQUEST_BODY = 1024 * 1024
_HEADER_TIMEOUT = 30.0
_WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


async def _dispatch_h2_stream(app: Any, scope: dict[str, Any], bridge: Any) -> None:
    """Dispatches an incoming HTTP/2 stream to the ASGI application."""

    async def receive() -> dict[str, Any]:
        if hasattr(bridge, "try_recv_body_chunk"):
            while True:
                has_item, chunk = bridge.try_recv_body_chunk()
                if has_item:
                    if chunk is None:
                        return {"type": "http.request", "body": b"", "more_body": False}
                    return {"type": "http.request", "body": bytes(chunk), "more_body": True}
                await asyncio.sleep(0.001)
        elif hasattr(bridge, "recv_body_chunk_async"):
            chunk = await bridge.recv_body_chunk_async()
            if chunk is None:
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.request", "body": bytes(chunk), "more_body": True}
        else:
            chunk = bridge.recv_body_chunk()
            if asyncio.iscoroutine(chunk):
                chunk = await chunk
            if chunk is None:
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.request", "body": bytes(chunk), "more_body": True}

    status_code = 200
    resp_headers: list[tuple[bytes, bytes]] = []

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code, resp_headers
        m_type = message["type"]
        if m_type == "http.response.start":
            status_code = message["status"]
            resp_headers = list(message.get("headers", []))
        elif m_type == "http.response.body":
            body = message.get("body", b"")
            more_body = message.get("more_body", False)
            bridge.send_response(status_code, resp_headers, list(body), more_body)

    try:
        await app(scope, receive, send)
    except Exception:
        bridge.send_response(500, [], list(b"Internal Server Error"), False)


# Ensure pyright recognizes _dispatch_h2_stream as an active module symbol
__all__ = ["MultiloopASGIWorker", "_dispatch_h2_stream"]


class MultiloopASGIWorker:
    """ASGI 3.0 Worker adapter for mounting FastAPI, Starlette, Litestar and Django Channels applications.

    Supports full ASGI 3.0 Lifespan protocol, HTTP/1.1 chunked streaming, and RFC 6455 full-duplex WebSockets.

    :param app:
        The ASGI 3.0 application callable (e.g. `FastAPI()`).

    :param pool:
        The `EventLoopThreadPool` instance to execute HTTP requests.

    :param host:
        Host address to listen on. Defaults to `"127.0.0.1"`.
    :type host: str

    :param port:
        Port number to listen on. Defaults to 0 (ephemeral port).
    :type port: int

    :param lifespan:
        Lifespan handling mode: `"auto"`, `"on"`, or `"off"`. Defaults to `"auto"`.
    :type lifespan: str
    """

    port: int

    def __init__(
        self,
        app: Callable[..., Any],
        pool: EventLoopThreadPool,
        host: str = "127.0.0.1",
        port: int = 0,
        lifespan: str = "auto",
    ) -> None:
        self.app = app
        self.pool = pool
        self.host = host
        self.port = port
        self.lifespan = lifespan
        self._server = ConnectionPinningServer(
            pool=pool, host=host, port=port, handler=self._handle_connection
        )
        self._lifespan_started = False
        self._lifespan_task: asyncio.Task[Any] | None = None
        self._shutdown_lifespan_hook: Callable[[], Any] | None = None

    def __repr__(self) -> str:
        return f"<MultiloopASGIWorker host={self.host} port={self.port} running={self.is_running}>"

    @property
    def is_running(self) -> bool:
        """Return whether the ASGI server is running.

        :returns: ``True`` if active, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        return self._server.is_running

    async def _run_lifespan_startup(self) -> None:
        """Execute ASGI 3.0 lifespan.startup protocol."""
        if self.lifespan == "off":
            return

        startup_event = Event()
        startup_status: dict[str, Any] = {
            "complete": False,
            "failed": False,
            "message": "",
            "exception": None,
        }
        shutdown_trigger = Event()
        shutdown_event = Event()
        shutdown_status: dict[str, Any] = {"complete": False, "failed": False, "message": ""}

        scope: dict[str, Any] = {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "state": {},
        }

        startup_sent = False

        async def lifespan_receive() -> dict[str, Any]:
            nonlocal startup_sent
            if not startup_sent:
                startup_sent = True
                return {"type": "lifespan.startup"}
            await shutdown_trigger.wait()
            return {"type": "lifespan.shutdown"}

        async def lifespan_send(message: dict[str, Any]) -> None:
            m_type = message.get("type", "")
            if m_type == "lifespan.startup.complete":
                startup_status["complete"] = True
                startup_event.set()
            elif m_type == "lifespan.startup.failed":
                startup_status["failed"] = True
                startup_status["message"] = message.get("message", "")
                startup_event.set()
            elif m_type == "lifespan.shutdown.complete":
                shutdown_status["complete"] = True
                shutdown_event.set()
            elif m_type == "lifespan.shutdown.failed":
                shutdown_status["failed"] = True
                shutdown_status["message"] = message.get("message", "")
                shutdown_event.set()

        loop = asyncio.get_running_loop()

        async def _lifespan_runner() -> None:
            try:
                await self.app(scope, lifespan_receive, lifespan_send)
            except Exception as exc:
                if not startup_event.is_set():
                    startup_status["exception"] = exc
                    startup_status["message"] = str(exc)
                    startup_event.set()
                if not shutdown_event.is_set():
                    shutdown_status["failed"] = True
                    shutdown_status["message"] = str(exc)
                    shutdown_event.set()

        self._lifespan_task = loop.create_task(_lifespan_runner())

        startup_timeout = 10.0 if self.lifespan == "on" else 0.05
        # Wait for startup with timeout
        try:
            await asyncio.wait_for(startup_event.wait(), timeout=startup_timeout)
        except TimeoutError:
            if self.lifespan == "on":
                raise RuntimeError("Lifespan startup timed out") from None
            # auto mode: lifespan not supported, cancel task and continue
            if self._lifespan_task and not self._lifespan_task.done():
                self._lifespan_task.cancel()
            return

        if startup_status["failed"]:
            msg = startup_status.get("message", "Lifespan startup failed")
            raise RuntimeError(f"Application startup failed: {msg}")

        if startup_status["exception"] is not None:
            if self.lifespan == "on":
                exc = startup_status["exception"]
                raise RuntimeError(f"Application startup failed: {exc}") from exc
            # auto mode: app does not support lifespan, cancel task and continue normally
            if self._lifespan_task and not self._lifespan_task.done():
                self._lifespan_task.cancel()
            return

        self._lifespan_started = True

        # Store shutdown trigger hook
        async def _trigger_shutdown() -> None:
            shutdown_trigger.set()
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=10.0)
            except Exception:
                pass
            if self._lifespan_task and not self._lifespan_task.done():
                self._lifespan_task.cancel()
                try:
                    await self._lifespan_task
                except (asyncio.CancelledError, Exception):
                    pass

        self._shutdown_lifespan_hook = _trigger_shutdown

    async def start(self) -> None:
        """Start the ASGI HTTP/WebSocket Server worker with Lifespan support."""
        if not self._lifespan_started:
            await self._run_lifespan_startup()

        await self._server.start(self._handle_connection)
        self.port = self._server.port

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        client_addr: tuple[str, int] | None = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        try:
            method = "GET"
            path = "/"
            query_string = ""
            http_version = "1.1"
            headers: list[tuple[bytes, bytes]] = []
            headers_dict: dict[str, str] = {}
            content_length = 0
            is_chunked_req = False
            body = b""
            initial_body = b""

            if _FastHttpParser is not None:
                raw_buf = bytearray()
                while True:
                    chunk = await asyncio.wait_for(reader.read(4096), timeout=_HEADER_TIMEOUT)
                    if not chunk:
                        return
                    raw_buf.extend(chunk)

                    # Detect HTTP/2 Connection Preface (RFC 9113)
                    if raw_buf.startswith(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"):
                        if _serve_h2_connection is not None:
                            sock = writer.get_extra_info("socket")
                            if sock is not None:
                                fd = os.dup(sock.fileno())
                                transport = writer.transport
                                if isinstance(transport, asyncio.ReadTransport):
                                    transport.pause_reading()
                                client_host, client_port = (
                                    client_addr if client_addr else ("127.0.0.1", 0)
                                )
                                server_host, server_port = self.host, self.port
                                done_event = asyncio.Event()
                                _serve_h2_connection(
                                    fd,
                                    bytes(raw_buf),
                                    self.app,
                                    loop,
                                    str(client_host),
                                    int(client_port),
                                    str(server_host),
                                    int(server_port),
                                    done_event.set,
                                )
                                await done_event.wait()
                        return

                    parsed = _FastHttpParser.parse_request(bytes(raw_buf))
                    if parsed is not None:
                        method, path, query_string, http_version, raw_headers, body_offset = parsed
                        headers = [(k, v) for k, v in raw_headers]
                        headers_dict = {
                            k.decode("latin1"): v.decode("latin1") for k, v in raw_headers
                        }
                        cl_str = headers_dict.get("content-length", "")
                        if cl_str:
                            try:
                                content_length = int(cl_str)
                            except ValueError:
                                content_length = 0
                        te_str = headers_dict.get("transfer-encoding", "").lower()
                        if "chunked" in te_str:
                            is_chunked_req = True
                        initial_body = bytes(raw_buf[body_offset:])
                        if not is_chunked_req and content_length > 0:
                            needed = content_length - len(initial_body)
                            if needed > 0:
                                extra = await asyncio.wait_for(
                                    reader.readexactly(needed), timeout=_HEADER_TIMEOUT
                                )
                                body = initial_body + extra
                            else:
                                body = initial_body[:content_length]
                        else:
                            body = initial_body
                        break
            else:
                req_line = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT)
                if not req_line:
                    return

                parts = req_line.decode("latin1").split()
                method = parts[0] if len(parts) > 0 else "GET"
                raw_target = parts[1] if len(parts) > 1 else "/"
                http_version = parts[2].replace("HTTP/", "") if len(parts) > 2 else "1.1"

                if "?" in raw_target:
                    path, query_string = raw_target.split("?", 1)
                else:
                    path, query_string = raw_target, ""

                while True:
                    line = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT)
                    if not line or line == b"\r\n":
                        break
                    if b":" in line:
                        k, v = line.decode("latin1").split(":", 1)
                        k_str = k.strip().lower()
                        v_str = v.strip()
                        headers_dict[k_str] = v_str
                        if k_str == "content-length":
                            try:
                                content_length = int(v_str)
                            except ValueError:
                                content_length = 0
                        elif k_str == "transfer-encoding" and "chunked" in v_str.lower():
                            is_chunked_req = True
                        headers.append((k_str.encode("latin1"), v_str.encode("latin1")))

                if not is_chunked_req and content_length > 0:
                    body = await asyncio.wait_for(
                        reader.readexactly(content_length), timeout=_HEADER_TIMEOUT
                    )

            # Detect WebSocket Upgrade
            upgrade_val = headers_dict.get("upgrade", "").lower()
            conn_val = headers_dict.get("connection", "").lower()
            is_websocket = "websocket" in upgrade_val and "upgrade" in conn_val

            if is_websocket:
                await self._handle_websocket(
                    reader=reader,
                    writer=writer,
                    path=path,
                    query_string=query_string,
                    headers=headers,
                    headers_dict=headers_dict,
                    client_addr=client_addr,
                    http_version=http_version,
                )
                return

            # Standard HTTP Request
            if not is_chunked_req and content_length > _MAX_REQUEST_BODY:
                writer.write(
                    b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
                await writer.drain()
                return

            # Build ASGI 3.0 HTTP Scope
            scope: dict[str, Any] = {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.0"},
                "http_version": http_version,
                "method": method,
                "path": path,
                "raw_path": path.encode("latin1"),
                "query_string": query_string.encode("latin1"),
                "headers": headers,
                "client": client_addr or (self.host, 0),
                "server": (self.host, self._server.port),
            }

            body_delivered = False
            chunk_finished = False
            disconnect_event = Event()
            chunk_buffer = bytearray(initial_body)

            async def _read_bytes(n: int) -> bytes:
                nonlocal chunk_buffer
                if len(chunk_buffer) >= n:
                    res = bytes(chunk_buffer[:n])
                    chunk_buffer = chunk_buffer[n:]
                    return res
                needed = n - len(chunk_buffer)
                res = bytes(chunk_buffer)
                chunk_buffer.clear()
                extra = await asyncio.wait_for(reader.readexactly(needed), timeout=_HEADER_TIMEOUT)
                return res + extra

            async def _read_line() -> bytes:
                nonlocal chunk_buffer
                if b"\n" in chunk_buffer:
                    idx = chunk_buffer.index(b"\n") + 1
                    res = bytes(chunk_buffer[:idx])
                    chunk_buffer = chunk_buffer[idx:]
                    return res
                line_extra = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT)
                res = bytes(chunk_buffer) + line_extra
                chunk_buffer.clear()
                return res

            async def _read_next_chunk() -> tuple[bytes, bool]:
                size_line = await _read_line()
                if not size_line:
                    return b"", False
                size_str = size_line.decode("latin1").strip().split(";")[0]
                try:
                    chunk_size = int(size_str, 16)
                except ValueError:
                    return b"", False
                if chunk_size == 0:
                    await _read_line()
                    return b"", False
                chunk_data = await _read_bytes(chunk_size)
                await _read_bytes(2)  # \r\n
                return chunk_data, True

            home_loop = asyncio.get_running_loop()

            async def receive() -> dict[str, Any]:
                cur_loop = asyncio.get_running_loop()
                if cur_loop is not home_loop:
                    fut = asyncio.run_coroutine_threadsafe(receive(), home_loop)
                    return await asyncio.wrap_future(fut)

                nonlocal body_delivered, chunk_finished
                if not is_chunked_req:
                    if not body_delivered:
                        body_delivered = True
                        return {"type": "http.request", "body": body, "more_body": False}
                    await disconnect_event.wait()
                    return {"type": "http.disconnect"}

                if not chunk_finished:
                    chunk_data, has_more = await _read_next_chunk()
                    if not has_more:
                        chunk_finished = True
                        return {"type": "http.request", "body": chunk_data, "more_body": False}
                    return {"type": "http.request", "body": chunk_data, "more_body": True}

                await disconnect_event.wait()
                return {"type": "http.disconnect"}

            headers_sent = False
            is_chunked = False
            status_code = 200
            resp_headers: list[tuple[bytes, bytes]] = []

            async def send(message: dict[str, Any]) -> None:
                nonlocal headers_sent, is_chunked, status_code, resp_headers
                cur_loop = asyncio.get_running_loop()
                if cur_loop is not home_loop:
                    fut = asyncio.run_coroutine_threadsafe(send(message), home_loop)
                    await asyncio.wrap_future(fut)
                    return
                m_type = message["type"]
                if m_type == "http.response.start":
                    status_code = message["status"]
                    resp_headers = list(message.get("headers", []))
                elif m_type == "http.response.body":
                    chunk = message.get("body", b"")
                    more_body = message.get("more_body", False)

                    if not headers_sent:
                        headers_sent = True
                        try:
                            reason = HTTPStatus(status_code).phrase
                        except ValueError:
                            reason = "OK"

                        has_cl = any(k.lower() == b"content-length" for k, _ in resp_headers)
                        if not more_body and not has_cl:
                            resp_headers.append(
                                (b"content-length", str(len(chunk)).encode("latin1"))
                            )
                            is_chunked = False
                        elif more_body and not has_cl:
                            resp_headers.append((b"transfer-encoding", b"chunked"))
                            is_chunked = True

                        resp_headers.append((b"connection", b"close"))
                        header_lines = [f"HTTP/1.1 {status_code} {reason}"]
                        header_lines.extend(
                            f"{k.decode('latin1')}: {v.decode('latin1')}" for k, v in resp_headers
                        )
                        writer.write(("\r\n".join(header_lines) + "\r\n\r\n").encode("latin1"))

                    if is_chunked:
                        if chunk:
                            writer.write(f"{len(chunk):X}\r\n".encode("latin1") + chunk + b"\r\n")
                        if not more_body:
                            writer.write(b"0\r\n\r\n")
                    else:
                        if chunk:
                            writer.write(chunk)

                    await writer.drain()

            try:
                await self.app(scope, receive, send)
            finally:
                disconnect_event.set()

        except Exception:  # noqa: BLE001
            try:
                err_resp = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 21\r\n\r\nInternal Server Error"
                writer.write(err_resp)
                await writer.drain()
            except Exception:  # noqa: BLE001, S110
                pass

    async def _handle_websocket(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        path: str,
        query_string: str,
        headers: list[tuple[bytes, bytes]],
        headers_dict: dict[str, str],
        client_addr: tuple[str, int] | None,
        http_version: str,
    ) -> None:
        """Handle RFC 6455 Full-Duplex WebSocket Protocol connection."""
        ws_key = headers_dict.get("sec-websocket-key", "")
        if not ws_key:
            writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            return

        accept_key = base64.b64encode(
            hashlib.sha1(ws_key.encode("latin1") + _WS_MAGIC).digest()
        ).decode("latin1")

        subprotocols = [
            s.strip()
            for s in headers_dict.get("sec-websocket-protocol", "").split(",")
            if s.strip()
        ]

        scope: dict[str, Any] = {
            "type": "websocket",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "http_version": http_version,
            "scheme": "ws",
            "path": path,
            "raw_path": path.encode("latin1"),
            "query_string": query_string.encode("latin1"),
            "headers": headers,
            "client": client_addr or (self.host, 0),
            "server": (self.host, self._server.port),
            "subprotocols": subprotocols,
        }

        # Concurrency & Frame Synchronization
        # multiloop.Lock guarantees atomic non-interleaved frame writing across coroutines / loops
        send_lock = Lock()
        inbound_channel: Channel = Channel(maxsize=256)
        connect_sent = False
        handshake_done = Event()
        closed_event = Event()

        async def _send_frame(opcode: int, payload: bytes) -> None:
            async with send_lock:
                header = bytearray()
                header.append(0x80 | (opcode & 0x0F))
                length = len(payload)
                if length < 126:
                    header.append(length)
                elif length <= 0xFFFF:
                    header.append(126)
                    header.extend(struct.pack("!H", length))
                else:
                    header.append(127)
                    header.extend(struct.pack("!Q", length))
                writer.write(bytes(header) + payload)
                await writer.drain()

        async def ws_receive() -> dict[str, Any]:
            nonlocal connect_sent
            if not connect_sent:
                connect_sent = True
                return {"type": "websocket.connect"}
            if closed_event.is_set() and inbound_channel.empty():
                return {"type": "websocket.disconnect", "code": 1000}
            try:
                item: Any = await inbound_channel.recv()
                return cast(dict[str, Any], item)
            except Exception:
                return {"type": "websocket.disconnect", "code": 1006}

        home_loop = asyncio.get_running_loop()

        async def ws_send(message: dict[str, Any]) -> None:
            cur_loop = asyncio.get_running_loop()
            if cur_loop is not home_loop:
                fut = asyncio.run_coroutine_threadsafe(ws_send(message), home_loop)
                await asyncio.wrap_future(fut)
                return
            m_type = message.get("type", "")
            if m_type == "websocket.accept":
                subproto = message.get("subprotocol")
                res_lines = [
                    "HTTP/1.1 101 Switching Protocols",
                    "Upgrade: websocket",
                    "Connection: Upgrade",
                    f"Sec-WebSocket-Accept: {accept_key}",
                ]
                if subproto:
                    res_lines.append(f"Sec-WebSocket-Protocol: {subproto}")
                res_lines.extend(["", ""])
                writer.write("\r\n".join(res_lines).encode("latin1"))
                await writer.drain()
                handshake_done.set()
            elif m_type == "websocket.send":
                if "text" in message and message["text"] is not None:
                    payload = message["text"].encode("utf-8")
                    await _send_frame(0x1, payload)
                elif "bytes" in message and message["bytes"] is not None:
                    payload = message["bytes"]
                    await _send_frame(0x2, payload)
            elif m_type == "websocket.close":
                code = message.get("code", 1000)
                reason = message.get("reason", "")
                payload = struct.pack("!H", code) + reason.encode("utf-8")
                try:
                    await _send_frame(0x8, payload)
                except Exception:
                    pass
                closed_event.set()
                writer.close()

        # Background frame reader
        async def _ws_reader_loop() -> None:
            await handshake_done.wait()
            try:
                while not closed_event.is_set():
                    head = await reader.readexactly(2)
                    b1, b2 = head[0], head[1]
                    opcode = b1 & 0x0F
                    masked = (b2 & 0x80) != 0
                    payload_len = b2 & 0x7F

                    if payload_len == 126:
                        len_bytes = await reader.readexactly(2)
                        payload_len = struct.unpack("!H", len_bytes)[0]
                    elif payload_len == 127:
                        len_bytes = await reader.readexactly(8)
                        payload_len = struct.unpack("!Q", len_bytes)[0]

                    mask_key = b""
                    if masked:
                        mask_key = await reader.readexactly(4)

                    payload = await reader.readexactly(payload_len) if payload_len > 0 else b""
                    if masked and mask_key:
                        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

                    if opcode == 0x1:  # Text
                        await inbound_channel.send(
                            {
                                "type": "websocket.receive",
                                "text": payload.decode("utf-8"),
                                "bytes": None,
                            }
                        )
                    elif opcode == 0x2:  # Binary
                        await inbound_channel.send(
                            {"type": "websocket.receive", "bytes": payload, "text": None}
                        )
                    elif opcode == 0x9:  # Ping -> Pong
                        await _send_frame(0xA, payload)
                    elif opcode == 0x8:  # Close
                        code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
                        try:
                            await _send_frame(0x8, payload)
                        except Exception:
                            pass
                        await inbound_channel.send({"type": "websocket.disconnect", "code": code})
                        closed_event.set()
                        break
            except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
                await inbound_channel.send({"type": "websocket.disconnect", "code": 1006})
                closed_event.set()
            except Exception:
                await inbound_channel.send({"type": "websocket.disconnect", "code": 1006})
                closed_event.set()

        loop = asyncio.get_running_loop()
        reader_task = loop.create_task(_ws_reader_loop())

        try:
            await self.app(scope, ws_receive, ws_send)
        finally:
            closed_event.set()
            reader_task.cancel()
            try:
                await reader_task
            except (asyncio.CancelledError, Exception):
                pass
            inbound_channel.close()

    async def close(self) -> None:
        """Stop the ASGI Worker Server and run lifespan shutdown."""
        await self._server.close()
        if self._shutdown_lifespan_hook is not None:
            hook = self._shutdown_lifespan_hook
            self._shutdown_lifespan_hook = None
            await hook()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.close()
