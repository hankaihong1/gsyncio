"""multiloop._http11 — Event-Driven HTTP/1.1 Protocol & Keep-Alive State Machine.

Provides high-throughput HTTP/1.1 request handling using asyncio.Protocol and Transport,
bypassing StreamReader overhead, with SIMD header acceleration, coarse timestamp caching,
single-write vector buffer fusion, and thread-safe cross-loop trampolining under Python 3.14t.
"""

from __future__ import annotations

import asyncio
import enum
import os
import sys
import time
from email.utils import formatdate
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, cast

from multiloop._rust import _try_import_rust_class

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

_FastHttpParser = _try_import_rust_class("multiloop._multiloop_core", "FastHttpParser")

_HEADER_TIMEOUT = 30.0
_KEEPALIVE_TIMEOUT = 5.0
_MAX_KEEPALIVE_REQUESTS = 10000
_MAX_REQUEST_BODY = 10 * 1024 * 1024  # 10MB default

_STATUS_LINES: dict[int, bytes] = {
    200: b"HTTP/1.1 200 OK\r\n",
    201: b"HTTP/1.1 201 Created\r\n",
    204: b"HTTP/1.1 204 No Content\r\n",
    301: b"HTTP/1.1 301 Moved Permanently\r\n",
    302: b"HTTP/1.1 302 Found\r\n",
    304: b"HTTP/1.1 304 Not Modified\r\n",
    400: b"HTTP/1.1 400 Bad Request\r\n",
    401: b"HTTP/1.1 401 Unauthorized\r\n",
    403: b"HTTP/1.1 403 Forbidden\r\n",
    404: b"HTTP/1.1 404 Not Found\r\n",
    405: b"HTTP/1.1 405 Method Not Allowed\r\n",
    413: b"HTTP/1.1 413 Payload Too Large\r\n",
    500: b"HTTP/1.1 500 Internal Server Error\r\n",
    502: b"HTTP/1.1 502 Bad Gateway\r\n",
    503: b"HTTP/1.1 503 Service Unavailable\r\n",
}
_SERVER_HEADER = b"server: multiloop\r\n"
_CONN_KEEP_ALIVE = b"connection: keep-alive\r\n"
_CONN_CLOSE = b"connection: close\r\n"
_TE_CHUNKED = b"transfer-encoding: chunked\r\n"
_CRLF = b"\r\n"


class _HttpClock:
    """Lock-free coarse timestamp clock for HTTP Date headers under Python 3.14t."""

    _last_time: float = 0.0
    _cached_date_header: bytes = b""

    @classmethod
    def get_date_header(cls) -> bytes:
        now = time.time()
        if now - cls._last_time >= 1.0 or not cls._cached_date_header:
            cls._last_time = now
            cls._cached_date_header = (
                b"date: " + formatdate(now, usegmt=True).encode("latin1") + b"\r\n"
            )
        return cls._cached_date_header


class HTTPState(enum.Enum):
    """Explicit state of an HTTP/1.1 connection."""

    WAITING_HEADER = "WAITING_HEADER"
    RECEIVING_BODY = "RECEIVING_BODY"
    SENDING_RESPONSE = "SENDING_RESPONSE"
    CLOSED = "CLOSED"


class Http11Protocol(asyncio.Protocol):
    """Event-driven high-throughput HTTP/1.1 Protocol adhering to asyncio.Protocol semantics.

    Eliminates StreamReader intermediate buffering and asyncio.wait_for timer heap overhead.
    """

    def __init__(
        self,
        app: Callable[
            [
                dict[str, Any],
                Callable[[], Coroutine[Any, Any, dict[str, Any]]],
                Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
            ],
            Coroutine[Any, Any, None],
        ],
        host: str = "127.0.0.1",
        port: int = 0,
        websocket_handler: Callable[..., Coroutine[Any, Any, None]] | None = None,
        is_worker_running: Callable[[], bool] | None = None,
        h2_dispatcher: Callable[[int, asyncio.Transport], Any] | None = None,
    ) -> None:
        self.app = app
        self.host = host
        self.port = port
        self.websocket_handler = websocket_handler
        self.is_worker_running = is_worker_running or (lambda: True)
        self.h2_dispatcher = h2_dispatcher

        self.transport: asyncio.Transport | None = None
        self._home_loop: asyncio.AbstractEventLoop | None = None
        self._client_addr: tuple[str, int] | None = None
        self._raw_buf = bytearray()
        self._request_count = 0
        self._timeout_handle: asyncio.TimerHandle | None = None
        self._current_task: asyncio.Task[Any] | None = None
        self._is_handling = False
        self._closed = False
        self._paused = False

        # Streaming body queue (if request body is stream/chunked)
        self._body_queue: asyncio.Queue[tuple[bytes, bool]] | None = None
        self._disconnect_event: asyncio.Event | None = None
        self._closed_event = asyncio.Event()
        self._h2_checked = False
        self._upgraded_protocol: asyncio.BaseProtocol | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.Transport, transport)
        self._home_loop = asyncio.get_running_loop()
        peer_obj = self.transport.get_extra_info("peername")
        if isinstance(peer_obj, tuple):
            tuple_peer = cast("tuple[object, ...]", peer_obj)
            if len(tuple_peer) >= 2:
                self._client_addr = (str(tuple_peer[0]), int(str(tuple_peer[1])))
            else:
                self._client_addr = (self.host, 0)
        else:
            self._client_addr = (self.host, 0)
        self._arm_timeout(_HEADER_TIMEOUT)

    def data_received(self, data: bytes) -> None:
        if self._upgraded_protocol is not None:
            data_recv = getattr(self._upgraded_protocol, "data_received", None)
            if callable(data_recv):
                data_recv(data)
            return

        if self._closed:
            return
        self._disarm_timeout()
        self._raw_buf.extend(data)

        # Check HTTP/2 Client Preface once on first data arrival
        if not self._h2_checked:
            if len(self._raw_buf) >= 24:
                self._h2_checked = True
                if (
                    self._raw_buf.startswith(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
                    and self.h2_dispatcher is not None
                    and self.transport is not None
                ):
                    sock = self.transport.get_extra_info("socket")
                    if sock is not None:
                        if sys.platform == "win32":
                            try:
                                import _winapi

                                cur_proc = _winapi.GetCurrentProcess()
                                fd = int(
                                    _winapi.DuplicateHandle(
                                        cur_proc,
                                        sock.fileno(),
                                        cur_proc,
                                        0,
                                        True,
                                        _winapi.DUPLICATE_SAME_ACCESS,
                                    )
                                )
                            except Exception:
                                fd = int(sock.fileno())
                        else:
                            fd = os.dup(sock.fileno())
                        self.transport.pause_reading()
                        self.h2_dispatcher(fd, self.transport)
                        return
            elif not self._raw_buf.startswith(b"PRI * HTTP/2.0"[: len(self._raw_buf)]):
                self._h2_checked = True

        # If currently streaming request body to an active app:
        if self._body_queue is not None:
            self._feed_streaming_body()
            return

        # If not currently handling a request, attempt to parse and start next request
        if not self._is_handling:
            self._try_parse_and_dispatch()

    def _arm_timeout(self, delay: float) -> None:
        self._disarm_timeout()
        if self._home_loop is not None and not self._closed:
            self._timeout_handle = self._home_loop.call_later(delay, self._on_timeout)

    def _disarm_timeout(self) -> None:
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None

    def _on_timeout(self) -> None:
        if not self._closed:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._disarm_timeout()
        if self._disconnect_event is not None:
            self._disconnect_event.set()
        if self._current_task is not None and not self._current_task.done():
            self._current_task.cancel()
        if self.transport is not None and not self.transport.is_closing():
            self.transport.close()
        self._closed_event.set()

    def connection_lost(self, exc: Exception | None) -> None:
        if self._upgraded_protocol is not None:
            conn_lost = getattr(self._upgraded_protocol, "connection_lost", None)
            if callable(conn_lost):
                conn_lost(exc)
        self._closed = True
        self._disarm_timeout()
        if self._disconnect_event is not None:
            self._disconnect_event.set()
        self._closed_event.set()

    async def wait_closed(self) -> None:
        """Wait until the connection is fully closed."""
        await self._closed_event.wait()

    def pause_writing(self) -> None:
        self._paused = True
        if self._upgraded_protocol is not None:
            pause = getattr(self._upgraded_protocol, "pause_writing", None)
            if callable(pause):
                pause()

    def resume_writing(self) -> None:
        self._paused = False
        if self._upgraded_protocol is not None:
            resume = getattr(self._upgraded_protocol, "resume_writing", None)
            if callable(resume):
                resume()

    def _try_parse_and_dispatch(self) -> None:
        if self._closed or self._is_handling or not self._raw_buf:
            return
        if not self.is_worker_running() or self._request_count >= _MAX_KEEPALIVE_REQUESTS:
            self.close()
            return

        if _FastHttpParser is not None:
            try:
                parsed = _FastHttpParser.parse_request(bytes(self._raw_buf))
            except Exception:
                self._send_error_and_close(400, b"Bad Request")
                return
        else:
            parsed = self._fallback_parse_request()

        if parsed is None:
            # Partial request, wait for more data
            self._arm_timeout(_HEADER_TIMEOUT if self._request_count == 0 else _KEEPALIVE_TIMEOUT)
            return

        method, path, query_string, http_version, raw_headers, body_offset = parsed
        self._request_count += 1

        content_length = 0
        is_chunked = False
        keep_alive = http_version == "1.1"
        is_upgrade = False
        upgrade_proto = b""

        for k, v in raw_headers:
            if k == b"content-length":
                try:
                    content_length = int(v)
                except ValueError:
                    content_length = 0
            elif k == b"connection":
                v_lower = v.lower()
                if b"close" in v_lower:
                    keep_alive = False
                elif b"keep-alive" in v_lower:
                    keep_alive = True
                if b"upgrade" in v_lower:
                    is_upgrade = True
            elif k == b"transfer-encoding":
                if b"chunked" in v.lower():
                    is_chunked = True
            elif k == b"upgrade":
                upgrade_proto = v.lower()

        # Check WebSocket upgrade
        if is_upgrade and b"websocket" in upgrade_proto and self.websocket_handler is not None:
            self._is_handling = True
            assert self._home_loop is not None
            self._current_task = self._home_loop.create_task(
                self._handle_websocket_upgrade(
                    path=path,
                    query_string=query_string,
                    http_version=http_version,
                    raw_headers=raw_headers,
                    body_offset=body_offset,
                )
            )
            return

        if not is_chunked and content_length > _MAX_REQUEST_BODY:
            self._send_error_and_close(413, b"Payload Too Large")
            return

        available_body_len = len(self._raw_buf) - body_offset

        if not is_chunked:
            if available_body_len >= content_length:
                # Complete body is already buffered (99% of requests)
                body = bytes(self._raw_buf[body_offset : body_offset + content_length])
                del self._raw_buf[: body_offset + content_length]
                self._is_handling = True
                assert self._home_loop is not None
                self._current_task = self._home_loop.create_task(
                    self._serve_http_cycle(
                        method=method,
                        path=path,
                        query_string=query_string,
                        http_version=http_version,
                        headers=raw_headers,
                        body=body,
                        is_streaming=False,
                        keep_alive=keep_alive,
                    )
                )
            else:
                # Incomplete body, wait for next data chunk
                self._arm_timeout(_HEADER_TIMEOUT)
        else:
            # Chunked transfer encoding
            initial_chunk_data = bytes(self._raw_buf[body_offset:])
            self._raw_buf.clear()
            self._body_queue = asyncio.Queue()
            self._is_handling = True
            assert self._home_loop is not None
            self._current_task = self._home_loop.create_task(
                self._serve_http_cycle(
                    method=method,
                    path=path,
                    query_string=query_string,
                    http_version=http_version,
                    headers=raw_headers,
                    body=b"",
                    is_streaming=True,
                    keep_alive=keep_alive,
                )
            )
            if initial_chunk_data:
                self._raw_buf.extend(initial_chunk_data)
                self._feed_streaming_body()

    def _feed_streaming_body(self) -> None:
        """Parse chunked body from _raw_buf and push to _body_queue."""
        if self._body_queue is None or not self._raw_buf:
            return

        while self._raw_buf:
            idx = self._raw_buf.find(b"\r\n")
            if idx == -1:
                break
            size_line = bytes(self._raw_buf[:idx]).strip().split(b";")[0]
            try:
                chunk_size = int(size_line, 16)
            except ValueError:
                self._body_queue.put_nowait((b"", False))
                self.close()
                return

            if chunk_size == 0:
                del self._raw_buf[: idx + 4]
                self._body_queue.put_nowait((b"", False))
                return

            if len(self._raw_buf) < idx + 2 + chunk_size + 2:
                break  # Incomplete chunk, wait for more data

            chunk_data = bytes(self._raw_buf[idx + 2 : idx + 2 + chunk_size])
            del self._raw_buf[: idx + 2 + chunk_size + 2]
            self._body_queue.put_nowait((chunk_data, True))

    async def _serve_http_cycle(
        self,
        method: str,
        path: str,
        query_string: str,
        http_version: str,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
        is_streaming: bool,
        keep_alive: bool,
    ) -> None:
        assert self._home_loop is not None
        disconnect_event = asyncio.Event()
        self._disconnect_event = disconnect_event

        scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "http_version": http_version,
            "method": method,
            "path": path,
            "raw_path": path.encode("latin1"),
            "query_string": query_string.encode("latin1"),
            "headers": headers,
            "client": self._client_addr or (self.host, 0),
            "server": (self.host, self.port),
        }

        body_delivered = False
        home_loop = self._home_loop
        assert home_loop is not None

        async def receive() -> dict[str, Any]:
            cur_loop = asyncio.get_running_loop()
            if cur_loop is not home_loop:
                fut = asyncio.run_coroutine_threadsafe(receive(), home_loop)
                return await asyncio.wrap_future(fut)

            nonlocal body_delivered
            if not is_streaming:
                if not body_delivered:
                    body_delivered = True
                    return {"type": "http.request", "body": body, "more_body": False}
                await disconnect_event.wait()
                return {"type": "http.disconnect"}

            assert self._body_queue is not None
            chunk_data, more = await self._body_queue.get()
            return {"type": "http.request", "body": chunk_data, "more_body": more}

        headers_sent = False
        is_resp_chunked = False
        status_code = 200
        resp_headers: list[tuple[bytes, bytes]] = []

        async def send(message: dict[str, Any]) -> None:
            nonlocal headers_sent, is_resp_chunked, status_code, resp_headers, keep_alive
            cur_loop = asyncio.get_running_loop()
            if cur_loop is not home_loop:
                fut = asyncio.run_coroutine_threadsafe(send(message), home_loop)
                await asyncio.wrap_future(fut)
                return

            if self._closed or self.transport is None or self.transport.is_closing():
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

                    status_line = _STATUS_LINES.get(status_code)
                    if status_line is None:
                        try:
                            reason = HTTPStatus(status_code).phrase
                        except ValueError:
                            reason = "OK"
                        status_line = f"HTTP/1.1 {status_code} {reason}\r\n".encode("latin1")

                    fused_buf = bytearray(status_line)
                    fused_buf.extend(_SERVER_HEADER)
                    fused_buf.extend(_HttpClock.get_date_header())

                    has_cl = False
                    has_conn = False

                    for k, v in resp_headers:
                        k_lower = k.lower()
                        if k_lower == b"content-length":
                            has_cl = True
                        elif k_lower == b"connection":
                            has_conn = True
                            if b"close" in v.lower():
                                keep_alive = False
                        fused_buf.extend(k)
                        fused_buf.extend(b": ")
                        fused_buf.extend(v)
                        fused_buf.extend(_CRLF)

                    if not more_body and not has_cl:
                        fused_buf.extend(
                            b"content-length: " + str(len(chunk)).encode("latin1") + _CRLF
                        )
                        is_resp_chunked = False
                    elif more_body and not has_cl:
                        fused_buf.extend(_TE_CHUNKED)
                        is_resp_chunked = True

                    if not has_conn:
                        if keep_alive:
                            fused_buf.extend(_CONN_KEEP_ALIVE)
                        else:
                            fused_buf.extend(_CONN_CLOSE)

                    fused_buf.extend(_CRLF)

                    if is_resp_chunked:
                        if chunk:
                            fused_buf.extend(f"{len(chunk):X}\r\n".encode("latin1") + chunk + _CRLF)
                        if not more_body:
                            fused_buf.extend(b"0\r\n\r\n")
                    else:
                        if chunk:
                            fused_buf.extend(chunk)

                    self.transport.write(fused_buf)
                else:
                    if is_resp_chunked:
                        if chunk:
                            self.transport.write(
                                f"{len(chunk):X}\r\n".encode("latin1") + chunk + _CRLF
                            )
                        if not more_body:
                            self.transport.write(b"0\r\n\r\n")
                    else:
                        if chunk:
                            self.transport.write(chunk)

        try:
            await self.app(scope, receive, send)
        except Exception:
            if not headers_sent:
                self._send_error_and_close(500, b"Internal Server Error")
                return
        finally:
            self._body_queue = None
            self._disconnect_event = None
            self._is_handling = False
            self._current_task = None

            if not keep_alive or self._closed:
                self.close()
            else:
                if self._raw_buf:
                    self._try_parse_and_dispatch()
                else:
                    self._arm_timeout(_KEEPALIVE_TIMEOUT)

    def _send_error_and_close(self, status: int, msg: bytes) -> None:
        if self._closed or self.transport is None or self.transport.is_closing():
            return
        status_line = _STATUS_LINES.get(status, f"HTTP/1.1 {status} Error\r\n".encode("latin1"))
        resp = (
            bytearray(status_line)
            + _SERVER_HEADER
            + _CONN_CLOSE
            + b"content-length: "
            + str(len(msg)).encode("latin1")
            + _CRLF
            + _CRLF
            + msg
        )
        self.transport.write(resp)
        self.close()

    async def _handle_websocket_upgrade(
        self,
        path: str,
        query_string: str,
        http_version: str,
        raw_headers: list[tuple[bytes, bytes]],
        body_offset: int,
    ) -> None:
        """Hand off connection to WebSocket state machine."""
        assert self._home_loop is not None
        assert self.transport is not None

        headers_dict = {k.decode("latin1"): v.decode("latin1") for k, v in raw_headers}
        reader = asyncio.StreamReader(loop=self._home_loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=self._home_loop)
        protocol.connection_made(self.transport)
        self._upgraded_protocol = protocol
        self._disarm_timeout()

        initial_body = bytes(self._raw_buf[body_offset:])
        self._raw_buf.clear()
        if initial_body:
            reader.feed_data(initial_body)

        writer = asyncio.StreamWriter(self.transport, protocol, reader, self._home_loop)

        if self.websocket_handler is not None:
            await self.websocket_handler(
                reader=reader,
                writer=writer,
                path=path,
                query_string=query_string,
                headers=raw_headers,
                headers_dict=headers_dict,
                client_addr=self._client_addr,
                http_version=http_version,
            )
        self.close()

    def _fallback_parse_request(
        self,
    ) -> tuple[str, str, str, str, list[tuple[bytes, bytes]], int] | None:
        """Fallback Python HTTP request parser when Rust FastHttpParser is unavailable."""
        idx = self._raw_buf.find(b"\r\n\r\n")
        if idx == -1:
            return None
        header_bytes = bytes(self._raw_buf[:idx])
        body_offset = idx + 4

        lines = header_bytes.split(b"\r\n")
        req_line = lines[0].decode("latin1")
        parts = req_line.split()
        method = parts[0] if len(parts) > 0 else "GET"
        raw_target = parts[1] if len(parts) > 1 else "/"
        http_version = parts[2].replace("HTTP/", "") if len(parts) > 2 else "1.1"

        if "?" in raw_target:
            path, query_string = raw_target.split("?", 1)
        else:
            path, query_string = raw_target, ""

        parsed_headers: list[tuple[bytes, bytes]] = []
        for line in lines[1:]:
            if b":" in line:
                k, v = line.split(b":", 1)
                parsed_headers.append((k.strip().lower(), v.strip()))

        return method, path, query_string, http_version, parsed_headers, body_offset


class HTTP11Connection:
    """Backward-compatible wrapper managing HTTP/1.1 over (reader, writer) pairs."""

    def __init__(
        self,
        app: Callable[
            [
                dict[str, Any],
                Callable[[], Coroutine[Any, Any, dict[str, Any]]],
                Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
            ],
            Coroutine[Any, Any, None],
        ],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        host: str,
        port: int,
        client_addr: tuple[str, int] | None = None,
        is_worker_running: Callable[[], bool] | None = None,
        websocket_handler: Callable[..., Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        self.app = app
        self.reader = reader
        self.writer = writer
        self.host = host
        self.port = port
        self.client_addr = client_addr
        self.is_worker_running = is_worker_running or (lambda: True)
        self.websocket_handler = websocket_handler
        self.state = HTTPState.WAITING_HEADER

    async def run(self, initial_buffer: bytearray | None = None) -> None:
        """Run connection loop using the transport if accessible."""
        proto = Http11Protocol(
            app=self.app,
            host=self.host,
            port=self.port,
            websocket_handler=self.websocket_handler,
            is_worker_running=self.is_worker_running,
        )
        transport = self.writer.transport
        proto.connection_made(transport)
        if initial_buffer:
            proto.data_received(bytes(initial_buffer))

        # Drive data from reader into protocol until EOF
        try:
            while self.is_worker_running() and not proto._closed:
                chunk = await self.reader.read(65536)
                if not chunk:
                    break
                proto.data_received(chunk)
        except Exception:
            pass
        finally:
            proto.close()
            self.state = HTTPState.CLOSED
