"""multiloop._http11 — High-throughput event-driven HTTP/1.1 protocol.

Adheres strictly to asyncio.Protocol semantics, bypassing StreamReader / StreamWriter
overhead and timer heap bottlenecks under Python 3.14t multi-core execution.

Follows the pure messenger pattern: Rust FastHttpConnection handles 100% of
protocol parsing, chunk decoding, pipelining, and wire serialization, while Python
handles pure network transport I/O and ASGI 3.0 coroutine driving.
"""

from __future__ import annotations

import asyncio
import enum
import time
from email.utils import formatdate
from typing import TYPE_CHECKING, Any, cast

from multiloop._rust import _try_import_rust_class

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

_FastHttpConnection = _try_import_rust_class("multiloop._multiloop_core", "FastHttpConnection")
_FastHttpParser = _try_import_rust_class("multiloop._multiloop_core", "FastHttpParser")

# Hard limits
_MAX_REQUEST_BODY = 10 * 1024 * 1024  # 10MB
_MAX_HEADER_SIZE = 64 * 1024  # 64KB
_MAX_KEEPALIVE_REQUESTS = 1000
_QUEUE_MAX_SIZE = 32
_QUEUE_RESUME_SIZE = 16

_SERVER_HEADER = b"server: multiloop\r\n"
_ASGI_SPEC: dict[str, str] = {"version": "3.0", "spec_version": "2.0"}
_EMPTY_BODY_MSG: dict[str, Any] = {"type": "http.request", "body": b"", "more_body": False}
_DISCONNECT_MSG: dict[str, Any] = {"type": "http.disconnect"}


class _HttpClock:
    """Coarse timestamp clock for HTTP Date headers with 1.0s caching."""

    _cached_date_header: bytes = b""
    _last_update: int = 0

    @classmethod
    def get_date_header(cls) -> bytes:
        now_int = int(time.time())
        if now_int != cls._last_update:
            cls._cached_date_header = (
                b"date: " + formatdate(float(now_int), usegmt=True).encode("latin1") + b"\r\n"
            )
            cls._last_update = now_int
        return cls._cached_date_header


class HTTPState(enum.Enum):
    """Explicit state of an HTTP/1.1 connection."""

    WAITING_HEADER = "WAITING_HEADER"
    SERVING_APP = "SERVING_APP"
    KEEP_ALIVE_WAIT = "KEEP_ALIVE_WAIT"
    CLOSED = "CLOSED"


class Http11Protocol(asyncio.Protocol):
    """Event-driven high-throughput HTTP/1.1 Protocol adhering to asyncio.Protocol semantics."""

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
        lifespan_state: dict[str, Any] | None = None,
    ) -> None:
        self.app = app
        self.host = host
        self.port = port
        self.websocket_handler = websocket_handler
        self.is_worker_running = is_worker_running or (lambda: True)
        self.lifespan_state = lifespan_state or {}

        self._server_tuple: tuple[str, int] = (self.host, self.port)
        self._default_client_tuple: tuple[str, int] = (self.host, 0)
        self.state: HTTPState = HTTPState.WAITING_HEADER
        self.transport: asyncio.Transport | None = None
        self._home_loop: asyncio.AbstractEventLoop | None = None
        self._client_addr: tuple[str, int] | None = None
        self._closed_event = asyncio.Event()
        self._drain_event = asyncio.Event()
        self._drain_event.set()

        self._conn = (
            _FastHttpConnection(_MAX_REQUEST_BODY, _MAX_HEADER_SIZE, _MAX_KEEPALIVE_REQUESTS)
            if _FastHttpConnection is not None
            else None
        )
        self._body_queue: asyncio.Queue[tuple[bytes, bool]] | None = None
        self._disconnect_event = asyncio.Event()
        self._current_task: asyncio.Task[None] | None = None
        self._upgraded_protocol: asyncio.Protocol | None = None
        self._pending_pipeline_events: list[tuple[dict[str, Any], bool, bool]] = []

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        self._home_loop = asyncio.get_running_loop()
        peer_raw: object = transport.get_extra_info("peername")
        if isinstance(peer_raw, tuple):
            peer_tuple = cast(tuple[object, ...], peer_raw)
            if len(peer_tuple) >= 2:
                self._client_addr = (str(peer_tuple[0]), int(cast(int, peer_tuple[1])))

    def data_received(self, data: bytes) -> None:
        if self._upgraded_protocol is not None:
            self._upgraded_protocol.data_received(data)
            return

        if self._conn is not None:
            try:
                events = self._conn.feed_data(data)
            except Exception:
                if self.transport is not None:
                    self.transport.write(
                        b"HTTP/1.1 400 Bad Request\r\nserver: multiloop\r\nconnection: close\r\ncontent-length: 0\r\n\r\n"
                    )
                    self.transport.close()
                self.close()
                return

            self._handle_events(events)

    def _handle_events(self, events: list[Any]) -> None:
        for ev in events:
            ev_tag = ev[0]
            if ev_tag == 1:
                # EVENT_REQUEST_START: (1, method, path, raw_path, query, version, headers, keep_alive, is_upgrade, upgrade_proto, has_body)
                (
                    _,
                    method,
                    path,
                    raw_path,
                    query_string,
                    http_version,
                    headers,
                    keep_alive,
                    is_upgrade,
                    upgrade_proto,
                    has_body,
                ) = ev
                scope: dict[str, Any] = {
                    "type": (
                        "websocket" if is_upgrade and upgrade_proto == b"websocket" else "http"
                    ),
                    "asgi": _ASGI_SPEC,
                    "http_version": http_version,
                    "method": method,
                    "scheme": "http",
                    "path": path,
                    "raw_path": raw_path,
                    "query_string": query_string,
                    "root_path": "",
                    "headers": headers,
                    "client": self._client_addr or self._default_client_tuple,
                    "server": self._server_tuple,
                    "state": self.lifespan_state.copy(),
                }
                if scope["type"] == "websocket":
                    self.state = HTTPState.SERVING_APP
                    if self._home_loop is not None:
                        self._current_task = self._home_loop.create_task(
                            self._handle_websocket_upgrade(
                                path=path,
                                query_string=query_string.decode("latin1", "replace"),
                                http_version=http_version,
                                raw_headers=headers,
                            )
                        )
                elif self.state == HTTPState.SERVING_APP:
                    self._pending_pipeline_events.append((scope, keep_alive, has_body))
                else:
                    self.state = HTTPState.SERVING_APP
                    self._body_queue = asyncio.Queue() if has_body else None
                    self._disconnect_event.clear()
                    if self._home_loop is not None:
                        self._current_task = self._home_loop.create_task(
                            self._serve_http_cycle(scope, keep_alive, has_body)
                        )
            elif ev_tag == 2:
                # EVENT_BODY_CHUNK: (2, chunk_bytes, more_body)
                if self._body_queue is not None:
                    self._body_queue.put_nowait((ev[1], ev[2]))
                    if self.transport is not None and self._body_queue.qsize() >= _QUEUE_MAX_SIZE:
                        self.transport.pause_reading()
            elif ev_tag == 3:
                pass
            elif ev_tag == 5:
                # EVENT_PROTOCOL_ERROR: (5, status_code, error_wire_bytes)
                if self.transport is not None:
                    self.transport.write(ev[2])
                    self.transport.close()
                self.close()
            elif ev_tag == 6:
                # EVENT_CLOSE
                if self.transport is not None:
                    self.transport.close()
                self.close()

    async def _serve_http_cycle(
        self, scope: dict[str, Any], req_keep_alive: bool, has_body: bool
    ) -> None:
        resp_status = 0  # 0: initial, 1: headers_sent, 2: streaming, 3: complete
        start_data: dict[str, Any] = {}
        keep_alive = req_keep_alive
        empty_body_delivered = False

        async def receive() -> dict[str, Any]:
            nonlocal empty_body_delivered
            if not has_body:
                if not empty_body_delivered:
                    empty_body_delivered = True
                    return _EMPTY_BODY_MSG
                await self._disconnect_event.wait()
                return _DISCONNECT_MSG

            if self._body_queue is None:
                await self._disconnect_event.wait()
                return _DISCONNECT_MSG

            chunk, more = await self._body_queue.get()
            if not more:
                self._body_queue = None
            elif self.transport is not None and self._body_queue.qsize() <= _QUEUE_RESUME_SIZE:
                self.transport.resume_reading()
                if self._conn is not None:
                    p_events = self._conn.pump_events()
                    if p_events:
                        self._handle_events(p_events)

            return {"type": "http.request", "body": chunk, "more_body": more}

        bytes_written_to_wire = False

        async def send(message: dict[str, Any]) -> None:
            nonlocal resp_status, keep_alive, bytes_written_to_wire
            msg_type = message["type"]
            if msg_type == "http.response.start":
                if resp_status != 0:
                    raise RuntimeError("Duplicate http.response.start called")
                resp_status = 1
                start_data["status"] = message["status"]
                start_data["headers"] = message.get("headers", [])
            elif msg_type == "http.response.body":
                if resp_status not in (1, 2):
                    raise RuntimeError(
                        "http.response.body sent before http.response.start or after completion"
                    )
                body = message.get("body", b"")
                more_body = message.get("more_body", False)
                if resp_status == 1:
                    status_code = start_data["status"]
                    headers = start_data["headers"]
                    is_no_body = status_code in (204, 304) or scope["method"] == "HEAD"
                    if _FastHttpConnection is not None:
                        wire_bytes, _, conn_keep_alive = _FastHttpConnection.format_response(
                            status_code,
                            _SERVER_HEADER,
                            _HttpClock.get_date_header(),
                            headers,
                            body,
                            more_body,
                            is_no_body,
                            keep_alive,
                        )
                        keep_alive = conn_keep_alive
                    else:
                        wire_bytes = b""

                    if self.transport is not None:
                        self.transport.write(wire_bytes)
                    bytes_written_to_wire = True
                    if not self._drain_event.is_set():
                        await self._drain_event.wait()
                    if more_body:
                        resp_status = 2
                    else:
                        resp_status = 3
                        if not keep_alive and self.transport is not None:
                            self.transport.close()
                else:
                    if _FastHttpConnection is not None:
                        chunk_wire = _FastHttpConnection.format_chunk(body, more_body)
                    else:
                        chunk_wire = b""
                    if self.transport is not None:
                        self.transport.write(chunk_wire)
                    bytes_written_to_wire = True
                    if not self._drain_event.is_set():
                        await self._drain_event.wait()
                    if not more_body:
                        resp_status = 3
            else:
                raise ValueError(f"Unknown ASGI message type: {msg_type}")

        try:
            await self.app(scope, receive, send)
        except Exception:
            if (
                not bytes_written_to_wire
                and self.transport is not None
                and not self.transport.is_closing()
            ):
                err_resp = (
                    b"HTTP/1.1 500 Internal Server Error\r\n"
                    + _SERVER_HEADER
                    + _HttpClock.get_date_header()
                    + b"connection: close\r\ncontent-length: 21\r\n\r\nInternal Server Error"
                )
                self.transport.write(err_resp)
                self.transport.close()
        finally:
            self._disconnect_event.set()
            if self._conn is not None:
                self._conn.reset_for_next_request()
                if self._conn.is_closed():
                    if self.transport is not None and not self.transport.is_closing():
                        self.transport.close()
                else:
                    self.state = HTTPState.KEEP_ALIVE_WAIT
                    if self._pending_pipeline_events:
                        next_scope, next_ka, next_hb = self._pending_pipeline_events.pop(0)
                        self.state = HTTPState.SERVING_APP
                        self._body_queue = asyncio.Queue() if next_hb else None
                        self._disconnect_event.clear()
                        if self._home_loop is not None:
                            self._current_task = self._home_loop.create_task(
                                self._serve_http_cycle(next_scope, next_ka, next_hb)
                            )
                    else:
                        p_events = self._conn.pump_events()
                        if p_events:
                            self._handle_events(p_events)

    async def _handle_websocket_upgrade(
        self,
        path: str,
        query_string: str,
        http_version: str,
        raw_headers: list[tuple[bytes, bytes]],
    ) -> None:
        assert self._home_loop is not None
        assert self.transport is not None

        headers_dict = {k.decode("latin1"): v.decode("latin1") for k, v in raw_headers}
        reader = asyncio.StreamReader(loop=self._home_loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=self._home_loop)
        protocol.connection_made(self.transport)
        self._upgraded_protocol = protocol

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

    def pause_writing(self) -> None:
        self._drain_event.clear()

    def resume_writing(self) -> None:
        self._drain_event.set()

    def eof_received(self) -> bool | None:
        if self._upgraded_protocol is not None:
            return bool(self._upgraded_protocol.eof_received())
        return False

    def connection_lost(self, exc: Exception | None) -> None:
        if self._upgraded_protocol is not None:
            self._upgraded_protocol.connection_lost(exc)
        self.close()

    def close(self) -> None:
        if self.state == HTTPState.CLOSED:
            return
        self.state = HTTPState.CLOSED
        self._closed_event.set()
        self._disconnect_event.set()
        if self.transport is not None and not self.transport.is_closing():
            self.transport.close()
        if self._current_task is not None and not self._current_task.done():
            self._current_task.cancel()
        self._current_task = None
        self._drain_event.set()

    async def wait_closed(self) -> None:
        await self._closed_event.wait()
