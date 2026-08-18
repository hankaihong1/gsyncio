"""ASGI 3.0, WebSocket & FastAPI application worker adapter.

Orchestrates multi-protocol ASGI 3.0 workloads across an :class:`~multiloop.pool.EventLoopThreadPool`,
coordinating Lifespan lifecycle (:mod:`multiloop._lifespan`), HTTP/1.1 Keep-Alive state machines
(:mod:`multiloop._http11`), and full-duplex RFC 6455 WebSockets (:mod:`multiloop._websocket`)
under Python 3.14t Free-Threaded (No-GIL) true physical parallelism.
"""

from __future__ import annotations

import asyncio
import types
from typing import TYPE_CHECKING, Any, Self, cast

from multiloop._http11 import Http11Protocol
from multiloop._lifespan import LifespanManager
from multiloop._rust import _try_import_rust_class
from multiloop._websocket import WebSocketConnection
from multiloop.server import ConnectionPinningServer

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from multiloop.pool import EventLoopThreadPool

__all__ = ["MultiloopASGIWorker", "_dispatch_h2_stream"]

_serve_h2_connection = _try_import_rust_class("multiloop._multiloop_core", "serve_h2_connection")


async def _dispatch_h2_stream(app: Any, scope: dict[str, Any], bridge: Any) -> None:
    """Dispatches an incoming HTTP/2 stream to the ASGI application.

    Called by the Rust Tokio HTTP/2 Bridge (:mod:`_multiloop_core.serve_h2_connection`).
    """

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
            if chunk is None:
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.request", "body": bytes(chunk), "more_body": True}

    status = 200
    headers: list[tuple[bytes, bytes]] = []

    async def send(message: dict[str, Any]) -> None:
        nonlocal status, headers
        m_type = message.get("type")
        if m_type == "http.response.start":
            status = message.get("status", 200)
            headers = list(message.get("headers", []))
        elif m_type == "http.response.body":
            body = message.get("body", b"")
            more_body = message.get("more_body", False)
            raw_headers = [(bytes(k), bytes(v)) for k, v in headers]
            bridge.send_response(status, raw_headers, bytes(body), more_body)

    await app(scope, receive, send)


class MultiloopASGIWorker:
    """High-performance ASGI 3.0 server worker for Python 3.14t Free-Threaded (No-GIL).

    Runs across an :class:`~multiloop.pool.EventLoopThreadPool`, binding incoming connections
    to pinned worker event loops using ``ConnectionPinningServer``.

    Supports full ASGI 3.0 Lifespan lifecycle management, WebSockets (RFC 6455),
    HTTP/1.1 Keep-Alive long-connection multiplexing, and SIMD accelerated HTTP parsing.

    :param app: The ASGI 3.0 application callable.
    :param pool: Optional :class:`~multiloop.pool.EventLoopThreadPool` instance.
    :param host: Host interface to bind on (default: ``"127.0.0.1"``).
    :param port: Port to bind on (default: ``8000``; ``0`` for dynamic random port).
    :param lifespan: Lifespan mode: ``"auto"`` (default), ``"on"``, or ``"off"``.
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
        pool: EventLoopThreadPool,
        host: str = "127.0.0.1",
        port: int = 8000,
        lifespan: str = "auto",
    ) -> None:
        self.app = app
        self.pool = pool
        self.host = host
        self.port = port
        self.lifespan = lifespan
        self._server = ConnectionPinningServer(
            pool=pool,
            host=host,
            port=port,
            protocol_factory=self._protocol_factory,
        )
        self._lifespan_mgr = LifespanManager(app=app, lifespan=lifespan)

    def __repr__(self) -> str:
        return f"<MultiloopASGIWorker host={self.host} port={self.port} running={self.is_running}>"

    @property
    def is_running(self) -> bool:
        """Return whether the ASGI server is running.

        :returns: ``True`` if active, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        return self._server.is_running

    async def start(self) -> None:
        """Start the ASGI HTTP/WebSocket Server worker with Lifespan support."""
        await self._lifespan_mgr.startup()
        await self._server.start(protocol_factory=self._protocol_factory)
        self.port = self._server.port

    def _protocol_factory(self) -> asyncio.Protocol:
        """Factory creating Http11Protocol instances for accepted client connections."""
        return Http11Protocol(
            app=self.app,
            host=self.host,
            port=self._server.port,
            websocket_handler=self._handle_websocket,
            is_worker_running=lambda: self.is_running,
            h2_dispatcher=self._dispatch_h2_socket,
        )

    def _dispatch_h2_socket(self, fd: int, transport: asyncio.Transport) -> None:
        """Dispatch HTTP/2 socket connection to the Rust Tokio H2 runtime."""
        if _serve_h2_connection is not None:
            loop = asyncio.get_running_loop()
            peer_obj = transport.get_extra_info("peername")
            if isinstance(peer_obj, tuple):
                tuple_peer = cast("tuple[object, ...]", peer_obj)
                if len(tuple_peer) >= 2:
                    client_host = str(tuple_peer[0])
                    client_port = int(str(tuple_peer[1]))
                else:
                    client_host, client_port = "127.0.0.1", 0
            else:
                client_host, client_port = "127.0.0.1", 0
            server_host, server_port = self.host, self._server.port
            done_event = asyncio.Event()
            _serve_h2_connection(
                fd,
                b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
                self.app,
                loop,
                str(client_host),
                int(client_port),
                str(server_host),
                int(server_port),
                done_event.set,
            )

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
        ws_conn = WebSocketConnection(
            app=self.app,
            reader=reader,
            writer=writer,
            path=path,
            query_string=query_string,
            headers=headers,
            headers_dict=headers_dict,
            client_addr=client_addr,
            server_addr=(self.host, self._server.port),
            http_version=http_version,
        )
        await ws_conn.run()

    async def close(self) -> None:
        """Stop the ASGI Worker Server and run lifespan shutdown."""
        await self._server.close()
        await self._lifespan_mgr.shutdown()

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
