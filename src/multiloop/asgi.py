"""ASGI 3.0, WebSocket & FastAPI application worker adapter.

Orchestrates multi-protocol ASGI 3.0 workloads across an :class:`~multiloop.pool.EventLoopThreadPool`,
coordinating Lifespan lifecycle (:mod:`multiloop._lifespan`), HTTP/1.1 Keep-Alive state machines
(:mod:`multiloop._http11`), and full-duplex RFC 6455 WebSockets (:mod:`multiloop._websocket`)
under Python 3.14t Free-Threaded (No-GIL) true physical parallelism.
"""

from __future__ import annotations

import asyncio
import types
from typing import TYPE_CHECKING, Any, Self

from multiloop._http11 import Http11Protocol
from multiloop._lifespan import LifespanManager
from multiloop._websocket import WebSocketConnection
from multiloop.server import ConnectionPinningServer

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from multiloop.pool import EventLoopThreadPool

__all__ = ["MultiloopASGIWorker"]


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
        self._lifespan_mgr = LifespanManager(app=app, lifespan=lifespan)
        self._server = ConnectionPinningServer(
            pool=pool,
            host=host,
            port=port,
            protocol_factory=self._protocol_factory,
        )

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
            lifespan_state=self._lifespan_mgr.lifespan_state,
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
            lifespan_state=self._lifespan_mgr.lifespan_state,
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
