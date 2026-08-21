"""Connection-pinning TCP server with multi-core SO_REUSEPORT listener support."""

from __future__ import annotations

import asyncio
import errno
import inspect
import socket
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    import concurrent.futures
    import types
    from collections.abc import Callable

    from multiloop.pool import EventLoopThreadPool

__all__ = ["ConnectionPinningServer"]


class ConnectionPinningServer:
    """TCP Server that pins incoming client connections to dedicated Worker Event Loop threads.

    On Linux kernels supporting ``SO_REUSEPORT``, each worker thread binds an independent socket
    listener on the same port, achieving zero-contention kernel-level connection load balancing.
    On macOS and Windows, automatically falls back to an event-loop-safe shared listener architecture.

    :param pool: The :class:`~multiloop.EventLoopThreadPool` instance to dispatch connections to.
    :param host: Host IP address to bind to (default: "127.0.0.1").
    :param port: Port number to listen on (0 for an OS-assigned ephemeral port).
    :param handler: Callable coroutine function ``(reader, writer)`` or ``(reader, writer, addr)``.
    """

    port: int

    def __init__(
        self,
        pool: EventLoopThreadPool,
        host: str = "127.0.0.1",
        port: int = 0,
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Any] | None = None,
        protocol_factory: Callable[[], asyncio.Protocol] | None = None,
    ) -> None:
        self.pool = pool
        self.host = host
        self.port = port
        self.handler = handler
        self.protocol_factory = protocol_factory
        self._server_sockets: list[socket.socket] = []
        self._server_socket: socket.socket | None = None
        self._accept_tasks: list[concurrent.futures.Future[Any]] = []
        self._worker_conn_tasks: dict[asyncio.AbstractEventLoop, set[asyncio.Task[Any]]] = {}
        self._tasks_lock = threading.Lock()
        self._running_lock = threading.Lock()
        self._running = False
        self._handler_accepts_addr = False

    @property
    def is_running(self) -> bool:
        """Return True if the server is actively listening and processing connections."""
        with self._running_lock:
            return self._running

    def __repr__(self) -> str:
        return (
            f"<ConnectionPinningServer host={self.host} port={self.port} running={self.is_running}>"
        )

    async def start(
        self,
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Any] | None = None,
        protocol_factory: Callable[[], asyncio.Protocol] | None = None,
    ) -> None:
        """Start listening for incoming client connections (idempotent).

        :param handler: Optional handler coroutine function.
        :param protocol_factory: Optional asyncio.Protocol factory.
        """
        with self._running_lock:
            if self._running:
                return
        if handler is not None:
            self.handler = handler
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory

        async def dummy_h(_r: asyncio.StreamReader, _w: asyncio.StreamWriter) -> None:
            pass

        active_handler = self.handler or dummy_h
        try:
            sig = inspect.signature(active_handler)
            self._handler_accepts_addr = len(sig.parameters) >= 3
        except (TypeError, ValueError):
            self._handler_accepts_addr = False

        can_reuseport = (
            sys.platform.startswith("linux")
            and hasattr(socket, "SO_REUSEPORT")
            and self.pool.num_threads > 1
        )

        with self._running_lock:
            if self._running:
                return

            self._server_sockets = []
            self._accept_tasks = []

            if can_reuseport:
                try:
                    sock0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock0.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock0.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    sock0.bind((self.host, self.port))
                    sock0.listen(128)
                    sock0.setblocking(False)

                    self.port = sock0.getsockname()[1]
                    self._server_sockets.append(sock0)
                    self._server_socket = sock0

                    for _ in range(1, self.pool.num_threads):
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                        sock.bind((self.host, self.port))
                        sock.listen(128)
                        sock.setblocking(False)
                        self._server_sockets.append(sock)

                    self._running = True
                except OSError:
                    for s in self._server_sockets:
                        try:
                            s.close()
                        except OSError:
                            pass
                    self._server_sockets.clear()
                    can_reuseport = False

            if not can_reuseport:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((self.host, self.port))
                sock.listen(128)
                sock.setblocking(False)

                self.port = sock.getsockname()[1]
                self._server_sockets.append(sock)
                self._server_socket = sock
                self._running = True

        if can_reuseport:
            for i, sock in enumerate(self._server_sockets):
                with self._running_lock:
                    fut = asyncio.run_coroutine_threadsafe(
                        self._worker_accept_loop(sock, active_handler), self.pool._get_loop(i)
                    )
                    self._accept_tasks.append(fut)
        else:
            shared_sock = self._server_sockets[0]
            acceptor_count = 1 if sys.platform == "win32" else self.pool.num_threads
            for i in range(acceptor_count):
                with self._running_lock:
                    fut = asyncio.run_coroutine_threadsafe(
                        self._worker_accept_loop(shared_sock, active_handler),
                        self.pool._get_loop(i),
                    )
                    self._accept_tasks.append(fut)

    async def _worker_accept_loop(
        self,
        shared_sock: socket.socket,
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Any],
    ) -> None:
        loop = asyncio.get_running_loop()
        with self._tasks_lock:
            worker_tasks = self._worker_conn_tasks.setdefault(loop, set())
        backoff = 0.001
        while self.is_running:
            try:
                client_sock, addr = await loop.sock_accept(shared_sock)
                client_sock.setblocking(False)
                try:
                    client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
                conn_task = loop.create_task(
                    self._run_pinned_connection(client_sock, loop, handler, addr)
                )
                with self._tasks_lock:
                    worker_tasks.add(conn_task)

                def _discard_task(t: asyncio.Task[Any]) -> None:
                    with self._tasks_lock:
                        worker_tasks.discard(t)

                conn_task.add_done_callback(_discard_task)
                backoff = 0.001
            except asyncio.CancelledError:
                break
            except (BlockingIOError, InterruptedError):
                continue
            except OSError as err:
                if err.errno in (
                    errno.EAGAIN,
                    errno.EWOULDBLOCK,
                    errno.ECONNABORTED,
                    errno.EINTR,
                ):
                    continue
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 0.1)
            except RuntimeError:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 0.1)

    async def _run_pinned_connection(
        self,
        client_sock: socket.socket,
        target_loop: asyncio.AbstractEventLoop,
        handler: Callable[..., Any],
        addr: tuple[str, int] | None = None,
    ) -> None:
        transport: asyncio.BaseTransport | None = None
        sock_guard: socket.socket | None = client_sock

        try:
            if self.protocol_factory is not None:
                proto = self.protocol_factory()
                transport, _ = await target_loop.connect_accepted_socket(lambda: proto, client_sock)
                sock_guard = None
                wait_closed_fn: Any = getattr(proto, "wait_closed", None)
                if callable(wait_closed_fn):
                    res: Any = wait_closed_fn()
                    if inspect.isawaitable(res):
                        await res
            else:
                reader = asyncio.StreamReader(loop=target_loop)
                protocol = asyncio.StreamReaderProtocol(reader, loop=target_loop)
                transport, _ = await target_loop.connect_accepted_socket(
                    lambda: protocol, client_sock
                )
                sock_guard = None
                writer = asyncio.StreamWriter(transport, protocol, reader, target_loop)

                if self._handler_accepts_addr and addr is not None:
                    await handler(reader, writer, addr)
                else:
                    await handler(reader, writer)
        except (
            ConnectionResetError,
            BrokenPipeError,
            asyncio.IncompleteReadError,
            OSError,
        ):
            pass
        finally:
            if sock_guard is not None:
                try:
                    sock_guard.close()
                except OSError:
                    pass
            elif transport and not transport.is_closing():
                try:
                    transport.close()
                except OSError:
                    pass

    async def close(self, drain_timeout: float = 5.0) -> None:
        """Stop listening for new connections and cleanly drain all in-flight connections."""
        with self._running_lock:
            self._running = False

        with self._running_lock:
            accept_tasks = list(self._accept_tasks)
            self._accept_tasks.clear()
        for fut in accept_tasks:
            fut.cancel()

        server_socks = list(getattr(self, "_server_sockets", []))
        if not server_socks and self._server_socket is not None:
            server_socks = [self._server_socket]
        for s in server_socks:
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass
        self._server_sockets = []
        self._server_socket = None

        # Allow in-flight requests to drain naturally up to drain_timeout
        deadline = time.monotonic() + drain_timeout
        while time.monotonic() < deadline:
            with self._tasks_lock:
                all_tasks_lists = [list(tasks) for tasks in self._worker_conn_tasks.values()]
            has_active = any(any(not t.done() for t in tasks) for tasks in all_tasks_lists)
            if not has_active:
                break
            await asyncio.sleep(0.05)

        with self._tasks_lock:
            items_snapshot = [
                (loop, list(tasks)) for loop, tasks in self._worker_conn_tasks.items()
            ]
            self._worker_conn_tasks.clear()

        for loop, tasks in items_snapshot:
            for task in tasks:
                try:
                    loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    pass

    async def __aenter__(self) -> Self:
        if not self._running:
            await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.close()
