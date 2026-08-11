"""Connection pinning TCP server."""

from __future__ import annotations

import asyncio
import inspect
import socket
import sys
import threading
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    import concurrent.futures
    import types
    from collections.abc import Callable

    from gsyncio.pool import EventLoopThreadPool


class ConnectionPinningServer:
    """TCP Server that pins incoming client connections to dedicated Worker Event Loop threads.

    :param pool:
        The `EventLoopThreadPool` instance to dispatch connections to.

    :param host:
        Host address to bind to. Defaults to `"127.0.0.1"`.
    :type host: str

    :param port:
        Port number to listen on. Defaults to 0 (ephemeral port).
    :type port: int

    :param handler:
        Optional connection handler coroutine function `(reader, writer)`.
    :type handler: callable or None

    """

    port: int

    def __init__(
        self,
        pool: EventLoopThreadPool,
        host: str = "127.0.0.1",
        port: int = 0,
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Any] | None = None,
    ) -> None:
        self.pool = pool
        self.host = host
        self.port = port
        self.handler = handler
        self._server_socket: socket.socket | None = None
        self._accept_tasks: list[
            concurrent.futures.Future[Any]
        ] = []  # List of concurrent.futures.Future
        # In-flight connection handler tasks, one per accepted socket. They
        # live on the worker loops and are awaited nowhere, so close() must
        # cancel them explicitly or the pool shutdown orphans them
        # ("Task was destroyed but it is pending!").
        # WHY: the set is mutated on worker loops (add/discard callbacks) and
        # iterated from close() on another thread — a bare set races on
        # free-threaded builds (W20), so every access goes through the lock.
        self._conn_tasks: set[asyncio.Task[Any]] = set()
        self._conn_tasks_lock = threading.Lock()
        self._running_lock = threading.Lock()
        self._running = False

    @property
    def is_running(self) -> bool:
        """Return whether the server is running.

        :returns: ``True`` if active, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        with self._running_lock:
            return self._running

    def __repr__(self) -> str:
        return (
            f"<ConnectionPinningServer host={self.host} port={self.port} running={self.is_running}>"
        )

    async def start(
        self,
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Any] | None = None,
    ) -> None:
        """Start listening for client connections.

        :param handler:
            Optional connection handler function `(reader, writer)`.

        Idempotent: calling :meth:`start` while the server is already
        running is a no-op (no second acceptor, port unchanged).  Set the
        handler before the first start — a handler passed to a second
        (no-op) call is not picked up (R2 FIX-17).

        """
        with self._running_lock:
            if self._running:
                return
        if handler is not None:
            self.handler = handler

        async def dummy_h(_r: asyncio.StreamReader, _w: asyncio.StreamWriter) -> None:
            pass

        active_handler = self.handler or dummy_h

        # Using Shared Acceptor (Thundering Herd) architecture
        # Bind one socket, and pass it to all worker loops to accept concurrently.
        # This provides perfect cross-platform load balancing (unlike macOS SO_REUSEPORT bias).
        # WHY: the idempotency check and the bind happen under the SAME lock —
        # a check-then-act across the lock boundary would let two concurrent
        # start() calls both bind (EADDRINUSE) and double-spawn acceptors
        # (R5 FIX-G).
        with self._running_lock:
            if self._running:
                return
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen(128)
            sock.setblocking(False)

            self.port = sock.getsockname()[1]
            self._server_socket = sock
            self._running = True
        self._accept_tasks = []

        # 多 acceptor 共享同一 listener socket（Thundering Herd）：
        # macOS/Linux 的 Selector 支持多个 loop 对同一 fd 做 read 监听，
        # accept 竞争由内核裁决（EWOULDBLOCK 重试）。但 Windows 的
        # Proactor 里一个 socket 只能关联**一个** IOCP（CreateIoCompletionPort
        # 对已关联句柄是 no-op），第二个 loop 的 AcceptEx 完成通知会投递到
        # 第一个 loop 的 IOCP——跨线程完成 future 的调度有竞态：连接被 accept
        # 但 handler 协程可能永不恢复，客户端连接挂起（实测 Windows 上
        # httpx 请求永久挂起）。所以 Windows 上只启动一个 acceptor。
        acceptor_count = 1 if sys.platform == "win32" else self.pool.num_threads
        for i in range(acceptor_count):
            fut = asyncio.run_coroutine_threadsafe(
                self._worker_accept_loop(sock, active_handler), self.pool._get_loop(i)
            )
            self._accept_tasks.append(fut)

    async def _worker_accept_loop(
        self,
        shared_sock: socket.socket,
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Any],
    ) -> None:
        """Dedicated acceptor running on a worker thread sharing the main socket."""
        loop = asyncio.get_running_loop()
        backoff = 0.001
        while self.is_running:
            try:
                client_sock, addr = await loop.sock_accept(shared_sock)
                client_sock.setblocking(False)
                # Pure local execution on the worker's loop! Zero IPC!
                conn_task = loop.create_task(
                    self._run_pinned_connection(client_sock, loop, handler, addr)
                )
                # Track so close() can cancel in-flight handlers; discard is
                # invoked on the worker loop thread, and set ops are
                # thread-safe under both GIL and free-threaded builds.
                with self._conn_tasks_lock:
                    self._conn_tasks.add(conn_task)
                conn_task.add_done_callback(self._discard_conn_task)
                backoff = 0.001  # success resets the error backoff
            except asyncio.CancelledError:
                break
            except (OSError, RuntimeError):
                # EWOULDBLOCK / EAGAIN are handled internally by asyncio,
                # so if we hit OSError here it's likely a real error or
                # thundering herd race condition.  Exponential backoff
                # (1ms → … → 100ms cap) keeps a persistently failing accept
                # from spinning the worker loop (C3).
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 0.1)

    def _discard_conn_task(self, task: asyncio.Task[Any]) -> None:
        """Done-callback: remove *task* from the tracked connection set.

        Also consumes the task's exception: a handler that raises a
        non-OSError (e.g. ValueError) would otherwise surface as "Task
        exception was never retrieved" noise — the failure is intentional
        and already reported by the handler itself (R3 FIX-23).
        """
        # WHY: on 3.14 task.exception() raises CancelledError for cancelled
        # tasks — cancellation is the normal shutdown path here, not an
        # error to log.
        if not task.cancelled():
            try:
                task.exception()
            except asyncio.CancelledError:
                pass
        with self._conn_tasks_lock:
            self._conn_tasks.discard(task)

    async def _run_pinned_connection(
        self,
        client_sock: socket.socket,
        target_loop: asyncio.AbstractEventLoop,
        handler: Callable[..., Any],
        addr: tuple[str, int] | None = None,
    ) -> None:
        """Run connection handler pinned on target_loop with silent disconnect handling."""
        reader = asyncio.StreamReader(loop=target_loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=target_loop)
        transport: asyncio.BaseTransport | None = None

        try:
            transport, _ = await target_loop.connect_accepted_socket(lambda: protocol, client_sock)
            writer = asyncio.StreamWriter(transport, protocol, reader, target_loop)

            # WHY: handlers may opt into the client address via a third
            # parameter (e.g. the ASGI worker for scope["client"]); legacy
            # two-parameter handlers keep working unchanged (S-2).
            if addr is not None:
                try:
                    sig = inspect.signature(handler)
                    handler_arity = len(sig.parameters)
                except (TypeError, ValueError):
                    handler_arity = 2
                if handler_arity >= 3:
                    await handler(reader, writer, addr)
                    return
            await handler(reader, writer)
        except (
            ConnectionResetError,
            BrokenPipeError,
            asyncio.IncompleteReadError,
            OSError,
        ):
            pass
        finally:
            if transport and not transport.is_closing():
                try:
                    transport.close()
                except OSError:
                    pass

    async def close(self) -> None:
        """Stop server listening, cancel in-flight connection handlers."""
        with self._running_lock:
            self._running = False

        if getattr(self, "_accept_tasks", None):
            for fut in self._accept_tasks:
                fut.cancel()
            self._accept_tasks.clear()

        # Cancel connection handlers before the pool stops its loops. Two
        # passes with a per-loop round-trip between them: pass 1 cancels
        # everything tracked so far, the round-trip lets the CancelledError
        # actually unwind each handler (its finally closes the transport),
        # pass 2 catches any task accepted in the shutdown window.
        for _ in range(2):
            with self._conn_tasks_lock:
                tasks = list(self._conn_tasks)
            if not tasks:
                break
            for task in tasks:
                try:
                    task.get_loop().call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    # WHY: the task's loop was already closed (pool shut down
                    # first) — the task is gone with it (R5 FIX-E/G).
                    pass
            for i in range(self.pool.num_threads):
                try:
                    loop = self.pool._get_loop(i)
                except RuntimeError:
                    # WHY: the pool was closed before the server — nothing
                    # left to round-trip; keep going so the socket still
                    # gets closed (R5 FIX-G).
                    continue
                if not loop.is_running():
                    continue
                try:
                    await asyncio.wait_for(
                        asyncio.wrap_future(
                            asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop)
                        ),
                        timeout=2,
                    )
                except Exception:
                    pass

        if getattr(self, "_server_socket", None) and self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
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
