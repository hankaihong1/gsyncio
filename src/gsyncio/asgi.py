"""ASGI 3.0 & FastAPI application worker adapter."""

import asyncio
import types
from collections.abc import Callable
from http import HTTPStatus
from typing import Any, Self

from gsyncio.pool import EventLoopThreadPool
from gsyncio.server import ConnectionPinningServer

# WHY: hard ceiling for request bodies — the worker reads the whole body
# before invoking the app, so an unbounded content-length would let a
# client exhaust worker memory (C4).
_MAX_REQUEST_BODY = 1024 * 1024
_HEADER_TIMEOUT = 30.0


class GsyncioASGIWorker:
    """ASGI 3.0 Worker adapter for mounting FastAPI/Starlette applications.

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

    """

    port: int

    def __init__(
        self,
        app: Callable[..., Any],
        pool: EventLoopThreadPool,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self.app = app
        self.pool = pool
        self.host = host
        self.port = port
        self._server = ConnectionPinningServer(
            pool=pool, host=host, port=port, handler=self._handle_connection
        )

    def __repr__(self) -> str:
        return f"<GsyncioASGIWorker host={self.host} port={self.port} running={self.is_running}>"

    @property
    def is_running(self) -> bool:
        """Return whether the ASGI server is running.

        :returns: ``True`` if active, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        return self._server.is_running

    async def start(self) -> None:
        """Start the ASGI HTTP Server worker."""
        await self._server.start(self._handle_connection)
        self.port = self._server.port

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        client_addr: tuple[str, int] | None = None,
    ) -> None:
        try:
            # WHY: a slow-loris client that never finishes its header would
            # pin the worker connection forever — bound the read (C4).
            req_line = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT)
            if not req_line:
                return

            parts = req_line.decode("latin1").split()
            method = parts[0] if len(parts) > 0 else "GET"
            raw_target = parts[1] if len(parts) > 1 else "/"
            if "?" in raw_target:
                path, query_string = raw_target.split("?", 1)
            else:
                path, query_string = raw_target, ""

            headers: list[tuple[bytes, bytes]] = []
            content_length = 0
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT)
                if not line or line == b"\r\n":
                    break
                if b":" in line:
                    k, v = line.decode("latin1").split(":", 1)
                    k_str = k.strip().lower()
                    v_str = v.strip()
                    if k_str == "content-length":
                        try:
                            content_length = int(v_str)
                        except ValueError:
                            content_length = 0
                    headers.append((k_str.encode("latin1"), v_str.encode("latin1")))

            if content_length > _MAX_REQUEST_BODY:
                writer.write(
                    b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
                await writer.drain()
                return

            body = b""
            if content_length > 0:
                body = await asyncio.wait_for(
                    reader.readexactly(content_length), timeout=_HEADER_TIMEOUT
                )

            # Build ASGI 3.0 Scope
            scope: dict[str, Any] = {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.0"},
                "http_version": "1.1",
                "method": method,
                "path": path,
                "raw_path": path.encode("latin1"),
                "query_string": query_string.encode("latin1"),
                "headers": headers,
                "client": client_addr or (self.host, 0),
                # WHY: read the bound port from the server, not self.port —
                # start() assigns self.port after server.start() returns, while
                # handlers may already run on worker loops (TS-7).
                "server": (self.host, self._server.port),
            }

            body_delivered = False
            disconnect_event = asyncio.Event()

            async def receive() -> dict[str, Any]:
                nonlocal body_delivered
                if not body_delivered:
                    body_delivered = True
                    return {"type": "http.request", "body": body, "more_body": False}
                # Once the request body is consumed, subsequent calls block until disconnect
                await disconnect_event.wait()
                return {"type": "http.disconnect"}

            headers_sent = False
            is_chunked = False
            status_code = 200
            resp_headers: list[tuple[bytes, bytes]] = []

            async def send(message: dict[str, Any]) -> None:
                nonlocal headers_sent, is_chunked, status_code, resp_headers
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

            # Call ASGI Application
            try:
                await self.app(scope, receive, send)
            finally:
                disconnect_event.set()

        # Intentionally catch all to prevent worker crash; the 500 response is the error surface
        except Exception:  # noqa: BLE001
            try:
                err_resp = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 21\r\n\r\nInternal Server Error"
                writer.write(err_resp)
                await writer.drain()
            except Exception:  # noqa: BLE001, S110
                pass

    async def close(self) -> None:
        """Stop the ASGI Worker Server."""
        await self._server.close()

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
