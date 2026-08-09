"""ASGI 3.0 & FastAPI application worker adapter."""

import asyncio
import types
from collections.abc import Callable
from http import HTTPStatus
from typing import Any, Self

from gsyncio.pool import EventLoopThreadPool
from gsyncio.server import ConnectionPinningServer


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
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            req_line = await reader.readline()
            if not req_line:
                return

            parts = req_line.decode("latin1").split()
            method = parts[0] if len(parts) > 0 else "GET"
            path = parts[1] if len(parts) > 1 else "/"

            headers: list[tuple[bytes, bytes]] = []
            content_length = 0
            while True:
                line = await reader.readline()
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

            body = b""
            if content_length > 0:
                body = await reader.readexactly(content_length)

            # Build ASGI 3.0 Scope
            scope: dict[str, Any] = {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.0"},
                "http_version": "1.1",
                "method": method,
                "path": path,
                "raw_path": path.encode("latin1"),
                "query_string": b"",
                "headers": headers,
                "client": ("127.0.0.1", 0),
                "server": (self.host, self.port),
            }

            async def receive() -> dict[str, Any]:
                return {"type": "http.request", "body": body, "more_body": False}

            # ASGI's start message carries no body length, and a response
            # without Content-Length must be close-delimited — that forbids
            # keep-alive and forces clients to read-to-EOF. So buffer the
            # body per connection and emit Content-Length with the final
            # body message (FastAPI/Starlette always send one, possibly empty).
            resp_state: dict[str, Any] = {}

            async def send(message: dict[str, Any]) -> None:
                if message["type"] == "http.response.start":
                    resp_state["status"] = message["status"]
                    resp_state["headers"] = list(message.get("headers", []))
                    resp_state["body"] = bytearray()
                elif message["type"] == "http.response.body":
                    body = message.get("body", b"")
                    if body:
                        resp_state["body"] += body
                    if message.get("more_body", False):
                        return

                    status = resp_state["status"]
                    # A real reason phrase instead of a hardcoded "OK":
                    # "HTTP/1.1 404 OK" is wrong and confuses tools.
                    try:
                        reason = HTTPStatus(status).phrase
                    except ValueError:
                        reason = "OK"
                    headers = resp_state["headers"]
                    if not any(k.lower() == b"content-length" for k, _ in headers):
                        headers = [
                            *headers,
                            (b"content-length", str(len(resp_state["body"])).encode("latin1")),
                        ]
                    header_lines = [f"HTTP/1.1 {status} {reason}"]
                    header_lines.extend(
                        f"{k.decode('latin1')}: {v.decode('latin1')}" for k, v in headers
                    )
                    writer.write(("\r\n".join(header_lines) + "\r\n\r\n").encode("latin1"))
                    if resp_state["body"]:
                        writer.write(bytes(resp_state["body"]))
                    await writer.drain()

            # Call ASGI Application
            await self.app(scope, receive, send)

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
