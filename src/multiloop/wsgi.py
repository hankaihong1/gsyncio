"""WSGI 1.0.1 (PEP 3333) worker adapter for synchronous Flask & Django applications."""

from __future__ import annotations

import asyncio
import io
import sys
import types
from collections.abc import Callable, Iterable
from typing import Any, Self

from multiloop._rust import _try_import_rust_class
from multiloop.pool import EventLoopThreadPool
from multiloop.primitives import Channel
from multiloop.server import ConnectionPinningServer

_FastHttpParser = _try_import_rust_class("multiloop._multiloop_core", "FastHttpParser")

# Hard ceiling for request bodies to prevent worker OOM
_MAX_REQUEST_BODY = 1024 * 1024
_HEADER_TIMEOUT = 30.0

__all__ = ["MultiloopWSGIWorker"]


class MultiloopWSGIWorker:
    """WSGI 1.0.1 (PEP 3333) Worker adapter for running synchronous Django, Flask and Bottle applications.

    Offloads synchronous application execution to the `EventLoopThreadPool` worker thread pool,
    bridging streaming responses back to the asynchronous I/O event loops via lock-free `Channel`.

    :param app: The synchronous WSGI application callable `(environ, start_response)`.
    :param pool: The `EventLoopThreadPool` instance to execute network I/O and worker tasks.
    :param host: Host address to listen on (default: "127.0.0.1").
    :param port: Port number to listen on (0 for ephemeral port).
    """

    port: int

    def __init__(
        self,
        app: Callable[[dict[str, Any], Callable[..., Any]], Iterable[bytes]],
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
        return f"<MultiloopWSGIWorker host={self.host} port={self.port} running={self.is_running}>"

    @property
    def is_running(self) -> bool:
        """Return whether the WSGI server is running.

        :returns: ``True`` if active, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        return self._server.is_running

    async def start(self) -> None:
        """Start the WSGI HTTP Server worker."""
        await self._server.start(self._handle_connection)
        self.port = self._server.port

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        client_addr: tuple[str, int] | None = None,
    ) -> None:
        try:
            method = "GET"
            path = "/"
            query_string = ""
            http_version = "1.1"
            headers_dict: dict[str, str] = {}
            content_length = 0
            body = b""

            if _FastHttpParser is not None:
                raw_buf = bytearray()
                while True:
                    chunk = await asyncio.wait_for(reader.read(4096), timeout=_HEADER_TIMEOUT)
                    if not chunk:
                        return
                    raw_buf.extend(chunk)
                    parsed = _FastHttpParser.parse_request(bytes(raw_buf))
                    if parsed is not None:
                        method, path, query_string, http_version, raw_headers, body_offset = parsed
                        headers_dict = {
                            k.decode("latin1"): v.decode("latin1") for k, v in raw_headers
                        }
                        cl_str = headers_dict.get("content-length", "")
                        if cl_str:
                            try:
                                content_length = int(cl_str)
                            except ValueError:
                                content_length = 0
                        initial_body = bytes(raw_buf[body_offset:])
                        if content_length > 0:
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

                if content_length > 0:
                    body = await asyncio.wait_for(
                        reader.readexactly(content_length), timeout=_HEADER_TIMEOUT
                    )

            # Build PEP 3333 WSGI Environ
            client_ip, client_port = client_addr if client_addr else (self.host, 0)
            environ: dict[str, Any] = {
                "REQUEST_METHOD": method,
                "SCRIPT_NAME": "",
                "PATH_INFO": path,
                "QUERY_STRING": query_string,
                "SERVER_NAME": self.host,
                "SERVER_PORT": str(self._server.port),
                "SERVER_PROTOCOL": f"HTTP/{http_version}",
                "REMOTE_ADDR": client_ip,
                "REMOTE_PORT": str(client_port),
                "CONTENT_TYPE": headers_dict.get("content-type", ""),
                "CONTENT_LENGTH": str(content_length) if content_length > 0 else "",
                "wsgi.version": (1, 0),
                "wsgi.url_scheme": "http",
                "wsgi.input": io.BytesIO(body),
                "wsgi.errors": sys.stderr,
                "wsgi.multithread": True,
                "wsgi.multiprocess": False,
                "wsgi.run_once": False,
            }

            # Map headers to HTTP_*
            for k, v in headers_dict.items():
                if k in ("content-type", "content-length"):
                    continue
                header_key = f"HTTP_{k.upper().replace('-', '_')}"
                environ[header_key] = v
            chunk_channel = Channel(maxsize=128)
            status_line: str | None = None
            response_headers: list[tuple[str, str]] = []
            sync_error: list[Exception] = []

            def start_response(
                status: str,
                headers: list[tuple[str, str]],
                exc_info: Any = None,
            ) -> Callable[[bytes], Any]:
                nonlocal status_line, response_headers
                if exc_info:
                    try:
                        if headers_sent:
                            raise exc_info[1].with_traceback(exc_info[2])
                    finally:
                        exc_info = None
                status_line = status
                response_headers = list(headers)

                def write(data: bytes) -> None:
                    if data:
                        chunk_channel.send_sync(data)

                return write

            # Synchronous WSGI execution runner
            def _run_wsgi_sync() -> None:
                try:
                    response_iterable = self.app(environ, start_response)
                    try:
                        for chunk in response_iterable:
                            if chunk:
                                chunk_channel.send_sync(chunk)
                    finally:
                        close_fn: Any = getattr(response_iterable, "close", None)
                        if callable(close_fn):
                            close_fn()
                except Exception as exc:
                    sync_error.append(exc)
                finally:
                    chunk_channel.send_sync(None)

            loop = asyncio.get_running_loop()
            sync_future = loop.run_in_executor(None, _run_wsgi_sync)

            # Consume chunk channel and stream to client asynchronously
            headers_sent = False
            is_chunked = False

            while True:
                try:
                    item = await chunk_channel.recv()
                except Exception:
                    break

                if item is None:
                    # Stream finished (EOF)
                    break

                if not headers_sent:
                    headers_sent = True
                    active_status = status_line or "200 OK"
                    has_cl = any(k.lower() == "content-length" for k, _ in response_headers)
                    if not has_cl:
                        response_headers.append(("transfer-encoding", "chunked"))
                        is_chunked = True
                    response_headers.append(("connection", "close"))

                    header_lines = [f"HTTP/1.1 {active_status}"]
                    header_lines.extend(f"{k}: {v}" for k, v in response_headers)
                    writer.write(("\r\n".join(header_lines) + "\r\n\r\n").encode("latin1"))

                if is_chunked:
                    writer.write(f"{len(item):X}\r\n".encode("latin1") + item + b"\r\n")
                else:
                    writer.write(item)

                await writer.drain()

            await sync_future

            if not headers_sent:
                headers_sent = True
                if sync_error:
                    active_status = "500 Internal Server Error"
                    body_bytes = b"Internal Server Error"
                else:
                    active_status = status_line or "200 OK"
                    body_bytes = b""

                response_headers = [
                    ("content-type", "text/plain"),
                    ("content-length", str(len(body_bytes))),
                    ("connection", "close"),
                ]
                header_lines = [f"HTTP/1.1 {active_status}"]
                header_lines.extend(f"{k}: {v}" for k, v in response_headers)
                writer.write(("\r\n".join(header_lines) + "\r\n\r\n").encode("latin1") + body_bytes)
                await writer.drain()
            elif is_chunked:
                # Terminal chunk
                writer.write(b"0\r\n\r\n")
                await writer.drain()

        except Exception:  # noqa: BLE001
            try:
                err_resp = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 21\r\n\r\nInternal Server Error"
                writer.write(err_resp)
                await writer.drain()
            except Exception:  # noqa: BLE001, S110
                pass

    async def close(self) -> None:
        """Stop the WSGI Worker Server."""
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
