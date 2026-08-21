"""WSGI 1.0.1 (PEP 3333) worker adapter for synchronous Flask & Django applications."""

from __future__ import annotations

import asyncio
import concurrent.futures
import io
import os
import sys
import threading
import types
import urllib.parse
from collections.abc import Callable, Iterable
from typing import Any, Self, cast

from multiloop._rust import _try_import_rust_class
from multiloop.pool import EventLoopThreadPool
from multiloop.primitives import Channel
from multiloop.server import ConnectionPinningServer

_FastHttpParser = _try_import_rust_class("multiloop._multiloop_core", "FastHttpParser")

# Hard ceilings to prevent worker OOM / Slowloris attacks
_MAX_REQUEST_BODY = 10 * 1024 * 1024  # 10MB
_MAX_HEADER_SIZE = 64 * 1024  # 64KB
_HEADER_TIMEOUT = 30.0

__all__ = ["MultiloopWSGIWorker", "SyncStreamReader"]


class SyncStreamReader(io.RawIOBase):
    """PEP 3333 compliant streaming synchronous stream reader for environ['wsgi.input']."""

    def __init__(self, data: bytes | None = None, channel: Channel | None = None) -> None:
        super().__init__()
        self._channel = channel
        self._buffer = (
            bytearray(data) if (data is not None and channel is not None) else bytearray()
        )
        self._bio = (
            io.BytesIO(data)
            if (data is not None and channel is None)
            else (None if channel is not None else io.BytesIO(b""))
        )
        self._eof = channel is None and (data is None or len(data) == 0)

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def read(self, size: int = -1) -> bytes:
        if self._bio is not None:
            return self._bio.read(size)

        if size == 0:
            return b""
        if size < 0:
            while not self._eof and self._channel is not None:
                try:
                    chunk = self._channel.recv_sync()
                    if not chunk:
                        self._eof = True
                        break
                    self._buffer.extend(chunk)
                except Exception:
                    self._eof = True
                    break
            res = bytes(self._buffer)
            self._buffer.clear()
            return res

        while len(self._buffer) < size and not self._eof and self._channel is not None:
            try:
                chunk = self._channel.recv_sync()
                if not chunk:
                    self._eof = True
                    break
                self._buffer.extend(chunk)
            except Exception:
                self._eof = True
                break

        chunk_res = bytes(self._buffer[:size])
        del self._buffer[:size]
        return chunk_res

    def readline(self, size: int | None = -1) -> bytes:
        if self._bio is not None:
            return self._bio.readline(-1 if size is None else size)

        max_len = size if (size is not None and size > 0) else None
        while True:
            nl_idx = self._buffer.find(b"\n")
            if nl_idx != -1:
                end = nl_idx + 1
                if max_len is not None and end > max_len:
                    end = max_len
                line = bytes(self._buffer[:end])
                del self._buffer[:end]
                return line

            if max_len is not None and len(self._buffer) >= max_len:
                line = bytes(self._buffer[:max_len])
                del self._buffer[:max_len]
                return line

            if self._eof or self._channel is None:
                line = bytes(self._buffer)
                self._buffer.clear()
                return line

            try:
                chunk = self._channel.recv_sync()
                if not chunk:
                    self._eof = True
                    break
                self._buffer.extend(chunk)
            except Exception:
                self._eof = True
                break

        line = bytes(self._buffer)
        self._buffer.clear()
        return line

    def readlines(self, hint: int = -1) -> list[bytes]:
        if self._bio is not None:
            return self._bio.readlines(hint)
        lines: list[bytes] = []
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            if hint > 0 and sum(len(l) for l in lines) >= hint:
                break
        return lines

    def __iter__(self) -> SyncStreamReader:
        return self

    def __next__(self) -> bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line


class MultiloopWSGIWorker:
    """WSGI 1.0.1 (PEP 3333) Worker adapter for running synchronous Django, Flask and Bottle applications.

    Offloads synchronous application execution to a dedicated, decoupled thread pool executor,
    bridging streaming responses back to the asynchronous I/O event loops via lock-free `Channel`.

    :param app: The synchronous WSGI application callable `(environ, start_response)`.
    :param pool: The `EventLoopThreadPool` instance to execute network I/O and worker tasks.
    :param host: Host address to listen on (default: "127.0.0.1").
    :param port: Port number to listen on (0 for ephemeral port).
    :param max_request_body: Max allowed request body bytes (default: 10MB).
    :param sync_workers: Number of dedicated synchronous WSGI worker threads (default: CPU cores * 8).
    """

    port: int

    def __init__(
        self,
        app: Callable[[dict[str, Any], Callable[..., Any]], Iterable[bytes]],
        pool: EventLoopThreadPool,
        host: str = "127.0.0.1",
        port: int = 0,
        max_request_body: int = _MAX_REQUEST_BODY,
        sync_workers: int | None = None,
    ) -> None:
        self.app = app
        self.pool = pool
        self.host = host
        self.port = port
        self.max_request_body = max_request_body
        num_sync = sync_workers if sync_workers is not None else min(64, (os.cpu_count() or 1) * 8)
        self._max_sync_workers = max(num_sync, 4)
        self._sync_task_count = 0
        self._sync_task_lock = threading.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_sync_workers,
            thread_name_prefix="multiloop-wsgi",
        )
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

    async def _read_chunked_body(self, reader: asyncio.StreamReader, initial: bytes) -> bytes:
        """Read and decode incoming Transfer-Encoding: chunked request body."""
        buf = bytearray(initial)
        body = bytearray()
        while True:
            while True:
                idx = buf.find(b"\r\n")
                if idx != -1:
                    break
                chunk = await asyncio.wait_for(reader.read(4096), timeout=_HEADER_TIMEOUT)
                if not chunk:
                    raise ConnectionResetError("Unexpected EOF while reading chunk size")
                buf.extend(chunk)

            size_line = bytes(buf[:idx]).strip().split(b";")[0]
            try:
                chunk_len = int(size_line, 16)
            except ValueError as err:
                raise ValueError("Invalid chunked encoding length") from err

            del buf[: idx + 2]

            if chunk_len == 0:
                # Read trailing CRLF
                while len(buf) < 2:
                    chunk = await asyncio.wait_for(reader.read(4096), timeout=_HEADER_TIMEOUT)
                    if not chunk:
                        break
                    buf.extend(chunk)
                if buf.startswith(b"\r\n"):
                    del buf[:2]
                break

            while len(buf) < chunk_len + 2:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=_HEADER_TIMEOUT)
                if not chunk:
                    raise ConnectionResetError("Unexpected EOF while reading chunk data")
                buf.extend(chunk)

            if len(body) + chunk_len > self.max_request_body:
                raise ValueError("Payload Too Large")

            body.extend(buf[:chunk_len])
            del buf[: chunk_len + 2]

        return bytes(body)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        client_addr: tuple[str, int] | None = None,
    ) -> None:
        with self._sync_task_lock:
            if self._sync_task_count >= self._max_sync_workers * 3:
                writer.write(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"content-length: 19\r\n"
                    b"connection: close\r\n\r\n"
                    b"Service Unavailable"
                )
                try:
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
                return
            self._sync_task_count += 1

        try:
            method = "GET"
            path = "/"
            query_string = ""
            http_version = "1.1"
            headers_list: list[tuple[str, str]] = []
            content_length = 0
            is_chunked_req = False
            body = b""
            body_channel: Channel | None = None
            feeder_task: asyncio.Task[None] | None = None

            if _FastHttpParser is not None:
                raw_buf = bytearray()
                while True:
                    chunk = await asyncio.wait_for(reader.read(4096), timeout=_HEADER_TIMEOUT)
                    if not chunk:
                        return
                    raw_buf.extend(chunk)
                    if len(raw_buf) > _MAX_HEADER_SIZE:
                        writer.write(
                            b"HTTP/1.1 431 Request Header Fields Too Large\r\n"
                            b"content-length: 0\r\n"
                            b"connection: close\r\n\r\n"
                        )
                        await writer.drain()
                        return

                    parsed = _FastHttpParser.parse_request(raw_buf)
                    if parsed is not None:
                        (
                            method,
                            path,
                            _raw_path,
                            _query_bytes,
                            http_version,
                            raw_headers,
                            body_offset,
                            raw_cl,
                            _ka,
                            is_chunked_req,
                            _upg,
                            _upg_p,
                        ) = parsed
                        query_string = _query_bytes.decode("latin1")
                        content_length = raw_cl if raw_cl >= 0 else 0
                        headers_list = [
                            (k.decode("latin1").lower(), v.decode("latin1")) for k, v in raw_headers
                        ]

                        if content_length > self.max_request_body:
                            writer.write(
                                b"HTTP/1.1 413 Payload Too Large\r\n"
                                b"content-length: 0\r\n"
                                b"connection: close\r\n\r\n"
                            )
                            await writer.drain()
                            return

                        initial_body = bytes(raw_buf[body_offset:])
                        if is_chunked_req:
                            try:
                                body = await self._read_chunked_body(reader, initial_body)
                                content_length = len(body)
                                input_stream = SyncStreamReader(body)
                            except ValueError:
                                writer.write(
                                    b"HTTP/1.1 400 Bad Request\r\n"
                                    b"content-length: 0\r\n"
                                    b"connection: close\r\n\r\n"
                                )
                                await writer.drain()
                                return
                        elif content_length > len(initial_body):
                            chan = Channel(maxsize=16)
                            body_channel = chan
                            input_stream = SyncStreamReader(initial_body, channel=chan)
                            remaining_bytes = content_length - len(initial_body)

                            async def _feed_body(target_chan: Channel, rem: int) -> None:
                                try:
                                    while rem > 0:
                                        c_size = min(rem, 65536)
                                        chunk = await asyncio.wait_for(
                                            reader.read(c_size), timeout=_HEADER_TIMEOUT
                                        )
                                        if not chunk:
                                            break
                                        rem -= len(chunk)
                                        await target_chan.send(chunk)
                                except Exception:
                                    pass
                                finally:
                                    try:
                                        await target_chan.send(b"")
                                    except Exception:
                                        pass
                                    target_chan.close()

                            loop = asyncio.get_running_loop()
                            feeder_task = loop.create_task(_feed_body(chan, remaining_bytes))
                        else:
                            body = initial_body[:content_length] if content_length > 0 else b""
                            input_stream = SyncStreamReader(body)
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

                header_bytes_total = len(req_line)
                while True:
                    line = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT)
                    if not line or line == b"\r\n":
                        break
                    header_bytes_total += len(line)
                    if header_bytes_total > _MAX_HEADER_SIZE:
                        writer.write(
                            b"HTTP/1.1 431 Request Header Fields Too Large\r\n"
                            b"content-length: 0\r\n"
                            b"connection: close\r\n\r\n"
                        )
                        await writer.drain()
                        return

                    if b":" in line:
                        k, v = line.decode("latin1").split(":", 1)
                        k_str = k.strip().lower()
                        v_str = v.strip()
                        headers_list.append((k_str, v_str))
                        if k_str == "content-length":
                            try:
                                content_length = int(v_str)
                            except ValueError:
                                content_length = 0
                        elif k_str == "transfer-encoding" and "chunked" in v_str.lower():
                            is_chunked_req = True

                if content_length > self.max_request_body:
                    writer.write(
                        b"HTTP/1.1 413 Payload Too Large\r\n"
                        b"content-length: 0\r\n"
                        b"connection: close\r\n\r\n"
                    )
                    await writer.drain()
                    return

                if is_chunked_req:
                    try:
                        body = await self._read_chunked_body(reader, b"")
                        content_length = len(body)
                        input_stream = SyncStreamReader(body)
                    except ValueError:
                        writer.write(
                            b"HTTP/1.1 400 Bad Request\r\n"
                            b"content-length: 0\r\n"
                            b"connection: close\r\n\r\n"
                        )
                        await writer.drain()
                        return
                elif content_length > 0:
                    chan = Channel(maxsize=16)
                    body_channel = chan
                    input_stream = SyncStreamReader(b"", channel=chan)

                    async def _feed_body_fallback(target_chan: Channel, rem: int) -> None:
                        try:
                            while rem > 0:
                                c_size = min(rem, 65536)
                                chunk = await asyncio.wait_for(
                                    reader.read(c_size), timeout=_HEADER_TIMEOUT
                                )
                                if not chunk:
                                    break
                                rem -= len(chunk)
                                await target_chan.send(chunk)
                        except Exception:
                            pass
                        finally:
                            try:
                                await target_chan.send(b"")
                            except Exception:
                                pass
                            target_chan.close()

                    loop = asyncio.get_running_loop()
                    feeder_task = loop.create_task(_feed_body_fallback(chan, content_length))
                else:
                    input_stream = SyncStreamReader(b"")

            # Build PEP 3333 WSGI Environ
            client_ip, client_port = client_addr if client_addr else (self.host, 0)

            # Resolve server name safely (prevent Django DisallowedHost when bound to 0.0.0.0)
            server_name = self.host
            if server_name in ("0.0.0.0", "::", ""):
                for k, v in headers_list:
                    if k == "host":
                        cand = v.split(":")[0].strip()
                        if cand and all(c.isalnum() or c in ".-_" for c in cand):
                            server_name = cand
                        break
                if server_name in ("0.0.0.0", "::", ""):
                    server_name = "127.0.0.1"

            content_type_val = ""
            content_len_val = str(content_length) if content_length > 0 else ""
            header_dict_pep: dict[str, list[str]] = {}

            for k, v in headers_list:
                if k == "content-type":
                    content_type_val = v
                elif k == "content-length":
                    content_len_val = v
                else:
                    header_key = f"HTTP_{k.upper().replace('-', '_')}"
                    header_dict_pep.setdefault(header_key, []).append(v)

            environ: dict[str, Any] = {
                "REQUEST_METHOD": method,
                "SCRIPT_NAME": "",
                "PATH_INFO": urllib.parse.unquote(path, encoding="latin-1"),
                "QUERY_STRING": query_string,
                "SERVER_NAME": server_name,
                "SERVER_PORT": str(self._server.port),
                "SERVER_PROTOCOL": f"HTTP/{http_version}",
                "REMOTE_ADDR": client_ip,
                "REMOTE_PORT": str(client_port),
                "wsgi.version": (1, 0),
                "wsgi.url_scheme": "http",
                "wsgi.input": input_stream,
                "wsgi.errors": sys.stderr,
                "wsgi.multithread": True,
                "wsgi.multiprocess": False,
                "wsgi.run_once": False,
            }

            if content_type_val:
                environ["CONTENT_TYPE"] = content_type_val
            if content_len_val:
                environ["CONTENT_LENGTH"] = content_len_val

            # Map multiple headers of same key with comma per PEP 3333
            for hk, hvals in header_dict_pep.items():
                environ[hk] = ", ".join(hvals)

            # Lock-free channel for immutable event message protocol
            chunk_channel: Channel = Channel(maxsize=128)
            headers_sent_event = threading.Event()
            headers_set = False

            def start_response(
                status: str,
                headers: list[tuple[str, str]],
                exc_info: Any = None,
            ) -> Callable[[bytes], Any]:
                nonlocal headers_set
                if exc_info:
                    try:
                        if headers_sent_event.is_set():
                            raise exc_info[1].with_traceback(exc_info[2])
                    finally:
                        exc_info = None
                elif headers_set:
                    raise AssertionError("Headers already set without exc_info")

                headers_set = True
                try:
                    chunk_channel.send_sync(("START", status, list(headers)))
                except Exception as exc:
                    raise BrokenPipeError("WSGI client connection closed") from exc

                def write(data: bytes) -> None:
                    if data:
                        try:
                            chunk_channel.send_sync(("DATA", data))
                        except Exception as exc:
                            raise BrokenPipeError("WSGI client connection closed") from exc

                return write

            # Synchronous WSGI execution runner in managed executor
            def _run_wsgi_sync() -> None:
                try:
                    response_iterable = self.app(environ, start_response)
                    try:
                        for chunk in response_iterable:
                            if chunk:
                                try:
                                    chunk_channel.send_sync(("DATA", chunk))
                                except Exception as exc:
                                    raise BrokenPipeError("WSGI client connection closed") from exc
                    finally:
                        close_fn: Any = getattr(response_iterable, "close", None)
                        if callable(close_fn):
                            close_fn()
                except BrokenPipeError:
                    pass
                except Exception as exc:
                    try:
                        chunk_channel.send_sync(("ERROR", exc))
                    except Exception:
                        pass
                finally:
                    try:
                        chunk_channel.send_sync(("END", None))
                    except Exception:
                        pass

            loop = asyncio.get_running_loop()
            sync_future = loop.run_in_executor(self._executor, _run_wsgi_sync)

            # Consume chunk channel and stream to client asynchronously
            headers_sent = False
            is_chunked = False
            active_status = "200 OK"
            response_headers: list[tuple[str, str]] = []

            try:
                while True:
                    try:
                        msg = await chunk_channel.recv()
                    except Exception:
                        break

                    if not isinstance(msg, tuple):
                        break

                    msg_tuple = cast("tuple[str, Any, Any]", msg)
                    msg_type: str = msg_tuple[0]
                    payload: Any = msg_tuple[1]

                    if writer.is_closing():
                        break

                    if msg_type == "START":
                        active_status = str(payload)
                        headers_raw = cast("list[tuple[str, str]]", msg_tuple[2])
                        response_headers = list(headers_raw)
                    elif msg_type == "DATA":
                        chunk = cast(bytes, payload)
                        if not headers_sent:
                            headers_sent = True
                            headers_sent_event.set()
                            has_cl = any(k.lower() == "content-length" for k, _ in response_headers)
                            has_te = any(
                                k.lower() == "transfer-encoding" for k, _ in response_headers
                            )
                            if not has_cl and not has_te:
                                response_headers.append(("transfer-encoding", "chunked"))
                                is_chunked = True
                            response_headers.append(("connection", "close"))

                            header_lines = [f"HTTP/1.1 {active_status}"]
                            header_lines.extend(f"{k}: {v}" for k, v in response_headers)
                            writer.write(("\r\n".join(header_lines) + "\r\n\r\n").encode("latin1"))

                        try:
                            if is_chunked:
                                writer.write(
                                    f"{len(chunk):X}\r\n".encode("latin1") + chunk + b"\r\n"
                                )
                            else:
                                writer.write(chunk)
                            await writer.drain()
                        except (ConnectionResetError, BrokenPipeError, OSError):
                            break

                    elif msg_type == "ERROR":
                        if not headers_sent:
                            headers_sent = True
                            headers_sent_event.set()
                            err_body = b"Internal Server Error"
                            err_resp = (
                                b"HTTP/1.1 500 Internal Server Error\r\n"
                                b"content-type: text/plain\r\n"
                                b"content-length: " + str(len(err_body)).encode("latin1") + b"\r\n"
                                b"connection: close\r\n\r\n" + err_body
                            )
                            writer.write(err_resp)
                            await writer.drain()
                        break

                    elif msg_type == "END":
                        if not headers_sent:
                            headers_sent = True
                            headers_sent_event.set()
                            has_cl = any(k.lower() == "content-length" for k, _ in response_headers)
                            if not has_cl:
                                response_headers.append(("content-length", "0"))
                            response_headers.append(("connection", "close"))
                            header_lines = [f"HTTP/1.1 {active_status}"]
                            header_lines.extend(f"{k}: {v}" for k, v in response_headers)
                            writer.write(("\r\n".join(header_lines) + "\r\n\r\n").encode("latin1"))
                            await writer.drain()
                        elif is_chunked:
                            try:
                                writer.write(b"0\r\n\r\n")
                                await writer.drain()
                            except (ConnectionResetError, BrokenPipeError, OSError):
                                pass
                        break

            except (ConnectionResetError, BrokenPipeError, OSError):
                pass
            finally:
                if body_channel is not None:
                    body_channel.close()
                if feeder_task is not None and not feeder_task.done():
                    feeder_task.cancel()
                chunk_channel.close()
                try:
                    await asyncio.wait_for(sync_future, timeout=2.0)
                except Exception:
                    pass

        except Exception:  # noqa: BLE001
            if not writer.is_closing():
                try:
                    err_resp = (
                        b"HTTP/1.1 500 Internal Server Error\r\n"
                        b"content-length: 21\r\n"
                        b"connection: close\r\n\r\n"
                        b"Internal Server Error"
                    )
                    writer.write(err_resp)
                    await writer.drain()
                except Exception:  # noqa: BLE001, S110
                    pass
        finally:
            with self._sync_task_lock:
                self._sync_task_count = max(0, self._sync_task_count - 1)
            if not writer.is_closing():
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    async def close(self) -> None:
        """Stop the WSGI Worker Server and shut down its executor."""
        await self._server.close()
        self._executor.shutdown(wait=False, cancel_futures=True)

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
