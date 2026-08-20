"""multiloop._websocket — Explicit RFC 6455 WebSocket State Machine & Frame Protocol.

Provides full-duplex WebSocket connection handling for ASGI 3.0 applications under
Python 3.14t multi-core execution, with thread-safe cross-loop trampolining and atomic framing.
"""

from __future__ import annotations

import asyncio
import base64
import enum
import hashlib
import struct
from typing import TYPE_CHECKING, Any, cast

from multiloop._rust import _try_import_rust_class
from multiloop._sync import Event, Lock
from multiloop.primitives import Channel

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

_WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_fast_websocket_unmask = _try_import_rust_class(
    "multiloop._multiloop_core", "fast_websocket_unmask"
)


class WebSocketState(enum.Enum):
    """Explicit state of an active WebSocket connection."""

    CONNECTING = "CONNECTING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class WebSocketConnection:
    """Explicit state machine managing RFC 6455 full-duplex WebSocket sessions."""

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
        path: str,
        query_string: str,
        headers: list[tuple[bytes, bytes]],
        headers_dict: dict[str, str],
        client_addr: tuple[str, int] | None,
        server_addr: tuple[str, int],
        http_version: str = "1.1",
    ) -> None:
        self.app = app
        self.reader = reader
        self.writer = writer
        self.path = path
        self.query_string = query_string
        self.headers = headers
        self.headers_dict = headers_dict
        self.client_addr = client_addr
        self.server_addr = server_addr
        self.http_version = http_version

        self.state = WebSocketState.CONNECTING
        self._home_loop = asyncio.get_running_loop()
        self._send_lock = Lock()
        self._inbound_channel: Channel = Channel(maxsize=256)
        self._connect_sent = False
        self._handshake_done = Event()
        self._closed_event = Event()
        self._accept_key = ""

    async def run(self) -> None:
        """Run the full WebSocket connection lifecycle."""
        ws_key = self.headers_dict.get("sec-websocket-key", "")
        if not ws_key:
            self.writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            await self.writer.drain()
            self.state = WebSocketState.CLOSED
            return

        self._accept_key = base64.b64encode(
            hashlib.sha1(ws_key.encode("latin1") + _WS_MAGIC).digest()
        ).decode("latin1")

        subprotocols = [
            s.strip()
            for s in self.headers_dict.get("sec-websocket-protocol", "").split(",")
            if s.strip()
        ]

        scope: dict[str, Any] = {
            "type": "websocket",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "http_version": self.http_version,
            "scheme": "ws",
            "path": self.path,
            "raw_path": self.path.encode("latin1"),
            "query_string": self.query_string.encode("latin1"),
            "headers": self.headers,
            "client": self.client_addr or ("127.0.0.1", 0),
            "server": self.server_addr,
            "subprotocols": subprotocols,
        }

        reader_task = self._home_loop.create_task(self._reader_loop())
        try:
            await self.app(scope, self.receive, self.send)
        finally:
            self.state = WebSocketState.CLOSED
            self._closed_event.set()
            reader_task.cancel()
            try:
                await reader_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001, S110
                pass
            self._inbound_channel.close()

    async def receive(self) -> dict[str, Any]:
        """ASGI receive callback for WebSocket messages."""
        cur_loop = asyncio.get_running_loop()
        if cur_loop is not self._home_loop:
            fut = asyncio.run_coroutine_threadsafe(self.receive(), self._home_loop)
            return await asyncio.wrap_future(fut)

        if not self._connect_sent:
            self._connect_sent = True
            return {"type": "websocket.connect"}
        if self._closed_event.is_set() and self._inbound_channel.empty():
            return {"type": "websocket.disconnect", "code": 1000}
        try:
            item: Any = await self._inbound_channel.recv()
            return cast(dict[str, Any], item)
        except Exception:  # noqa: BLE001
            return {"type": "websocket.disconnect", "code": 1006}

    async def send(self, message: dict[str, Any]) -> None:
        """ASGI send callback for WebSocket messages."""
        cur_loop = asyncio.get_running_loop()
        if cur_loop is not self._home_loop:
            fut = asyncio.run_coroutine_threadsafe(self.send(message), self._home_loop)
            await asyncio.wrap_future(fut)
            return

        m_type = message.get("type", "")
        if m_type == "websocket.accept":
            subproto = message.get("subprotocol")
            res_lines = [
                "HTTP/1.1 101 Switching Protocols",
                "Upgrade: websocket",
                "Connection: Upgrade",
                f"Sec-WebSocket-Accept: {self._accept_key}",
            ]
            if subproto:
                res_lines.append(f"Sec-WebSocket-Protocol: {subproto}")
            res_lines.extend(["", ""])
            self.writer.write("\r\n".join(res_lines).encode("latin1"))
            await self.writer.drain()
            self.state = WebSocketState.OPEN
            self._handshake_done.set()
        elif m_type == "websocket.send":
            if "text" in message and message["text"] is not None:
                payload = message["text"].encode("utf-8")
                await self._send_frame(0x1, payload)
            elif "bytes" in message and message["bytes"] is not None:
                payload = message["bytes"]
                await self._send_frame(0x2, payload)
        elif m_type == "websocket.close":
            self.state = WebSocketState.CLOSING
            code = message.get("code", 1000)
            reason = message.get("reason", "")
            payload = struct.pack("!H", code) + reason.encode("utf-8")
            try:
                await self._send_frame(0x8, payload)
            except Exception:  # noqa: BLE001, S110
                pass
            self.state = WebSocketState.CLOSED
            self._closed_event.set()
            self.writer.close()

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        """Send an RFC 6455 frame atomically."""
        async with self._send_lock:
            header = bytearray()
            header.append(0x80 | (opcode & 0x0F))
            length = len(payload)
            if length < 126:
                header.append(length)
            elif length <= 0xFFFF:
                header.append(126)
                header.extend(struct.pack("!H", length))
            else:
                header.append(127)
                header.extend(struct.pack("!Q", length))
            self.writer.write(bytes(header) + payload)
            await self.writer.drain()

    async def _reader_loop(self) -> None:
        """Background coroutine reading incoming RFC 6455 WebSocket frames."""
        await self._handshake_done.wait()
        try:
            while not self._closed_event.is_set():
                head = await self.reader.readexactly(2)
                b1, b2 = head[0], head[1]
                opcode = b1 & 0x0F
                masked = (b2 & 0x80) != 0
                payload_len = b2 & 0x7F

                if payload_len == 126:
                    len_bytes = await self.reader.readexactly(2)
                    payload_len = struct.unpack("!H", len_bytes)[0]
                elif payload_len == 127:
                    len_bytes = await self.reader.readexactly(8)
                    payload_len = struct.unpack("!Q", len_bytes)[0]

                mask_key = b""
                if masked:
                    mask_key = await self.reader.readexactly(4)

                payload = await self.reader.readexactly(payload_len) if payload_len > 0 else b""
                if masked and mask_key:
                    if _fast_websocket_unmask is not None and len(mask_key) == 4:
                        payload = _fast_websocket_unmask(payload, mask_key)
                    else:
                        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

                if opcode == 0x1:  # Text
                    await self._inbound_channel.send(
                        {
                            "type": "websocket.receive",
                            "text": payload.decode("utf-8"),
                            "bytes": None,
                        }
                    )
                elif opcode == 0x2:  # Binary
                    await self._inbound_channel.send(
                        {"type": "websocket.receive", "bytes": payload, "text": None}
                    )
                elif opcode == 0x9:  # Ping -> Pong
                    await self._send_frame(0xA, payload)
                elif opcode == 0x8:  # Close
                    code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
                    try:
                        await self._send_frame(0x8, payload)
                    except Exception:  # noqa: BLE001, S110
                        pass
                    await self._inbound_channel.send({"type": "websocket.disconnect", "code": code})
                    self.state = WebSocketState.CLOSED
                    self._closed_event.set()
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            await self._inbound_channel.send({"type": "websocket.disconnect", "code": 1006})
            self.state = WebSocketState.CLOSED
            self._closed_event.set()
        except Exception:  # noqa: BLE001
            await self._inbound_channel.send({"type": "websocket.disconnect", "code": 1006})
            self.state = WebSocketState.CLOSED
            self._closed_event.set()
