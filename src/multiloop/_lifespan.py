"""multiloop._lifespan — Explicit ASGI 3.0 Lifespan State Machine.

Manages ASGI application startup and shutdown lifecycle protocols, conforming
to the ASGI 3.0 Lifespan specification with strict timeout handling and error
propagation under Python 3.14t multi-core execution.
"""

from __future__ import annotations

import asyncio
import enum
from typing import TYPE_CHECKING, Any

from multiloop._sync import Event

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


class LifespanState(enum.Enum):
    """Explicit state enum of the ASGI Lifespan lifecycle."""

    UNINITIALIZED = "UNINITIALIZED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class LifespanManager:
    """Explicit state machine managing ASGI 3.0 lifespan lifecycle (startup/shutdown)."""

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
        lifespan: str = "auto",
        startup_timeout: float = 10.0,
        shutdown_timeout: float = 10.0,
    ) -> None:
        self.app = app
        self.lifespan = lifespan
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self.state = LifespanState.UNINITIALIZED

        self._startup_event = Event()
        self._shutdown_trigger = Event()
        self._shutdown_event = Event()

        self._startup_status: dict[str, Any] = {
            "complete": False,
            "failed": False,
            "message": "",
            "exception": None,
        }
        self._shutdown_status: dict[str, Any] = {
            "complete": False,
            "failed": False,
            "message": "",
        }
        self._lifespan_task: asyncio.Task[None] | None = None
        self._startup_sent = False
        self._scope: dict[str, Any] = {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "state": {},
        }

    async def startup(self) -> None:
        """Run lifespan startup protocol."""
        if self.lifespan == "off":
            self.state = LifespanState.RUNNING
            return

        self.state = LifespanState.STARTING
        loop = asyncio.get_running_loop()
        self._lifespan_task = loop.create_task(self._lifespan_runner())

        effective_timeout = self.startup_timeout if self.lifespan == "on" else 0.05
        try:
            await asyncio.wait_for(self._startup_event.wait(), timeout=effective_timeout)
        except TimeoutError:
            if self.lifespan == "on":
                self.state = LifespanState.FAILED
                raise RuntimeError("Lifespan startup timed out") from None
            # auto mode: lifespan not supported by app, clean up task
            if self._lifespan_task and not self._lifespan_task.done():
                self._lifespan_task.cancel()
            self.state = LifespanState.RUNNING
            return

        if self._startup_status["failed"]:
            self.state = LifespanState.FAILED
            msg = self._startup_status.get("message", "Lifespan startup failed")
            raise RuntimeError(f"Application startup failed: {msg}")

        if self._startup_status["exception"] is not None:
            if self.lifespan == "on":
                self.state = LifespanState.FAILED
                exc = self._startup_status["exception"]
                raise RuntimeError(f"Application startup failed: {exc}") from exc
            # auto mode: app does not support lifespan, clean up task
            if self._lifespan_task and not self._lifespan_task.done():
                self._lifespan_task.cancel()
            self.state = LifespanState.RUNNING
            return

        self.state = LifespanState.RUNNING

    async def shutdown(self) -> None:
        """Run lifespan shutdown protocol."""
        if self.state is not LifespanState.RUNNING:
            return

        self.state = LifespanState.STOPPING
        self._shutdown_trigger.set()
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=self.shutdown_timeout)
        except Exception:  # noqa: BLE001, S110
            pass

        if self._lifespan_task and not self._lifespan_task.done():
            self._lifespan_task.cancel()
            try:
                await self._lifespan_task
            except (asyncio.CancelledError, Exception):
                pass

        self.state = LifespanState.STOPPED

    async def _lifespan_receive(self) -> dict[str, Any]:
        if not self._startup_sent:
            self._startup_sent = True
            return {"type": "lifespan.startup"}
        await self._shutdown_trigger.wait()
        return {"type": "lifespan.shutdown"}

    async def _lifespan_send(self, message: dict[str, Any]) -> None:
        m_type = message.get("type", "")
        if m_type == "lifespan.startup.complete":
            self._startup_status["complete"] = True
            self._startup_event.set()
        elif m_type == "lifespan.startup.failed":
            self._startup_status["failed"] = True
            self._startup_status["message"] = message.get("message", "")
            self._startup_event.set()
        elif m_type == "lifespan.shutdown.complete":
            self._shutdown_status["complete"] = True
            self._shutdown_event.set()
        elif m_type == "lifespan.shutdown.failed":
            self._shutdown_status["failed"] = True
            self._shutdown_status["message"] = message.get("message", "")
            self._shutdown_event.set()

    async def _lifespan_runner(self) -> None:
        try:
            await self.app(self._scope, self._lifespan_receive, self._lifespan_send)
        except Exception as exc:  # noqa: BLE001
            if not self._startup_event.is_set():
                self._startup_status["exception"] = exc
                self._startup_status["message"] = str(exc)
                self._startup_event.set()
            if not self._shutdown_event.is_set():
                self._shutdown_status["failed"] = True
                self._shutdown_status["message"] = str(exc)
                self._shutdown_event.set()
