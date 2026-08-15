"""Task-local cancellation scopes (anyio / trio inspired) for gsyncio."""

from __future__ import annotations

import asyncio
import contextvars
import math
import threading
from typing import Any, Self

from gsyncio.exceptions import TimeoutError

_scope_stack_var: contextvars.ContextVar[tuple[CancelScope, ...]] = contextvars.ContextVar(
    "_cancel_scope_stack", default=()
)


def _get_scope_stack() -> tuple[CancelScope, ...]:
    """Return the task-local scope stack, creating it on first access."""
    return _scope_stack_var.get()


class CancelScope:
    """A task-local cancellation scope that propagates cancellation hierarchically.

    Each scope lives on a per-task stack managed via :data:`contextvars.ContextVar`.
    Entered scopes track their own cancellation flag, an optional absolute deadline,
    and a *shield* that snapshots and clears the task's pending cancellation count
    on entry, restoring it on exit — it absorbs cancellations *already injected*
    before entry.
    """

    def __init__(self, deadline: float = float("inf"), shield: bool = False) -> None:
        if not (math.isfinite(deadline) or deadline == float("inf")):
            raise ValueError("deadline must be a finite number or float('inf')")
        self._cancel_called = False
        self._cancel_lock = threading.Lock()
        self._deadline = deadline
        self._shield = shield
        self._cancelled_caught = False
        self._convert_to_timeout = False
        self._task: asyncio.Task[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._deadline_handle: asyncio.TimerHandle | None = None
        self._saved_cancel_count = 0
        self._injected = False

    # -- inspectable properties & dynamic setters -----------------------------

    @property
    def cancel_called(self) -> bool:
        """Return ``True`` after :meth:`cancel` has been called at least once."""
        with self._cancel_lock:
            return self._cancel_called

    @property
    def deadline(self) -> float:
        """The absolute monotonic deadline (``float('inf')`` means no deadline)."""
        with self._cancel_lock:
            return self._deadline

    @deadline.setter
    def deadline(self, value: float) -> None:
        if not (math.isfinite(value) or value == float("inf")):
            raise ValueError("deadline must be a finite number or float('inf')")

        with self._cancel_lock:
            self._deadline = value
            task = self._task
            loop = self._loop
            old_handle = self._deadline_handle
            self._deadline_handle = None

        if old_handle is not None:
            old_handle.cancel()

        if task is not None and not task.done() and loop is not None:
            if value == float("inf"):
                return

            def _schedule() -> None:
                with self._cancel_lock:
                    if self._task is not task or self._deadline != value or self._cancel_called:
                        return
                remaining = value - loop.time()
                if remaining <= 0:
                    self.cancel()
                else:
                    with self._cancel_lock:
                        if (
                            self._task is task
                            and self._deadline == value
                            and not self._cancel_called
                        ):
                            self._deadline_handle = loop.call_later(
                                remaining, self._deadline_callback
                            )

            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None

            if current_loop is loop:
                _schedule()
            else:
                try:
                    loop.call_soon_threadsafe(_schedule)
                except RuntimeError:
                    pass

    @property
    def shield(self) -> bool:
        """Return ``True`` if this scope blocks parent cancellation."""
        with self._cancel_lock:
            return self._shield

    @shield.setter
    def shield(self, value: bool) -> None:
        with self._cancel_lock:
            if self._shield == value:
                return
            self._shield = value
            task = self._task

        if task is not None and not task.done():
            if value:
                count = task.cancelling()
                for _ in range(count):
                    task.uncancel()
                self._saved_cancel_count += count
            else:
                if self._saved_cancel_count > 0:
                    for _ in range(self._saved_cancel_count):
                        task.cancel()
                    self._saved_cancel_count = 0

    @property
    def cancelled_caught(self) -> bool:
        """Return ``True`` if this scope silently absorbs cancellation (move-on-*)."""
        return self._cancelled_caught

    # -- cancellation ---------------------------------------------------------

    def cancel(self) -> None:
        """Mark this scope as cancelled and inject into the hosting task."""
        with self._cancel_lock:
            if self._cancel_called:
                return
            self._cancel_called = True
            task = self._task
            loop = self._loop

        if task is not None and not task.done():
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if loop is not None and current_loop is not loop:
                try:

                    def _deliver() -> None:
                        with self._cancel_lock:
                            if self._task is not task:
                                return
                            if not task.done() and task.cancel():
                                self._injected = True

                    loop.call_soon_threadsafe(_deliver)
                except RuntimeError:
                    pass
            else:
                if task.cancel():
                    with self._cancel_lock:
                        self._injected = True

    def _take_injected(self) -> bool:
        """Return and clear the injection ledger (TaskGroup failure path)."""
        with self._cancel_lock:
            injected = self._injected
            self._injected = False
            return injected

    def _effectively_cancelled(self) -> bool:
        """Whether *this* scope (or an unshielded ancestor) is cancelled."""
        if self.cancel_called:
            return True
        if self._shield:
            return False
        stack = _get_scope_stack()
        try:
            idx = stack.index(self)
        except ValueError:
            return False
        for i in range(idx - 1, -1, -1):
            if stack[i].cancel_called:
                return True
            if stack[i]._shield:
                return False
        return False

    # -- deadline scheduling --------------------------------------------------

    def _deadline_callback(self) -> None:
        """Event-loop callback fired when the deadline expires."""
        self._deadline_handle = None
        self.cancel()

    def _rollback_aenter(self, clear_binding: bool = True) -> None:
        """Undo the ``__aenter__`` side effects before raising from it."""
        stack = _get_scope_stack()
        if stack and stack[-1] is self:
            _scope_stack_var.set(stack[:-1])
        else:
            _scope_stack_var.set(tuple(s for s in stack if s is not self))
        if self._deadline_handle is not None:
            self._deadline_handle.cancel()
            self._deadline_handle = None
        if clear_binding:
            with self._cancel_lock:
                self._task = None
                self._loop = None

    def _restore_saved_cancel_count(self, target_task: asyncio.Task[Any] | None = None) -> None:
        """Re-inject the cancellation count a shielded enter cleared."""
        if not self._shield or self._saved_cancel_count <= 0:
            return
        if target_task is None:
            with self._cancel_lock:
                target_task = self._task
        if target_task is not None:
            for _ in range(self._saved_cancel_count):
                target_task.cancel()

    # -- context manager protocol ---------------------------------------------

    async def __aenter__(self) -> Self:
        with self._cancel_lock:
            host = asyncio.current_task()
            if self._task is not None and self._task is not host:
                raise RuntimeError(
                    "CancelScope cannot be shared across tasks: already entered by another task"
                )
            self._task = host
            self._loop = asyncio.get_running_loop()
            self._injected = False

        stack = _get_scope_stack()
        _scope_stack_var.set((*stack, self))

        if self._shield:
            task = self._task
            if task is not None:
                self._saved_cancel_count = task.cancelling()
                for _ in range(self._saved_cancel_count):
                    task.uncancel()

        if self._deadline != float("inf"):
            loop = asyncio.get_running_loop()
            delta = self._deadline - loop.time()
            if delta <= 0:
                if self._convert_to_timeout:
                    with self._cancel_lock:
                        self._cancel_called = True
                    self._restore_saved_cancel_count()
                    self._rollback_aenter()
                    raise TimeoutError() from None
                if self._cancelled_caught:
                    self.cancel()
                    self._rollback_aenter(clear_binding=False)
                    return self
                with self._cancel_lock:
                    self._cancel_called = True
                self._restore_saved_cancel_count()
                self._rollback_aenter()
                raise asyncio.CancelledError()
            else:
                self._deadline_handle = loop.call_later(delta, self._deadline_callback)

        if self._effectively_cancelled():
            if self._cancel_called and self._convert_to_timeout:
                self._restore_saved_cancel_count()
                self._rollback_aenter()
                raise TimeoutError() from None
            if self._cancel_called and self._cancelled_caught:
                if host is not None and not host.done() and host.cancel():
                    with self._cancel_lock:
                        self._injected = True
                self._rollback_aenter(clear_binding=False)
                return self
            if not self._shield and host is not None:
                for _ in range(host.cancelling()):
                    host.uncancel()
            self._restore_saved_cancel_count()
            self._rollback_aenter()
            raise asyncio.CancelledError()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool | None:
        with self._cancel_lock:
            task = self._task
            injected = self._injected
            self._injected = False
            self._task = None
            self._loop = None
        try:
            stack = _get_scope_stack()
            if stack and stack[-1] is self:
                _scope_stack_var.set(stack[:-1])
            else:
                _scope_stack_var.set(tuple(s for s in stack if s is not self))

            if self._deadline_handle is not None:
                self._deadline_handle.cancel()
                self._deadline_handle = None

            self._restore_saved_cancel_count(target_task=task)

            if (
                exc_type is not None
                and issubclass(exc_type, asyncio.CancelledError)
                and self._convert_to_timeout
                and self.cancel_called
            ):
                if task is not None and injected and task.cancelling() > 0:
                    task.uncancel()
                raise TimeoutError() from exc_val

            if self._cancelled_caught and self.cancel_called:
                if task is not None and injected and task.cancelling() > 0:
                    task.uncancel()
                if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
                    return True
                return None

            if (
                self.cancel_called
                and injected
                and task is not None
                and (exc_type is None or not issubclass(exc_type, asyncio.CancelledError))
                and task.cancelling() > 0
            ):
                task.uncancel()

            return None
        finally:
            with self._cancel_lock:
                self._task = None
                self._loop = None


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def fail_after(seconds: float) -> CancelScope:
    """Return a :class:`CancelScope` that raises :class:`TimeoutError` on expiry."""
    loop = asyncio.get_running_loop()
    scope = CancelScope(deadline=loop.time() + seconds)
    scope._convert_to_timeout = True
    return scope


def move_on_after(seconds: float) -> CancelScope:
    """Return a :class:`CancelScope` that silently exits on timeout."""
    loop = asyncio.get_running_loop()
    scope = CancelScope(deadline=loop.time() + seconds)
    scope._cancelled_caught = True
    return scope


def fail_at(deadline: float) -> CancelScope:
    """Return a :class:`CancelScope` with an absolute deadline that raises :class:`TimeoutError` on expiry."""
    scope = CancelScope(deadline=deadline)
    scope._convert_to_timeout = True
    return scope


def move_on_at(deadline: float) -> CancelScope:
    """Return a :class:`CancelScope` with an absolute deadline that silently exits on expiry."""
    scope = CancelScope(deadline=deadline)
    scope._cancelled_caught = True
    return scope


async def checkpoint() -> None:
    """Check for effective cancellation and raise if needed."""
    stack = _get_scope_stack()
    if not stack:
        return
    current = stack[-1]
    if current._effectively_cancelled():
        task = asyncio.current_task()
        if task is not None:
            for _ in range(task.cancelling()):
                task.uncancel()
        raise asyncio.CancelledError()


def current_effective_deadline() -> float:
    """Walk the task-local scope stack and return the tightest deadline."""
    stack = _get_scope_stack()
    min_deadline = float("inf")
    for scope in reversed(stack):
        min_deadline = min(min_deadline, scope._deadline)
        if scope._shield:
            break
    return min_deadline
