"""Cross-thread task context and cancellation manager (Go context.Context style)."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any, Self

from multiloop._cancel import CancelScope
from multiloop.pool import EventLoopThreadPool

__all__ = ["AsyncContext"]


class AsyncContext:
    """Hierarchical cross-thread cancellation context inspired by Go's ``context.Context``.

    Enables structured, cascading task cancellation across independent OS threads and isolated
    asyncio event loops. Supports explicit programmatic cancellation (:meth:`cancel`) as well
    as automatic cancellation upon exiting an ``async with`` block.

    :param parent: Optional parent context. If provided, cancelling the parent automatically
                   cascades cancellation to this child context.
    """

    def __init__(self, parent: AsyncContext | None = None) -> None:
        self._parent = parent
        self._cancelled = False
        self._lock = threading.Lock()
        self._children: set[AsyncContext] = set()
        # Track pending futures and their per-task cancel scopes
        self._futures: dict[asyncio.Future[Any], asyncio.AbstractEventLoop | None] = {}
        self._scopes: dict[asyncio.Future[Any], CancelScope] = {}

        if parent is not None:
            parent._add_child(self)

    def __repr__(self) -> str:
        return f"<AsyncContext cancelled={self.is_cancelled}>"

    @property
    def parent(self) -> AsyncContext | None:
        """Return the parent context, or ``None`` for a root context (read-only)."""
        return self._parent

    def _add_child(self, child: AsyncContext) -> None:
        cancelled = False
        with self._lock:
            if self._cancelled:
                cancelled = True
            else:
                self._children.add(child)
        if cancelled:
            child.cancel()

    def _remove_child(self, child: AsyncContext) -> None:
        """Remove a child context from the registered set to prevent memory retention."""
        with self._lock:
            self._children.discard(child)

    @property
    def is_cancelled(self) -> bool:
        """Return True if this context has been cancelled."""
        with self._lock:
            return self._cancelled

    def submit(
        self,
        pool: EventLoopThreadPool,
        target: Callable[..., Any] | Any,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        """Submit a task to a thread pool bound to this cancellation context.

        :param pool: The :class:`~multiloop.EventLoopThreadPool` instance to execute the task.
        :param target: The target coroutine function or callable task.
        :param args: Positional arguments to forward to `target`.
        :param kwargs: Keyword arguments to forward to `target`.
        :returns: An :class:`asyncio.Future` tracking the task's execution.
        """
        with self._lock:
            if self._cancelled:
                caller_loop = asyncio.get_running_loop()
                fut: asyncio.Future[Any] = caller_loop.create_future()
                fut.cancel()
                return fut
            scope = CancelScope()

        fut_res: asyncio.Future[Any] = pool.submit(target, *args, cancel_scope=scope, **kwargs)

        with self._lock:
            # Double check: handle race where cancel() fired between initial check and pool.submit()
            if self._cancelled:
                scope.cancel()
                if not fut_res.done():
                    fut_loop = fut_res.get_loop()
                    if fut_loop and fut_loop.is_running():
                        try:
                            fut_loop.call_soon_threadsafe(fut_res.cancel)
                        except RuntimeError:
                            pass
                    else:
                        fut_res.cancel()
            else:
                submit_loop: asyncio.AbstractEventLoop | None = None
                try:
                    submit_loop = asyncio.get_running_loop()
                except RuntimeError:
                    submit_loop = None
                self._futures[fut_res] = submit_loop
                self._scopes[fut_res] = scope
                fut_res.add_done_callback(self._discard_future)

        return fut_res

    def _discard_future(self, fut: asyncio.Future[Any]) -> None:
        """Remove a completed future and its scope from tracking dicts."""
        with self._lock:
            self._futures.pop(fut, None)
            self._scopes.pop(fut, None)

    def cancel(self) -> None:
        """Cancel this context and thread-safely propagate cancellation to all child contexts and futures."""
        with self._lock:
            if self._cancelled:
                return
            self._cancelled = True
            children = list(self._children)
            futures = list(self._futures.items())
            scopes = list(self._scopes.values())
            self._children.clear()
            self._futures.clear()
            self._scopes.clear()

        # Detach from parent to prevent reference cycle leaks
        if self._parent is not None:
            self._parent._remove_child(self)

        for child in children:
            child.cancel()

        for scope in scopes:
            scope.cancel()

        for fut, loop in futures:
            if not fut.done():
                if loop and loop.is_running():
                    try:
                        loop.call_soon_threadsafe(fut.cancel)
                    except RuntimeError:
                        pass
                else:
                    fut.cancel()

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit the async context manager and cancel the context."""
        self.cancel()
