"""Cross-thread task context and cancellation manager."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any, Self

from gsyncio._cancel import CancelScope
from gsyncio.pool import EventLoopThreadPool


class AsyncContext:
    """Cross-thread Context for cascading Task cancellation (Go `context.Context` style).

    This class enables hierarchical task cancellation across different worker event loops.
    Supports both explicit Go-style :meth:`cancel` and Pythonic ``async with`` scope management.

    :param parent:
        Optional parent context. If specified, cancelling `parent` automatically cancels this child context.
    :type parent: AsyncContext or None
    """

    def __init__(self, parent: AsyncContext | None = None) -> None:
        self._parent = parent
        self._cancelled = False
        self._lock = threading.Lock()
        self._children: list[AsyncContext] = []
        # Track futures and per-task cancel scopes
        self._futures: dict[asyncio.Future[Any], asyncio.AbstractEventLoop | None] = {}
        self._scopes: dict[asyncio.Future[Any], CancelScope] = {}

        if parent is not None:
            parent._add_child(self)

    def __repr__(self) -> str:
        return f"<AsyncContext cancelled={self.is_cancelled}>"

    @property
    def parent(self) -> AsyncContext | None:
        """Return the parent context, or ``None`` for a root context.

        Read-only: the parent is fixed at construction time.
        """
        return self._parent

    def _add_child(self, child: AsyncContext) -> None:
        cancelled = False
        with self._lock:
            if self._cancelled:
                cancelled = True
            else:
                self._children.append(child)
        if cancelled:
            child.cancel()

    def _remove_child(self, child: AsyncContext) -> None:
        """Remove a child context from the registered children list (cleanup)."""
        with self._lock:
            try:
                self._children.remove(child)
            except ValueError:
                pass

    @property
    def is_cancelled(self) -> bool:
        """Return whether the context has been cancelled.

        :returns: ``True`` if cancelled, ``False`` otherwise.
        :rtype: :class:`bool`
        """
        with self._lock:
            return self._cancelled

    def submit(
        self,
        pool: EventLoopThreadPool,
        target: Callable[..., Any] | Any,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        """Submit a task to a thread pool bound to this context.

        :param pool:
            The `EventLoopThreadPool` instance.

        :param target:
            The target coroutine or callable task.

        :param args:
            Positional arguments to pass to `target`.

        :param kwargs:
            Keyword arguments to pass to `target`.

        :returns: An :class:`asyncio.Future` bound to this context.
        :rtype: :class:`asyncio.Future`
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
            # WHY: cancel() can fire between the first check above and pool.submit().
            # Without this second check the task would keep running in the pool
            # after cancellation, so the future and scope are cancelled immediately instead.
            if self._cancelled:
                scope.cancel()
                if not fut_res.done():
                    fut_loop = fut_res.get_loop()
                    if fut_loop and fut_loop.is_running():
                        try:
                            fut_loop.call_soon_threadsafe(fut_res.cancel)
                        except RuntimeError:
                            # WHY: the future's loop closed between the
                            # snapshot and the wakeup — the future is gone
                            # with it; skip instead of surfacing a spurious
                            # error to the submitter (R5 FIX-E pattern).
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
                # WHY: drop the entry as soon as the task finishes — without
                # this every completed submission stays tracked forever and
                # cancel() walks stale futures (R3 FIX-21).
                fut_res.add_done_callback(self._discard_future)

        return fut_res

    def _discard_future(self, fut: asyncio.Future[Any]) -> None:
        """Remove a finished future and scope from the tracking dicts (done-callback).

        The callback runs on the future's owning loop, so the dict access is
        serialised with submit()/cancel() via ``_lock``; a completed entry is
        simply absent the next time cancel() walks the dict.
        """
        with self._lock:
            self._futures.pop(fut, None)
            self._scopes.pop(fut, None)

    def cancel(self) -> None:
        """Cancel this context and thread-safely cascade to all child contexts and futures."""
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

        # Detach from parent to prevent memory retention
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
                        # WHY: the future's loop closed between the snapshot
                        # and the wakeup — the future is gone with it; skip
                        # it instead of aborting the whole cascade, which
                        # would leave the remaining futures and children
                        # uncancelled (R5 FIX-E pattern).
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
