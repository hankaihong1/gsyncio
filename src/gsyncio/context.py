"""Cross-thread task context and cancellation manager."""

import asyncio
import threading
from collections.abc import Callable
from typing import Any

from gsyncio._cancel import CancelScope
from gsyncio.pool import EventLoopThreadPool


class AsyncContext:
    """Cross-thread Context for cascading Task cancellation (Go `context.Context` style).

    This class enables hierarchical task cancellation across different worker event loops.

    :param parent:
        Optional parent context. If specified, cancelling `parent` automatically cancels this child context.
    :type parent: AsyncContext or None

    """

    def __init__(self, parent: AsyncContext | None = None) -> None:
        self._parent = parent
        self._cancelled = False
        self._lock = threading.Lock()
        self._cancel_scope = CancelScope()
        self._children: list[AsyncContext] = []
        self._futures: list[tuple[asyncio.AbstractEventLoop | None, asyncio.Future[Any]]] = []

        if parent:
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
        with self._lock:
            if self._cancelled:
                child.cancel()
            else:
                self._children.append(child)

    @property
    def is_cancelled(self) -> bool:
        """Return whether the context has been cancelled.

        :returns: ``True`` if cancelled, ``False`` otherwise.
        :rtype: :class:`bool`

        """
        with self._lock:
            return self._cancelled

    def submit(
        self, pool: EventLoopThreadPool, target: Callable[..., Any], *args: Any, **kwargs: Any
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
                fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                fut.cancel()
                return fut

        fut_res: asyncio.Future[Any] = pool.submit(
            target, *args, cancel_scope=self._cancel_scope, **kwargs
        )

        with self._lock:
            # WHY: cancel() can fire between the first check above and pool.submit().
            # Without this second check the task would keep running in the pool
            # after cancellation, so the future is cancelled immediately instead.
            if self._cancelled:
                if not fut_res.done():
                    fut_loop = fut_res.get_loop()
                    if fut_loop and fut_loop.is_running():
                        fut_loop.call_soon_threadsafe(fut_res.cancel)
                    else:
                        fut_res.cancel()
            else:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                self._futures.append((loop, fut_res))

        return fut_res

    def cancel(self) -> None:
        """Cancel this context and thread-safely cascade to all child contexts and futures."""
        with self._lock:
            if self._cancelled:
                return
            self._cancelled = True
            self._cancel_scope.cancel()
            children = list(self._children)
            futures = list(self._futures)
            self._children.clear()
            self._futures.clear()

        for child in children:
            child.cancel()

        for loop, fut in futures:
            if not fut.done():
                if loop and loop.is_running():
                    loop.call_soon_threadsafe(fut.cancel)
                else:
                    fut.cancel()
