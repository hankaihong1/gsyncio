"""Task-local cancellation scopes (anyio / trio inspired) for gsyncio."""

import asyncio
import contextvars
import math
import threading
from typing import Self

from gsyncio.exceptions import TimeoutError

_scope_stack_var: contextvars.ContextVar[list["CancelScope"]] = contextvars.ContextVar(
    "_cancel_scope_stack"
)


def _get_scope_stack() -> list["CancelScope"]:
    """Return the task-local scope stack, creating it on first access."""
    try:
        return _scope_stack_var.get()
    except LookupError:
        new_stack: list[CancelScope] = []
        _scope_stack_var.set(new_stack)
        return new_stack


class CancelScope:
    """A task-local cancellation scope that propagates cancellation hierarchically.

    Each scope lives on a per-task stack managed via :data:`contextvars.ContextVar`.
    Entered scopes track their own cancellation flag, an optional absolute deadline,
    and a *shield* that snapshots and clears the task's pending cancellation count
    on entry, restoring it on exit — it absorbs cancellations *already injected*
    before entry.  A cancellation delivered *while* inside a shielded scope is not
    deferred (unlike trio/anyio shields): it interrupts the body's next await.  For
    cleanup that must survive mid-flight cancellation, use a retry loop (see
    ``Condition._reacquire_lock``) instead of relying on the shield.

    Typical use::

        async with CancelScope(deadline=loop.time() + 5) as scope:
            await do_work()
            # scope.cancel_called is True if the deadline fired
    """

    def __init__(self, deadline: float = float("inf"), shield: bool = False) -> None:
        # WHY: a NaN deadline would flow into loop.call_later's timer heap,
        # corrupt the loop's selector-timeout computation and crash the whole
        # event loop with TypeError (probe R3-F). -inf is rejected too —
        # "already expired" is expressed with fail_after(0) / fail_at(past).
        if not (math.isfinite(deadline) or deadline == float("inf")):
            raise ValueError("deadline must be a finite number or float('inf')")
        self._cancel_called = False
        self._cancel_lock = threading.Lock()
        self._deadline = deadline
        self._shield = shield
        self._cancelled_caught = False
        self._convert_to_timeout = False
        self._task: asyncio.Task[object] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._deadline_handle: asyncio.TimerHandle | None = None
        self._saved_cancel_count = 0
        # R7-A accounting refinement (anyio `_pending_uncancellations` parity):
        # cancel() can set cancel_called WITHOUT injecting (task already done /
        # binding cleared by a racing __aexit__), so the __aexit__ compensation
        # must uncancel only the injections that actually happened — otherwise
        # it would consume a residual external/ancestor count.  Read and
        # written under _cancel_lock (cancel/__aenter__/__aexit__).
        self._injected = False

    # -- inspectable properties ------------------------------------------------

    @property
    def cancel_called(self) -> bool:
        """Return ``True`` after :meth:`cancel` has been called at least once."""
        with self._cancel_lock:
            return self._cancel_called

    @property
    def deadline(self) -> float:
        """The absolute monotonic deadline (``float('inf')`` means no deadline)."""
        return self._deadline

    @property
    def shield(self) -> bool:
        """Return ``True`` if this scope blocks parent cancellation."""
        return self._shield

    @property
    def cancelled_caught(self) -> bool:
        """Return ``True`` if this scope silently absorbs cancellation (move-on-*)."""
        return self._cancelled_caught

    # -- cancellation ---------------------------------------------------------

    def cancel(self) -> None:
        """Mark this scope as cancelled and inject into the hosting task.

        Idempotent — subsequent calls are no-ops.
        """
        with self._cancel_lock:
            if self._cancel_called:
                return
            self._cancel_called = True
            # WHY: _task/_loop are written under the same lock in __aenter__ /
            # __aexit__, so they must be read here under the lock too — on
            # free-threaded builds a bare read would race with __aexit__
            # clearing the binding.
            task = self._task
            loop = self._loop
            # R7-A injection accounting.  The ledger is decided from the
            # actual delivery below — task.cancel() is idempotent, so a
            # cancel() racing an already-cancelling host injects nothing
            # and must record nothing (R10 P5: recording a phantom
            # injection let the __aexit__ compensation consume a foreign
            # cancellation count).
            self._injected = False

        if task is not None and not task.done():
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if loop is not None and current_loop is not loop:
                try:
                    # WHY: the delivery happens later on the host loop; the
                    # callback records the actual outcome so the ledger is
                    # never a guess.
                    def _deliver() -> None:
                        delivered = task.cancel()
                        with self._cancel_lock:
                            self._injected = delivered and not task.done()

                    loop.call_soon_threadsafe(_deliver)
                except RuntimeError:
                    # WHY: the host loop was closed — the task is gone with
                    # it; nothing to cancel (R5 FIX-E).
                    pass
            else:
                delivered = task.cancel()
                with self._cancel_lock:
                    self._injected = delivered

    def _take_injected(self) -> bool:
        """Return and clear the injection ledger (TaskGroup failure path).

        WHY (R10 P5): the host task manually consumes the cancellation its
        own scope injected during sibling-cancel; clearing the ledger here
        stops the __aexit__ compensation from uncancelling a SECOND count —
        which would swallow an external cancellation that landed in between.
        The actual uncancel() must happen outside this lock (the host is
        never concurrently in __aenter__/__aexit__ at this point, but the
        lock keeps the ledger consistent with cross-thread cancel()).
        """
        with self._cancel_lock:
            injected = self._injected
            self._injected = False
            return injected

    def _effectively_cancelled(self) -> bool:
        """Whether *this* scope (or an unshielded ancestor) is cancelled.

        A shielded scope blocks parent cancellation from entering, so a
        shielded scope is never *effectively* cancelled by its ancestors
        (though a direct :meth:`cancel` call still affects it).
        """
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
        """Undo the ``__aenter__`` side effects before raising from it.

        WHY: ``__aexit__`` is never called when ``__aenter__`` raises
        (with-statement semantics), so a bare raise would leak a stale scope
        on the task-local stack, a live deadline timer that later cancels a
        task which already left the scope, and the ``_task``/``_loop``
        binding (R1 FIX-2).

        ``clear_binding=False`` keeps the host binding: the move_on expired-
        deadline path returns normally, so ``__aexit__`` runs and needs the
        binding to uncancel the injection (its ``finally`` clears it).
        """
        stack = _get_scope_stack()
        if stack and stack[-1] is self:
            stack.pop()
        else:
            stack = [s for s in stack if s is not self]
        _scope_stack_var.set(stack)
        if self._deadline_handle is not None:
            self._deadline_handle.cancel()
            self._deadline_handle = None
        if clear_binding:
            # The binding is cleared under the lock — cancel() reads
            # _task/_loop from any thread and must never see a torn binding.
            with self._cancel_lock:
                self._task = None
                self._loop = None

    def _restore_saved_cancel_count(self) -> None:
        """Re-inject the cancellation count a shielded enter cleared.

        Called on the aenter-raise paths where ``__aexit__`` never runs:
        the shield must give back what it took, or the task's pending
        cancellation is silently lost (U1 re-audit).  ``__aexit__`` uses
        the same helper so there is exactly one restoration implementation.
        """
        if not self._shield or self._saved_cancel_count <= 0:
            return
        with self._cancel_lock:
            task = self._task
        if task is not None:
            for _ in range(self._saved_cancel_count):
                task.cancel()

    # -- context manager protocol ---------------------------------------------

    async def __aenter__(self) -> Self:
        # WHY: A scope belongs to exactly one hosting task.  Entering it from a
        # second task silently overwrites _task, after which cancel() targets
        # the wrong task.  The check-and-set must be atomic under
        # _cancel_lock: on free-threaded builds a bare check-then-act is a
        # data race.
        with self._cancel_lock:
            host = asyncio.current_task()
            if self._task is not None and self._task is not host:
                raise RuntimeError(
                    "CancelScope cannot be shared across tasks: already entered by another task"
                )
            self._task = host
            self._loop = asyncio.get_running_loop()
            # R7-A: re-entry resets the injection accounting — the previous
            # round's compensation already consumed it (or nothing was
            # injected); the flag must be clean or a cancel-free round would
            # spuriously uncancel.
            self._injected = False

        stack = _get_scope_stack()
        stack.append(self)
        _scope_stack_var.set(stack)

        # Shield: clear any pending cancellation injected by parent scopes.
        # WHY: If a parent scope was cancelled before we entered, its injection is
        # already counted on this task. Leaving it would make the first await inside
        # the shield raise CancelledError and break the shield's promise. The count
        # is restored in __aexit__ so cancellation fires right after the shield.
        if self._shield:
            task = self._task
            if task is not None:
                self._saved_cancel_count = task.cancelling()
                for _ in range(self._saved_cancel_count):
                    task.uncancel()

        # Schedule a deadline timer when one is set.
        if self._deadline != float("inf"):
            loop = asyncio.get_running_loop()
            delta = self._deadline - loop.time()
            if delta <= 0:
                # WHY: the deadline is already expired.  self.cancel() followed
                # by a bare raise would (a) leak the injected cancellation
                # count — __aexit__ never runs, so the conversion paths below
                # would never uncancel — and (b) surface the wrong exception
                # class: fail_after(0) must raise TimeoutError and
                # move_on_after(0) must be silent (probe R3-A).
                if self._convert_to_timeout:
                    # WHY: the deadline expired without ever running its
                    # callback — mark the scope cancelled so cancel_called
                    # reflects the fired deadline (anyio parity), then roll
                    # back and raise; there is no injection to undo.
                    with self._cancel_lock:
                        self._cancel_called = True
                    self._restore_saved_cancel_count()
                    self._rollback_aenter()
                    raise TimeoutError() from None
                if self._cancelled_caught:
                    # Inject one cancellation: the body's first await raises
                    # CancelledError and __aexit__'s swallow branch uncancels
                    # it; an await-free body is compensated in __aexit__'s
                    # normal path instead.  The binding must survive the
                    # rollback — __aexit__ needs it to uncancel.
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

        # If an ancestor is already cancelled (and we are not shielded),
        # surface that cancellation immediately — but undo the stack push and
        # the deadline timer first (see _rollback_aenter).
        if self._effectively_cancelled():
            # WHY: only the scope's OWN pre-entry cancel is converted or
            # swallowed — inherited cancellation must pass through untouched
            # (trio/anyio: fail_after/move_on handle only their own cancel).
            if self._cancel_called and self._convert_to_timeout:
                # Mirror the delta <= 0 branch: __aexit__ never runs on an
                # entry raise, so the deadline conversion happens here.
                self._restore_saved_cancel_count()
                self._rollback_aenter()
                raise TimeoutError() from None
            if self._cancel_called and self._cancelled_caught:
                # WHY: move_on semantics — the body's first await raises
                # CancelledError and __aexit__'s swallow branch uncancels it
                # (an await-free body is compensated there instead).  Inject
                # via host.cancel() directly: self.cancel() is an idempotent
                # early-return once the flag is set, so it would not inject.
                # The binding must survive the rollback — __aexit__ needs it
                # to uncancel.
                if host is not None and not host.done():
                    # R7-A injection accounting: the F-1 pre-cancelled path
                    # bypasses self.cancel() (idempotent early return) and
                    # injects directly — record it manually so the __aexit__
                    # compensation can consume it (otherwise cancel_called is
                    # True but injected is False and the count leaks again).
                    with self._cancel_lock:
                        self._injected = True
                    host.cancel()
                self._rollback_aenter(clear_binding=False)
                return self
            # WHY: the raise below is a user-level raise — it does NOT
            # consume the task's pending cancellation count, so an ancestor's
            # injected cancel would deliver AGAIN at the next await (double
            # delivery of one cancellation).  Consume the pending count so
            # this raise IS the single delivery.  A shielded scope skips the
            # consume: its count was just restored above (the shield-deferred
            # parent cancellation) and must stay pending to surface.
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
        exc_tb: object,
    ) -> bool | None:
        # Snapshot the host binding once; all exit paths use it and the
        # binding is cleared in the finally block below.
        with self._cancel_lock:
            task = self._task
            injected = self._injected
        try:
            # Pop self from the task-local stack.
            stack = _get_scope_stack()
            if stack and stack[-1] is self:
                stack.pop()
            else:
                # Belt-and-suspenders: remove from anywhere in the stack.
                stack = [s for s in stack if s is not self]
            _scope_stack_var.set(stack)

            # Clean up the deadline timer.
            if self._deadline_handle is not None:
                self._deadline_handle.cancel()
                self._deadline_handle = None

            # Restore saved cancel count for shielded scopes.
            self._restore_saved_cancel_count()

            # fail_after / fail_at: convert CancelledError → TimeoutError
            # (only when this scope's own deadline fired / cancel was called).
            if (
                exc_type is not None
                and issubclass(exc_type, asyncio.CancelledError)
                and self._convert_to_timeout
                and self.cancel_called
            ):
                # WHY: The deadline cancel was counted on this task.  asyncio
                # only decrements the count when CancelledError propagates out
                # of the task top-level; converting it to TimeoutError here
                # would leak the count and fire a spurious CancelledError on a
                # later await.  Undo the count before raising — same order as
                # asyncio.timeout (uncancel first, then raise), so an external
                # cancel racing in after the uncancel still lands correctly.
                if task is not None:
                    task.uncancel()
                raise TimeoutError() from exc_val

            # move_on_* variants silently swallow CancelledError — but only the
            # one we injected ourselves.  An external task.cancel() must pass
            # through: swallowing it would silently break the caller's control
            # flow (the task would "succeed" even though it was cancelled).
            if (
                exc_type is not None
                and issubclass(exc_type, asyncio.CancelledError)
                and self._cancelled_caught
                and self.cancel_called
            ):
                if task is not None:
                    task.uncancel()
                return True

            # move_on with an await-free body: the deadline-expired injection
            # (__aenter__'s delta <= 0 branch) was never delivered by an await,
            # so the swallow branch above did not run — compensate the count
            # here or it leaks into outer scopes (R3 FIX-18).  The same
            # compensation covers a body that exits with a NON-Cancelled
            # exception before its first await: the injection is still
            # undelivered and would otherwise fire a spurious CancelledError
            # at the next await (N1 — pre-cancelled move_on + sync body raise).
            if (
                (exc_type is None or not issubclass(exc_type, asyncio.CancelledError))
                and self._cancelled_caught
                and self.cancel_called
                and task is not None
            ):
                task.uncancel()

            # R7-A (anyio `_pending_uncancellations` parity): the scope's own
            # injection must be consumed when the body exits without the
            # scope's CancelledError in flight — a caught or never-delivered
            # cancellation would otherwise leak its count into outer scopes,
            # where an enclosing shield snapshots it as a real cancel and
            # re-injects it on exit (probe R7-A2: spurious CE in unrelated
            # code), and _wait_children's uncancel accounting would consume
            # the wrong count.  uncancel() clamps at 0, so this is idempotent
            # with the three branches above (convert/swallow/N1); the shield
            # ancestor counts already restored by _restore_saved_cancel_count
            # are untouched.  The `injected` record guarantees only actual
            # injections are consumed — cancel() with a cleared binding or a
            # done host neither injects nor records, so the compensation can
            # never swallow external or ancestor counts.  The reset below is
            # a lock-free write: the host is inside its own synchronous
            # __aexit__ section, so a cross-thread cancel() callback cannot
            # run until the host yields and cannot rewrite the flag in this
            # window; __aenter__'s locked reset is the second line of defense
            # on re-entry.
            if self.cancel_called and injected and task is not None:
                task.uncancel()
                self._injected = False

            return None
        finally:
            # Release the host binding so the scope can be re-entered later.
            # WHY: kept under _cancel_lock — cancel() reads _task/_loop from
            # any thread and must never see a torn binding.
            with self._cancel_lock:
                self._task = None
                self._loop = None


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def fail_after(seconds: float) -> CancelScope:
    """Return a :class:`CancelScope` that raises :class:`TimeoutError` on expiry.

    Usage::

        async with fail_after(5):
            await long_operation()
    """
    loop = asyncio.get_running_loop()
    scope = CancelScope(deadline=loop.time() + seconds)
    scope._convert_to_timeout = True
    return scope


def move_on_after(seconds: float) -> CancelScope:
    """Return a :class:`CancelScope` that silently exits on timeout.

    The ``scope.cancelled_caught`` property will be ``True`` after the deadline
    fires.

    Usage::

        async with move_on_after(5) as scope:
            await maybe_slow()
        if scope.cancelled_caught:
            print("timed out silently")
    """
    loop = asyncio.get_running_loop()
    scope = CancelScope(deadline=loop.time() + seconds)
    scope._cancelled_caught = True
    return scope


def fail_at(deadline: float) -> CancelScope:
    """Return a :class:`CancelScope` with an absolute deadline that raises
    :class:`TimeoutError` on expiry."""
    scope = CancelScope(deadline=deadline)
    scope._convert_to_timeout = True
    return scope


def move_on_at(deadline: float) -> CancelScope:
    """Return a :class:`CancelScope` with an absolute deadline that silently
    exits on expiry."""
    scope = CancelScope(deadline=deadline)
    scope._cancelled_caught = True
    return scope


async def checkpoint() -> None:
    """Check for effective cancellation and raise if needed.

    Raises :exc:`asyncio.CancelledError` when the current scope — or any
    unshielded ancestor — has been cancelled.

    Call this periodically inside long-running coroutines that cannot
    ``await`` frequently.
    """
    stack = _get_scope_stack()
    if not stack:
        return
    current = stack[-1]
    if current._effectively_cancelled():
        task = asyncio.current_task()
        if task is not None:
            # R7-B: consume the pending injection so this raise IS the single
            # delivery — a user-level CancelledError raise does not decrement
            # cancelling() (same reasoning as __aenter__'s consume branch);
            # without it the pending _must_cancel fires a second CancelledError
            # at the next real await (probe R7-B: DOUBLE-DELIVERY).  Shielded
            # scopes never reach this branch (_effectively_cancelled returns
            # False for them), so a shield-deferred ancestor cancellation is
            # never consumed here.
            for _ in range(task.cancelling()):
                task.uncancel()
        raise asyncio.CancelledError()


def current_effective_deadline() -> float:
    """Walk the task-local scope stack and return the tightest deadline.

    Shielded scopes act as a barrier: deadlines from scopes outside a shielded
    scope are invisible to code inside the shield.  Returns ``float('inf')``
    when no deadline is active.
    """
    stack = _get_scope_stack()
    min_deadline = float("inf")
    for scope in reversed(stack):
        min_deadline = min(min_deadline, scope._deadline)
        if scope._shield:
            break
    return min_deadline
