"""Lock-free atomic performance metrics collector for EventLoopThreadPool."""

from __future__ import annotations

from typing import Any, Protocol

from multiloop._rust import _try_import_rust_class

__all__ = ["AtomicMetrics", "MetricsCollector"]


class _MetricsProtocol(Protocol):
    """Protocol for the native Rust AtomicMetrics class."""

    def __init__(self, num_threads: int) -> None: ...
    def inc_active(self, index: int) -> None: ...
    def dec_active(self, index: int) -> None: ...
    def get_active(self, index: int) -> int: ...
    def get_completed(self, index: int) -> int: ...
    def inc_global_pull(self, index: int) -> None: ...
    def get_global_pull(self, index: int) -> int: ...
    def inc_park(self, index: int) -> None: ...
    def get_park(self, index: int) -> int: ...
    def set_injection_queue_depth(self, index: int, depth: int) -> None: ...
    def get_injection_queue_depth(self, index: int) -> int: ...
    def inc_remote_schedule(self, index: int) -> None: ...
    def get_remote_schedule(self, index: int) -> int: ...


AtomicMetrics: type[_MetricsProtocol] | None = _try_import_rust_class(
    "multiloop._multiloop_core", "AtomicMetrics"
)


class MetricsCollector:
    """Wraps Rust ``AtomicMetrics`` lock-free counters into a Python-friendly telemetry collector.

    Provides per-worker ``inc_active`` / ``dec_active`` helpers and a snapshot dictionary
    for monitoring thread-pool performance, worker saturation, and work-stealing efficiency.
    """

    def __init__(self, num_threads: int) -> None:
        self._num_threads = num_threads
        self._metrics: _MetricsProtocol | None = None
        if AtomicMetrics is not None:
            self._metrics = AtomicMetrics(num_threads)

    @property
    def is_enabled(self) -> bool:
        """Return True when the native Rust metrics extension is loaded and active."""
        return self._metrics is not None

    def inc_active(self, index: int) -> None:
        """Increment the active task counter for worker at ``index``."""
        if self._metrics:
            self._metrics.inc_active(index)

    def dec_active(self, index: int) -> None:
        """Decrement active and increment completed task counters for worker at ``index``."""
        if self._metrics:
            self._metrics.dec_active(index)

    def get_active(self, index: int) -> int:
        """Get the current count of active tasks for worker at ``index``."""
        if self._metrics:
            return self._metrics.get_active(index)
        return 0

    def get_snapshot(self, is_running: bool) -> dict[str, Any]:
        """Return an instantaneous metrics snapshot across all worker threads.

        :param is_running: Whether the owning thread pool is active.
        :returns: Telemetry dictionary including active, completed, global pull, and park counts.
        """
        active_tasks = [
            self._metrics.get_active(i) if self._metrics else 0 for i in range(self._num_threads)
        ]
        completed_tasks = [
            self._metrics.get_completed(i) if self._metrics else 0 for i in range(self._num_threads)
        ]
        result: dict[str, Any] = {
            "is_running": is_running,
            "thread_count": self._num_threads,
            "completed_tasks": completed_tasks,
            "active_tasks": active_tasks,
        }
        if self._metrics is not None:
            result["global_pull_count"] = [
                self._metrics.get_global_pull(i) for i in range(self._num_threads)
            ]
            result["park_count"] = [self._metrics.get_park(i) for i in range(self._num_threads)]
            result["injection_queue_depth"] = [
                self._metrics.get_injection_queue_depth(i) for i in range(self._num_threads)
            ]
            result["remote_schedule_count"] = [
                self._metrics.get_remote_schedule(i) for i in range(self._num_threads)
            ]
        else:
            result["global_pull_count"] = [0] * self._num_threads
            result["park_count"] = [0] * self._num_threads
            result["injection_queue_depth"] = [0] * self._num_threads
            result["remote_schedule_count"] = [0] * self._num_threads
        return result
