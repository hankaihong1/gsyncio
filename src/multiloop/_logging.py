"""Structured task-aware logging configuration for multiloop."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

__all__ = ["get_logger", "set_log_level"]

_logger = logging.getLogger("multiloop")
_logger.setLevel(logging.WARNING)
_logger.addHandler(logging.NullHandler())


def _current_task_id() -> int | None:
    """Return the current asyncio task id, or None outside a running task."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    return id(task) if task is not None else None


class _MultiloopLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """A :class:`logging.LoggerAdapter` that injects structured fields into log records.

    Automatically injects ``task_id`` (the id of the current asyncio Task) and ``span``
    into every log record's ``extra`` dictionary.
    """

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("task_id", _current_task_id())
        extra.setdefault("span", None)
        return msg, kwargs


def set_log_level(level: int) -> None:
    """Set the multiloop logger's minimum log level.

    :param level: A :mod:`logging` level constant (e.g. ``logging.INFO``, ``logging.DEBUG``).
    """
    _logger.setLevel(level)


def get_logger(name: str | None = None) -> logging.LoggerAdapter[logging.Logger]:
    """Return a structured logger adapter for the multiloop namespace.

    :param name: Optional sub-logger name (e.g. ``"pool"``, ``"server"``).
    :returns: A :class:`logging.LoggerAdapter` injecting ``task_id`` and ``span``.
    """
    logger = logging.getLogger(f"multiloop.{name}") if name else _logger
    return _MultiloopLoggerAdapter(logger, {})
