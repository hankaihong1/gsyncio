"""Logging configuration for gsyncio."""

import asyncio
import logging
from typing import Any

_logger = logging.getLogger("gsyncio")
_logger.setLevel(logging.WARNING)
_logger.addHandler(logging.NullHandler())


def _current_task_id() -> int | None:
    """Return the current asyncio task id, or None outside a running task."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    return id(task) if task is not None else None


class _GsyncioLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """A :class:`logging.LoggerAdapter` that injects structured fields.

    Every emitted record gains ``task_id`` (the ``id()`` of the current
    :class:`asyncio.Task`, or ``None`` when no task is running) and ``span``
    (a caller-provided span identifier, defaulting to ``None``) as
    ``extra`` fields, available to formatters via ``LogRecord.__dict__``.
    """

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("task_id", _current_task_id())
        extra.setdefault("span", None)
        return msg, kwargs


def set_log_level(level: int) -> None:
    """Set the gsyncio logger's minimum log level.

    :param level: A :mod:`logging` level constant (e.g. ``logging.INFO``).
    :type level: int

    """
    _logger.setLevel(level)


def get_logger(name: str | None = None) -> logging.LoggerAdapter[logging.Logger]:
    """Return a structured logger adapter for the gsyncio namespace.

    The returned adapter wraps the ``"gsyncio"`` logger (or the
    ``"gsyncio.<name>"`` sub-logger when *name* is given, which propagates to
    the root ``"gsyncio"`` logger and inherits its level) and injects
    ``task_id`` and ``span`` structured fields into every record.

    :param name: Optional sub-logger name (e.g. ``"pool"``).
    :type name: str or None

    :returns: A :class:`logging.LoggerAdapter` for the gsyncio logger.
    :rtype: :class:`logging.LoggerAdapter`

    """
    logger = logging.getLogger(f"gsyncio.{name}") if name else _logger
    return _GsyncioLoggerAdapter(logger, {})
