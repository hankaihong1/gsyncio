"""Shared helpers for lazy-importing the optional ``_gsyncio_core`` Rust extension."""

from typing import Any

__all__ = ["_try_import_rust_class"]


def _try_import_rust_class(module_path: str, class_name: str) -> Any | None:
    """Import a class from the Rust extension, or return ``None`` if it is missing.

    The ``_gsyncio_core`` C-extension is optional: it is only compiled on
    supported platforms. Every module that depends on it follows the same
    pattern, so the fallback is centralized here.

    :param module_path: Full dotted module path (e.g. ``"gsyncio._gsyncio_core"``).
    :param class_name: Name of the class to import from that module.
    :returns: The class object, or ``None`` if the extension is unavailable.
    """
    try:
        mod = __import__(module_path, fromlist=[class_name])
        return getattr(mod, class_name)
    except (ImportError, AttributeError):
        return None
