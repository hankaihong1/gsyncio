"""Tests for multiloop CLI runner."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import pytest

from multiloop.cli import _watch_files_thread, build_parser, detect_interface, import_app

if TYPE_CHECKING:
    import pathlib


async def sample_asgi_app(scope: dict[str, object], receive: object, send: object) -> None:
    pass


def sample_wsgi_app(environ: dict[str, object], start_response: object) -> list[bytes]:
    return [b"ok"]


def test_import_app() -> None:
    """Verify application string import."""
    app = import_app("multiloop.cli:build_parser")
    assert app is build_parser

    with pytest.raises(ValueError, match="Invalid application specification"):
        import_app("invalid_spec_without_separator")

    with pytest.raises(AttributeError, match="has no attribute"):
        import_app("multiloop.cli:non_existent_attribute_123")


def test_detect_interface() -> None:
    """Verify interface detection for ASGI and WSGI applications."""
    assert detect_interface(sample_asgi_app) == "asgi"
    assert detect_interface(sample_wsgi_app) == "wsgi"


def test_build_parser() -> None:
    """Verify CLI parser options."""
    parser = build_parser()
    args = parser.parse_args(
        ["run", "main:app", "--host", "0.0.0.0", "--port", "9000", "--workers", "8"]
    )
    assert args.command == "run"
    assert args.app == "main:app"
    assert args.host == "0.0.0.0"
    assert args.port == 9000
    assert args.workers == "8"


def test_cli_file_watcher_thread(tmp_path: pathlib.Path) -> None:
    """Verify background file watcher detects modifications asynchronously."""
    test_file = tmp_path / "app.py"
    test_file.write_text("a = 1")

    reload_event = threading.Event()
    stop_event = threading.Event()

    t = threading.Thread(
        target=_watch_files_thread,
        args=(reload_event, stop_event, tmp_path, 0.05),
        daemon=True,
    )
    t.start()

    try:
        assert not reload_event.is_set()
        time.sleep(0.1)
        # Modify file
        test_file.write_text("a = 2")
        # Wait for watcher to trigger
        triggered = reload_event.wait(timeout=2.0)
        assert triggered
    finally:
        stop_event.set()
        t.join(timeout=1.0)
