"""Command-line interface (CLI) for running FastAPI, Starlette, Django, and Flask applications."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import os
import pathlib
import signal
import sys
from typing import Any

from multiloop._logging import set_log_level
from multiloop._sync import Event
from multiloop.asgi import MultiloopASGIWorker
from multiloop.pool import EventLoopThreadPool
from multiloop.wsgi import MultiloopWSGIWorker

__all__ = ["build_parser", "detect_interface", "import_app", "main", "serve_app"]


def import_app(app_spec: str) -> Any:
    """Import application object from string specification (e.g. 'main:app' or 'app.server:app')."""
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    if ":" in app_spec:
        module_name, attr_name = app_spec.split(":", 1)
    elif "." in app_spec:
        module_name, attr_name = app_spec.rsplit(".", 1)
    else:
        raise ValueError(
            f"Invalid application specification: '{app_spec}'. Expected 'module:app' format."
        )

    module = importlib.import_module(module_name)
    try:
        app = getattr(module, attr_name)
    except AttributeError:
        raise AttributeError(f"Module '{module_name}' has no attribute '{attr_name}'.") from None
    return app


def detect_interface(app: Any) -> str:
    """Detect whether an application object implements ASGI or WSGI protocol."""
    if inspect.iscoroutinefunction(app):
        return "asgi"
    if callable(app) and inspect.iscoroutinefunction(app.__call__):
        return "asgi"

    try:
        sig = inspect.signature(app)
        param_count = len(sig.parameters)
        if param_count == 3:
            return "asgi"
        if param_count == 2:
            return "wsgi"
    except (TypeError, ValueError):
        pass

    if hasattr(app, "routes") or hasattr(app, "router") or hasattr(app, "middleware_stack"):
        return "asgi"
    if hasattr(app, "wsgi_app"):
        return "wsgi"

    return "asgi"


async def _watch_files_for_reload(reload_event: Event, poll_interval: float = 0.5) -> None:
    """Watch source files in current working directory for changes and trigger reload."""
    watch_dir = pathlib.Path.cwd()
    ignored_patterns = {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        ".pytest_cache",
        ".ruff_cache",
        "target",
    }

    def _get_mtimes() -> dict[pathlib.Path, float]:
        mtimes: dict[pathlib.Path, float] = {}
        for root, dirs, files in os.walk(watch_dir):
            dirs[:] = [d for d in dirs if d not in ignored_patterns]
            for file in files:
                if file.endswith((".py", ".html", ".json")):
                    p = pathlib.Path(root) / file
                    try:
                        mtimes[p] = p.stat().st_mtime
                    except OSError:
                        pass
        return mtimes

    last_mtimes = _get_mtimes()
    while not reload_event.is_set():
        await asyncio.sleep(poll_interval)
        current_mtimes = _get_mtimes()
        if current_mtimes != last_mtimes:
            reload_event.set()
            break


async def serve_app(
    app_spec: str,
    host: str = "127.0.0.1",
    port: int = 8000,
    workers: int | str = "auto",
    reload: bool = False,
    interface: str = "auto",
    shutdown_event: Event | None = None,
) -> None:
    """Start multiloop web server for the specified application."""
    if isinstance(workers, str) and workers == "auto":
        worker_count = os.cpu_count() or 4
    else:
        try:
            worker_count = int(workers)
            if worker_count <= 0:
                worker_count = os.cpu_count() or 4
        except ValueError:
            worker_count = os.cpu_count() or 4

    app = import_app(app_spec)
    app_interface = interface.lower()
    if app_interface == "auto":
        app_interface = detect_interface(app)

    exit_event = shutdown_event or Event()
    reload_trigger = Event()

    async with EventLoopThreadPool(num_threads=worker_count) as pool:
        if app_interface == "asgi":
            worker_server: MultiloopASGIWorker | MultiloopWSGIWorker = MultiloopASGIWorker(
                app=app, pool=pool, host=host, port=port
            )
        else:
            worker_server = MultiloopWSGIWorker(app=app, pool=pool, host=host, port=port)

        async with worker_server:
            bound_port = worker_server.port
            print(
                f"[multiloop] Serving {app_interface.upper()} app '{app_spec}' at http://{host}:{bound_port} "
                f"({worker_count} worker threads, Python {sys.version.split()[0]}t, reload={'on' if reload else 'off'})"
            )

            watcher_task = None
            if reload:
                loop = asyncio.get_running_loop()
                watcher_task = loop.create_task(_watch_files_for_reload(reload_trigger))

            loop = asyncio.get_running_loop()
            if sys.platform != "win32":
                for sig in (signal.SIGINT, signal.SIGTERM):
                    try:
                        loop.add_signal_handler(sig, exit_event.set)
                    except (NotImplementedError, RuntimeError):
                        pass

            while not exit_event.is_set() and not reload_trigger.is_set():
                await asyncio.sleep(0.1)

            if watcher_task and not watcher_task.done():
                watcher_task.cancel()
                try:
                    await watcher_task
                except (asyncio.CancelledError, Exception):
                    pass

    if reload_trigger.is_set() and not exit_event.is_set():
        print("[multiloop] File modification detected. Reloading server...")
        await serve_app(
            app_spec=app_spec,
            host=host,
            port=port,
            workers=workers,
            reload=reload,
            interface=interface,
            shutdown_event=exit_event,
        )


def build_parser() -> argparse.ArgumentParser:
    """Build multiloop command line argument parser."""
    parser = argparse.ArgumentParser(
        prog="multiloop",
        description="High-performance multi-event-loop concurrency engine and Web server runner.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    run_parser = subparsers.add_parser(
        "run", help="Run an ASGI (FastAPI) or WSGI (Django/Flask) application"
    )
    run_parser.add_argument(
        "app",
        type=str,
        help="Application import string, e.g. 'main:app' or 'my_project.wsgi:application'",
    )
    run_parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Bind socket host (default: 127.0.0.1)",
    )
    run_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Bind socket port (default: 8000, 0 for ephemeral)",
    )
    run_parser.add_argument(
        "--workers",
        default="auto",
        help="Number of worker event loop threads (default: auto)",
    )
    run_parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="Enable auto-reload upon file modifications",
    )
    run_parser.add_argument(
        "--interface",
        type=str,
        choices=["auto", "asgi", "wsgi"],
        default="auto",
        help="Application interface type (default: auto)",
    )
    run_parser.add_argument(
        "--log-level",
        type=str,
        choices=["debug", "info", "warning", "error"],
        default="info",
        help="Log verbosity level (default: info)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "run":
        set_log_level(args.log_level.upper())
        try:
            asyncio.run(
                serve_app(
                    app_spec=args.app,
                    host=args.host,
                    port=args.port,
                    workers=args.workers,
                    reload=args.reload,
                    interface=args.interface,
                )
            )
        except KeyboardInterrupt:
            print("\n[multiloop] Server stopped by user.")
        except Exception as exc:
            print(f"[multiloop] Error: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
