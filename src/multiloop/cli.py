"""Command-line interface (CLI) for running FastAPI, Starlette, Django, and Flask applications."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import os
import pathlib
import signal
import subprocess
import sys
import threading
from typing import Any

from multiloop._logging import set_log_level
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

    if (
        hasattr(app, "routes")
        or hasattr(app, "router")
        or hasattr(app, "middleware_stack")
        or hasattr(app, "build_middleware_stack")
    ):
        return "asgi"
    if hasattr(app, "wsgi_app"):
        return "wsgi"

    try:
        sig = inspect.signature(app)
        param_names = [p.name.lower() for p in sig.parameters.values()]
        if "environ" in param_names or "start_response" in param_names:
            return "wsgi"
        if "scope" in param_names or "receive" in param_names or "send" in param_names:
            return "asgi"
        param_count = len(sig.parameters)
        if param_count == 3:
            return "asgi"
        if param_count == 2:
            return "wsgi"
    except (TypeError, ValueError):
        pass

    return "asgi"


def _scan_mtimes(watch_dir: pathlib.Path, ignored_patterns: set[str]) -> dict[pathlib.Path, float]:
    mtimes: dict[pathlib.Path, float] = {}
    for root, dirs, files in os.walk(watch_dir):
        dirs[:] = [d for d in dirs if d not in ignored_patterns and not d.startswith(".")]
        for file in files:
            if file.endswith((".py", ".html", ".json")):
                p = pathlib.Path(root) / file
                try:
                    mtimes[p] = p.stat().st_mtime
                except OSError:
                    pass
    return mtimes


def _watch_files_thread(
    reload_event: threading.Event,
    stop_event: threading.Event,
    watch_dir: pathlib.Path,
    poll_interval: float = 0.3,
    debounce_interval: float = 0.2,
) -> None:
    """Background thread watching source files for changes without blocking event loops."""
    ignored_patterns = {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        ".benchmarks",
        "node_modules",
        "target",
        ".hermes",
        ".codegraph",
        "dist",
        "build",
        ".tox",
        ".coverage",
        ".idea",
        ".vscode",
    }
    last_mtimes = _scan_mtimes(watch_dir, ignored_patterns)
    current_poll = poll_interval
    while not stop_event.is_set():
        if stop_event.wait(timeout=current_poll):
            break
        current_mtimes = _scan_mtimes(watch_dir, ignored_patterns)
        if current_mtimes != last_mtimes:
            # Reset poll interval upon detected changes
            current_poll = poll_interval
            # Debounce: wait for quiet period to prevent restart storms on multi-file edits
            while not stop_event.is_set():
                if stop_event.wait(timeout=debounce_interval):
                    return
                new_mtimes = _scan_mtimes(watch_dir, ignored_patterns)
                if new_mtimes == current_mtimes:
                    break
                current_mtimes = new_mtimes
            if not stop_event.is_set():
                reload_event.set()
            break
        else:
            # Smooth backoff up to 0.8s to avoid spinning on large projects
            current_poll = min(current_poll + 0.1, 0.8)


def _terminate_child(child_proc: subprocess.Popen[Any], sig: int = signal.SIGINT) -> None:
    if child_proc.poll() is not None:
        return
    if sys.platform != "win32":
        try:
            os.killpg(os.getpgid(child_proc.pid), sig)
        except (ProcessLookupError, OSError):
            try:
                child_proc.send_signal(sig)
            except OSError:
                pass
    else:
        child_proc.terminate()


def _kill_child(child_proc: subprocess.Popen[Any]) -> None:
    if child_proc.poll() is not None:
        return
    if sys.platform != "win32":
        try:
            os.killpg(os.getpgid(child_proc.pid), signal.SIGKILL)
        except (ProcessLookupError, OSError):
            try:
                child_proc.kill()
            except OSError:
                pass
    else:
        child_proc.kill()


def _run_supervisor(
    app_spec: str,
    host: str,
    port: int,
    workers: int | str,
    interface: str,
    log_level: str,
    watch_dir: pathlib.Path | None = None,
) -> int:
    """Multi-process supervisor that restarts child worker processes upon source modifications."""
    target_dir = watch_dir or pathlib.Path.cwd()
    cmd = [
        sys.executable,
        "-m",
        "multiloop.cli",
        "run",
        app_spec,
        "--host",
        host,
        "--port",
        str(port),
        "--workers",
        str(workers),
        "--interface",
        interface,
        "--log-level",
        log_level,
    ]

    stop_supervisor = threading.Event()

    def _handle_signal(_signum: int, _frame: Any) -> None:
        stop_supervisor.set()

    if sys.platform != "win32":
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    print(
        f"[multiloop] Supervisor active for '{app_spec}'. Auto-reload enabled across worker subprocesses."
    )

    while not stop_supervisor.is_set():
        reload_event = threading.Event()
        stop_watcher = threading.Event()
        watcher_thread = threading.Thread(
            target=_watch_files_thread,
            args=(reload_event, stop_watcher, target_dir),
            daemon=True,
        )
        watcher_thread.start()

        child_proc = subprocess.Popen(
            cmd,
            start_new_session=(sys.platform != "win32"),
        )

        child_crashed = False
        while not stop_supervisor.is_set() and not reload_event.is_set():
            try:
                ret = child_proc.wait(timeout=0.2)
                # Child process exited on its own (e.g. SyntaxError or startup crash)
                if not stop_supervisor.is_set():
                    child_crashed = True
                    print(
                        f"[multiloop] Worker subprocess exited (code {ret}). "
                        "Watching for file changes to reload...",
                        file=sys.stderr,
                    )
                    # Block until file modification is detected or supervisor is stopped
                    while not stop_supervisor.is_set() and not reload_event.is_set():
                        if stop_supervisor.wait(timeout=0.2):
                            break
                break
            except subprocess.TimeoutExpired:
                pass

        stop_watcher.set()

        if reload_event.is_set() and not stop_supervisor.is_set():
            print(
                "[multiloop] File modification detected. Gracefully restarting worker subprocess..."
            )
            if not child_crashed and child_proc.poll() is None:
                _terminate_child(child_proc, signal.SIGINT)
                try:
                    child_proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    _kill_child(child_proc)
                    child_proc.wait()
            continue

        if stop_supervisor.is_set():
            if child_proc.poll() is None:
                _terminate_child(child_proc, signal.SIGINT)
                try:
                    child_proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    _kill_child(child_proc)
                    child_proc.wait()
            break

    print("\n[multiloop] Supervisor shutdown complete.")
    return 0


async def serve_app(
    app_spec: str,
    host: str = "127.0.0.1",
    port: int = 8000,
    workers: int | str = "auto",
    reload: bool = False,
    interface: str = "auto",
    shutdown_event: asyncio.Event | None = None,
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

    exit_event = shutdown_event or asyncio.Event()

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

            loop = asyncio.get_running_loop()
            if sys.platform != "win32":
                for sig in (signal.SIGINT, signal.SIGTERM):
                    try:
                        loop.add_signal_handler(sig, exit_event.set)
                    except (NotImplementedError, RuntimeError):
                        pass

            await exit_event.wait()


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
        if args.reload:
            return _run_supervisor(
                app_spec=args.app,
                host=args.host,
                port=args.port,
                workers=args.workers,
                interface=args.interface,
                log_level=args.log_level,
            )

        try:
            asyncio.run(
                serve_app(
                    app_spec=args.app,
                    host=args.host,
                    port=args.port,
                    workers=args.workers,
                    reload=False,
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
