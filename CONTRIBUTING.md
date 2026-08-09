# Contributing to gsyncio

First off, thank you for taking the time to contribute! 🎉

This document outlines how to set up a development environment, run the test
suite, satisfy the linting/type-checking gates, and get your changes merged.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before participating —
we expect all contributors to uphold it.

---

## Table of Contents

- [Development Setup](#development-setup)
- [Building the Rust Extension](#building-the-rust-extension)
- [Testing](#testing)
- [Linting & Type Checking](#linting--type-checking)
- [PR Process](#pr-process)

---

## Development Setup

`gsyncio` is a Python 3.14t (free-threaded / no-GIL) library with a Rust core
built with PyO3 and maturin. All tooling is managed through **`uv`** — please do
not use `pip`.

### Prerequisites

- **Python** `>= 3.14` (free-threaded build recommended)
- **Rust** toolchain (stable) — needed to compile the `_gsyncio_core` extension
- **`uv`** — the package/project manager used throughout this repo

### Install dev dependencies

```bash
uv sync --group dev
```

This installs the dev dependency group from `pyproject.toml`, including
`maturin`, `mypy`, `pytest`, `pytest-asyncio`, `pytest-repeat`,
`ruff`, `httpx`, and `winuvloop`.

---

## Building the Rust Extension

The core engine (`_gsyncio_core`) is a Rust cdylib (PyO3 + `flume` + `parking_lot`)
that must be compiled before Python code can import `gsyncio`.

Build it with the release profile:

```bash
uv run maturin develop --release
```

> The `--release` profile is important — it enables `opt-level = 3`, LTO, and
> single codegen units as configured in `Cargo.toml`, which is what produces the
> performance characteristics the benchmarks rely on.

---

## Testing

The full test suite lives under `tests/` and is driven by `pytest`.

Run the full test suite:

```bash
uv run pytest
```

Useful variations:

```bash
# Run a single test file
uv run pytest tests/test_channels.py

# Run a specific test
uv run pytest tests/test_channels.py::test_send_recv

# Repeat a test to shake out flaky concurrency behavior
uv run pytest tests/test_pool.py -p no:cacheprovider --count=50
```

> Because `gsyncio` is a concurrency library, `pytest-repeat` (`--count=N`) is
> your friend for stress-testing channels, pools, and wait groups.

### Quick Start (5 minutes to first run)

```bash
uv sync --group dev
uv run maturin develop --release   # compile the Rust core
uv run python examples/00_pool_basics.py   # run the first example
```

- Not sure which primitive to use? See the decision table in
  [docs/CHOOSING.md](docs/CHOOSING.md).
- About to touch concurrency code? Read
  [docs/CONCURRENCY.md](docs/CONCURRENCY.md) first — it lists the eight
  race-condition trap patterns and the mandatory pre-change checklist.
- More runnable examples: [`examples/README.md`](examples/README.md).

### Documentation (EN/ZH mirrors)

Every doc exists in an English original plus a Chinese mirror:
`README.md` ↔ `README_ZH.md`, `docs/API.md` ↔ `docs/API_ZH.md`,
`docs/CHOOSING.md` ↔ `docs/CHOOSING_ZH.md`,
`docs/CONCURRENCY.md` ↔ `docs/CONCURRENCY_ZH.md` (examples/ same).

- Always edit the English original and the Chinese mirror **in the same PR**.
- The two versions must stay structurally identical (same sections, same
  headings, same code blocks) — `tests/test_docs.py` enforces this.
- The language-switch link at the top of each doc must keep pointing at the
  other version.

### Benchmarks

Performance-sensitive changes should be validated against the benchmark suite:

```bash
uv run python benchmarks/bench_multithread_loops.py
uv run python benchmarks/benchmark_pull_model.py
uv run python benchmarks/bench_asgi_throughput.py
uv run python benchmarks/benchmark_winuvloop.py
```

---

## Linting & Type Checking

The CI gates are strict: `ruff` for lint/format, `mypy` in strict mode for
Python typing, and `clippy`/`fmt` for the Rust side.

### Python lint & format (ruff)

```bash
uv run ruff check .
uv run ruff format .
```

### Python type checking (mypy, strict)

```bash
uv run mypy src/gsyncio
```

`mypy` runs with `strict = true` and `disallow_untyped_defs = true` (see
`pyproject.toml`) — every public function must be fully typed.

### Rust lint & format

```bash
cargo clippy -- -D warnings
cargo fmt --check
```

`clippy` is run with `-D warnings`, so any warning fails the gate. Run
`cargo fmt` (without `--check`) to auto-format before submitting.

### Pre-submission checklist

Before opening a PR, make sure the following all pass:

```bash
uv run maturin develop --release   # builds the Rust core
cargo clippy -- -D warnings
cargo fmt --check
uv run mypy src/gsyncio
uv run ruff check .
uv run ruff format .
uv run pytest
```

---

## PR Process

- All changes are submitted as pull requests **to `main`**.
- **Request before code**: open an issue/feature request first for any
  non-trivial change and wait for maintainer approval before writing code —
  it lets maintainers and other contributors weigh in on the design before
  effort is spent.
- **Green before merge**: a PR is merged only after **all** required CI
  checks pass (lint, type checks, tests). A red or incomplete PR is never
  merged; if a check fails, fix it in follow-up commits and wait for the
  checks to go green before requesting merge.
- Keep PRs focused: one logical change per PR, with a clear title and a
  description of *what* and *why*.
- **Label the PR** (`feature` / `fix` / `docs` / `chore` / `breaking`):
  Release Drafter groups release notes and derives the version bump from
  these labels — an unlabeled PR lands in the default "patch" category.
- Include tests for new functionality and update existing tests if behavior
  changes. Concurrency code should be tested with `pytest-repeat` to catch
  races.
- Ensure the [pre-submission checklist](#pre-submission-checklist) passes
  before requesting review.
- Address review feedback in follow-up commits (do not force-push over review
  history unless asked).
- When your PR is approved and green, a maintainer will merge it into `main`.

---

## License

By contributing, you agree that your contributions will be licensed under the
same [MIT License](LICENSE) as the rest of the project.
