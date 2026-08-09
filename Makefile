.PHONY: develop build test bench lint format docs clean all

develop:
	uv run maturin develop --release

build:
	uv run maturin build --release

test:
	uv run pytest

bench:
	for f in benchmarks/benchmark_pull_model.py benchmarks/bench_asgi_throughput.py benchmarks/bench_multithread_loops.py; do \
		uv run python "$$f"; \
	done

lint:
	uv run ruff check .
	uv run mypy --strict src/gsyncio
	uv run pyright src/gsyncio
	cargo clippy -- -D warnings
	cargo fmt --check

format:
	uv run ruff format .
	cargo fmt

docs:
	uv run --with sphinx --with sphinx-rtd-theme sphinx-build -b html docs docs/sphinx_html

clean:
	cargo clean
	rm -rf .pytest_cache .ruff_cache .mypy_cache

all: develop lint test
