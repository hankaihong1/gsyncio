"""Shared fixtures for gsyncio tests."""

import pytest

from gsyncio.testing import rust_available, wait_all_tasks_blocked

skip_if_no_rust = pytest.mark.skipif(
    not rust_available(), reason="_gsyncio_core rust extension not available"
)


@pytest.fixture
def yielder():
    """Provide wait_all_tasks_blocked as a test-local name for brevity."""
    return wait_all_tasks_blocked
