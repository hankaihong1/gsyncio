"""Shared fixtures for multiloop tests."""

import pytest

from multiloop.testing import rust_available, wait_all_tasks_blocked

skip_if_no_rust = pytest.mark.skipif(
    not rust_available(), reason="_multiloop_core rust extension not available"
)


@pytest.fixture
def yielder():
    """Provide wait_all_tasks_blocked as a test-local name for brevity."""
    return wait_all_tasks_blocked
