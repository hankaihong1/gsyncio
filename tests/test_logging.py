"""Tests for gsyncio logging utilities."""

import logging

import pytest

from gsyncio import get_logger, set_log_level
from gsyncio._logging import _GsyncioLoggerAdapter, _logger


@pytest.mark.asyncio
async def test_get_logger_returns_adapter():
    """get_logger() returns a _GsyncioLoggerAdapter instance."""
    logger = get_logger()
    assert isinstance(logger, _GsyncioLoggerAdapter)


@pytest.mark.asyncio
async def test_get_logger_with_sub_name():
    """get_logger('sub') returns adapter for gsyncio.sub logger."""
    logger = get_logger("test_sub")
    assert isinstance(logger, _GsyncioLoggerAdapter)


@pytest.mark.asyncio
async def test_set_log_level_changes_level():
    """set_log_level changes the gsyncio logger level."""
    original = _logger.level
    try:
        set_log_level(logging.DEBUG)
        assert _logger.level == logging.DEBUG

        set_log_level(logging.ERROR)
        assert _logger.level == logging.ERROR
    finally:
        _logger.level = original
