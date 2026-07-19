"""Tests de configure_logging y JSONFormatter."""

import json
import logging

import pytest

from shared.logging.config import JSONFormatter, configure_logging


@pytest.fixture(autouse=True)
def reset_root_logger():
    yield
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.WARNING)


def test_configure_logging_sets_correct_level():
    configure_logging(service="test-service", level="DEBUG")

    root_logger = logging.getLogger()
    assert root_logger.level == logging.DEBUG


def test_configure_logging_raises_on_invalid_level():
    with pytest.raises(ValueError):
        configure_logging(service="test-service", level="NOT_A_LEVEL")


def test_configure_logging_does_not_duplicate_handlers():
    configure_logging(service="test-service", level="INFO")
    configure_logging(service="test-service", level="INFO")

    root_logger = logging.getLogger()
    assert len(root_logger.handlers) == 1


def test_json_formatter_produces_expected_fields():
    formatter = JSONFormatter(service="test-service")
    record = logging.LogRecord(
        name="my.module",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hola mundo",
        args=None,
        exc_info=None,
    )

    formatted = json.loads(formatter.format(record))

    assert formatted["service"] == "test-service"
    assert formatted["level"] == "INFO"
    assert formatted["logger"] == "my.module"
    assert formatted["message"] == "hola mundo"
    assert "timestamp" in formatted
