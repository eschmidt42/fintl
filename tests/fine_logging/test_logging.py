import json
import logging
import logging.config
from pathlib import Path

from fintl.fine_logging import (
    DependencyFilter,
    JSONFormatter,
    Logging,
    setup_logging_from_json,
    setup_logging_from_toml,
)


def test_JSONFormatter():
    logger = logging.getLogger("test")
    record = logger.makeRecord(
        name="test",
        level=42,
        fn="whot",
        lno=21,
        msg="some text",
        args=None,  # type: ignore
        exc_info=None,
    )
    formatter = JSONFormatter()
    message = formatter.format(record)

    # is json readable
    parsed_message = json.loads(message)

    # contains at least message with above string and a timestamp like
    # '{"message": "some text", "timestamp": "2024-06-23T06:19:06.740402+00:00"}'
    assert "message" in parsed_message
    assert parsed_message["message"] == "some text"
    assert "timestamp" in parsed_message


def test_setup_logging_with_json():
    # https://pytest-with-eric.com/fixtures/built-in/pytest-caplog/#Pytest-Caplog-Example

    logger = logging.getLogger("test")

    path_file = Path(__file__)
    config_file = path_file.parent / "logger-config.json"
    assert config_file.exists()
    setup_logging_from_json(config_file)

    assert isinstance(logger.root.handlers[0].filters[0], DependencyFilter)


def test_setup_logging_with_toml():
    # https://pytest-with-eric.com/fixtures/built-in/pytest-caplog/#Pytest-Caplog-Example

    logger = logging.getLogger("test2")

    config = Logging()
    setup_logging_from_toml(config)

    assert isinstance(logger.root.handlers[0].filters[0], DependencyFilter)
