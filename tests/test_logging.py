import io
import json
import logging
import logging.config
import logging.handlers
import sys
from pathlib import Path

from rich.console import Console

from fintl.fine_logging import (
    DependencyFilter,
    JSONFormatter,
    Logging,
    WarningBufferHandler,
    _build_table,
    print_warning_summary,
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


# ── JSONFormatter: exc_info, stack_info, custom attributes ────────────────────


def test_JSONFormatter_with_exc_info():
    """_prepare_log_dict must include exc_info in the message when set."""
    formatter = JSONFormatter()
    try:
        raise ValueError("test error")
    except ValueError:
        import sys

        exc_info = sys.exc_info()

    logger = logging.getLogger("test_exc")
    record = logger.makeRecord(
        name="test_exc",
        level=logging.ERROR,
        fn="file.py",
        lno=1,
        msg="error occurred",
        args=None,  # type: ignore
        exc_info=exc_info,
    )
    message = formatter.format(record)
    parsed = json.loads(message)
    assert "exc_info" in parsed


def test_JSONFormatter_with_stack_info():
    """_prepare_log_dict must include stack_info in the message when set."""

    formatter = JSONFormatter()
    logger = logging.getLogger("test_stack")
    record = logger.makeRecord(
        name="test_stack",
        level=logging.DEBUG,
        fn="file.py",
        lno=1,
        msg="stack trace",
        args=None,  # type: ignore
        exc_info=None,
    )
    record.stack_info = "stack info here"
    message = formatter.format(record)
    parsed = json.loads(message)
    assert "stack_info" in parsed


def test_JSONFormatter_with_extra_attributes():
    """Custom attributes added to a log record must appear in the JSON output."""
    formatter = JSONFormatter()
    logger = logging.getLogger("test_extra")
    record = logger.makeRecord(
        name="test_extra",
        level=logging.INFO,
        fn="file.py",
        lno=1,
        msg="extra attr",
        args=None,  # type: ignore
        exc_info=None,
    )
    record.custom_key = "custom_value"  # type: ignore[attr-defined]
    message = formatter.format(record)
    parsed = json.loads(message)
    assert parsed.get("custom_key") == "custom_value"


# ── DependencyFilter ──────────────────────────────────────────────────────────


def test_dependency_filter_allows_1st_party_logger():
    """A logger whose name starts with 'fintl', or '__main__' must always be allowed regardless of level."""
    filter_ = DependencyFilter(param=logging.WARNING)
    for name in ("fintl.something", "__main__"):
        record = logging.LogRecord(
            name=name,
            level=logging.DEBUG,
            pathname="",
            lineno=0,
            msg="",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True


def test_dependency_filter_blocks_3rd_party_below_threshold():
    """A 3rd-party logger with a level below param must be filtered out."""
    filter_ = DependencyFilter(param=logging.WARNING)
    record = logging.LogRecord(
        name="some_library",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    )
    assert filter_.filter(record) is False


# ── Logging.path_valid ────────────────────────────────────────────────────────


def test_logging_path_valid_none_returns_none():
    """path_valid validator must return None when config_file is explicitly None."""
    log_cfg = Logging(config_file=None)
    assert log_cfg.config_file is None


# ── setup_logging ─────────────────────────────────────────────────────────────


def test_setup_logging_with_config_file_calls_from_json():
    """setup_logging must delegate to setup_logging_from_json when config_file is set."""
    from unittest.mock import patch

    from fintl.fine_logging import setup_logging

    config_file = Path(__file__).parent / "logger-config.json"
    log_cfg = Logging(config_file=config_file)

    with patch("fintl.fine_logging.setup_logging_from_json") as mock_json:
        setup_logging(log_cfg)

    mock_json.assert_called_once_with(config_file)


def test_setup_logging_without_config_file_calls_from_toml():
    """setup_logging must delegate to setup_logging_from_toml when config_file is None."""
    from unittest.mock import patch

    from fintl.fine_logging import setup_logging

    log_cfg = Logging()  # config_file defaults to None

    with patch("fintl.fine_logging.setup_logging_from_toml") as mock_toml:
        setup_logging(log_cfg)

    mock_toml.assert_called_once_with(log_cfg)


def test_setup_logging_from_toml_no_queue_handler():
    """setup_logging_from_toml must not crash when no queue_handler is registered."""
    from unittest.mock import patch

    log_cfg = Logging()

    # Simulate an environment where getHandlerByName returns None.
    with patch("logging.getHandlerByName", return_value=None):
        setup_logging_from_toml(log_cfg)  # must not raise


def test_setup_logging_from_json_with_queue_handler():
    """setup_logging_from_json must start the queue_handler listener when present."""
    from pathlib import Path
    from unittest.mock import MagicMock, patch

    config_file = Path(__file__).parent / "logger-config.json"

    mock_handler = MagicMock(spec=logging.handlers.QueueHandler)
    mock_handler.listener = MagicMock()

    with patch("logging.getHandlerByName", return_value=mock_handler):
        setup_logging_from_json(config_file)

    mock_handler.listener.start.assert_called_once()


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_record(name: str, level: int, msg: str, exc_info=None) -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=level,
        pathname="somefile.py",
        lineno=42,
        msg=msg,
        args=(),
        exc_info=exc_info,
    )


def _render(renderable) -> str:
    buf = io.StringIO()
    console = Console(file=buf, width=120, no_color=True)
    console.print(renderable)
    return buf.getvalue()


def _summary_output(records: list[logging.LogRecord]) -> str:
    buf = io.StringIO()
    console = Console(file=buf, width=120, no_color=True)
    print_warning_summary(records, console)
    return buf.getvalue()


# ── WarningBufferHandler ──────────────────────────────────────────────────────


def test_warning_buffer_handler_emit():
    handler = WarningBufferHandler()
    record = _make_record("fintl.test", logging.WARNING, "watch out")
    handler.emit(record)
    assert len(handler.records) == 1
    assert handler.records[0] is record


def test_warning_buffer_handler_accumulates_multiple():
    handler = WarningBufferHandler()
    for i in range(3):
        handler.emit(_make_record("fintl.test", logging.WARNING, f"msg {i}"))
    assert len(handler.records) == 3


# ── _build_table ──────────────────────────────────────────────────────────────


def test_build_table_contains_message():
    record = _make_record("fintl.runner", logging.WARNING, "something bad")
    rendered = _render(_build_table([record]))
    assert "something bad" in rendered


def test_build_table_strip_prefix():
    record = _make_record("fintl.runner", logging.WARNING, "msg")
    rendered = _render(_build_table([record], strip_prefix="fintl."))
    assert "runner" in rendered
    assert "fintl.runner" not in rendered


def test_build_table_no_strip_prefix_keeps_full_name():
    record = _make_record("fintl.runner", logging.WARNING, "msg")
    rendered = _render(_build_table([record]))
    assert "fintl.runner" in rendered


def test_build_table_exc_info_appended_to_message():
    try:
        raise ValueError("something exploded")
    except ValueError:
        exc_info = sys.exc_info()
    record = _make_record("fintl.test", logging.ERROR, "error msg", exc_info=exc_info)
    rendered = _render(_build_table([record]))
    assert "ValueError" in rendered
    assert "something exploded" in rendered


def test_build_table_location_column():
    record = _make_record("fintl.test", logging.WARNING, "msg")
    rendered = _render(_build_table([record]))
    assert "somefile.py:42" in rendered


# ── print_warning_summary ─────────────────────────────────────────────────────


def test_print_warning_summary_empty_produces_no_output():
    assert _summary_output([]).strip() == ""


def test_print_warning_summary_fintl_only_shows_fintl_panel():
    record = _make_record("fintl.runner", logging.WARNING, "fintl problem")
    output = _summary_output([record])
    assert "fintl warnings+" in output
    assert "third-party" not in output


def test_print_warning_summary_third_party_only_shows_third_party_panel():
    record = _make_record("some_lib", logging.WARNING, "http warning")
    output = _summary_output([record])
    assert "third-party warnings+" in output
    assert "fintl warnings+" not in output


def test_print_warning_summary_mixed_shows_both_panels():
    records = [
        _make_record("fintl.etl", logging.ERROR, "fintl error"),
        _make_record("httpx", logging.WARNING, "http warning"),
    ]
    output = _summary_output(records)
    assert "fintl warnings+" in output
    assert "third-party warnings+" in output


def test_print_warning_summary_count_in_title():
    records = [_make_record("fintl.x", logging.WARNING, f"msg {i}") for i in range(3)]
    output = _summary_output(records)
    assert "(3)" in output
