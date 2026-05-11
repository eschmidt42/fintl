"""Logging

Based on: https://github.com/mCodingLLC/VideosSampleCode/blob/master/videos/135_modern_logging
"""

import atexit
import datetime as dt
import json
import logging
import logging.config
import logging.handlers
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import override

import rich.logging
from pydantic import BaseModel, field_validator
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from fintl.common.paths import normalize_path, sanity_check_path

LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}

_LEVEL_STYLES = {
    "WARNING": "yellow",
    "ERROR": "bold red",
    "CRITICAL": "bold reverse red",
}


class JSONFormatter(logging.Formatter):
    """A logging formatter that outputs log records as JSON strings.

    Attributes:
        fmt_keys (dict[str, str]): A mapping of JSON field names to LogRecord attribute names.
    """

    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        """Initializes the JSONFormatter.

        Args:
            fmt_keys (dict[str, str] | None): Optional mapping of target JSON keys
                to LogRecord attributes. Defaults to an empty dict.
        """
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        """Formats a log record as a JSON string.

        Args:
            record (logging.LogRecord): The log record to format.

        Returns:
            str: The JSON-encoded log record.
        """
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
        """Prepares a dictionary representation of the log record for JSON encoding.

        Args:
            record (logging.LogRecord): The log record to prepare.

        Returns:
            dict: A dictionary containing the log record attributes.
        """
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


class WarningBufferHandler(logging.Handler):
    """A logging handler that buffers warnings in memory for later summary.

    Attributes:
        records (list[logging.LogRecord]): A list of buffered log records.
    """

    def __init__(self):
        """Initializes the WarningBufferHandler with a WARNING level."""
        super().__init__(level=logging.WARNING)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord):
        """Buffers a log record.

        Args:
            record (logging.LogRecord): The log record to buffer.
        """
        self.records.append(record)


class DependencyFilter(logging.Filter):
    """Filter to only keep third party logrecords above `param`.

    This filter identifies 1st party logs (starting with 'fintl' or '__main__')
    and allows them all, while filtering 3rd party logs based on the provided
    level parameter.

    Args:
        param (int): The minimum logging level required for 3rd party logs to be kept.

    logrecord: https://docs.python.org/3/library/logging.html#logrecord-attributes
    logging levels: https://docs.python.org/3/library/logging.html
    custom level handling: https://docs.python.org/3/howto/logging-cookbook.html#custom-handling-of-levels
    custom filters: https://docs.python.org/3/howto/logging-cookbook.html#configuring-filters-with-dictconfig

    level & numerical value mapping (https://docs.python.org/3/library/logging.html#logging-levels):
    - NOTSET -> 0
    - DEBUG -> 10
    - INFO -> 20
    - WARNING -> 30
    - ERROR -> 40
    - CRITICAL -> 50
    """

    def __init__(self, param: int):
        """Initializes the DependencyFilter.

        Args:
            param (int): The minimum logging level for 3rd party logs.
        """
        self.param = param

    @override
    def filter(self, record: logging.LogRecord) -> bool:
        """Determines if a log record should be kept.

        Args:
            record (logging.LogRecord): The log record to filter.

        Returns:
            bool: True if the record should be kept, False otherwise.
        """
        is_1st_party = record.name.startswith("fintl") or record.name == "__main__"
        is_3rd_party = not is_1st_party
        if is_3rd_party:
            allow = record.levelno >= self.param
            return allow
        else:
            return True


def setup_logging_from_json(config_file: Path):
    """Sets up logging using a JSON configuration file.

    Args:
        config_file (Path): Path to the JSON configuration file.
    """
    with config_file.open("r") as f:
        config = json.load(f)

    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")

    if queue_handler is not None and isinstance(
        queue_handler, logging.handlers.QueueHandler
    ):
        queue_handler.listener.start()  # type: ignore
        atexit.register(queue_handler.listener.stop)  # type: ignore


class LevelsEnum(str, Enum):
    """Enum for logging levels.

    See logging._nameToLevel for available levels.
    """

    critical = "CRITICAL"
    fatal = "FATAL"
    error = "ERROR"
    warn = "WARN"
    warning = "WARNING"
    info = "INFO"
    debug = "DEBUG"
    notset = "NOTSET"


class Logging(BaseModel):
    """Configuration for the logging system.

    Attributes:
        config_file (Path | None): Optional path to a JSON logging config file.
        third_party_filter_level (int): Minimum level for 3rd party logs.
        handlers_stdout_level (LevelsEnum): Log level for stdout handler.
        handlers_file_json_level (LevelsEnum): Log level for JSON file handler.
        handlers_file_json_filename (str): Filename for the JSON log file.
        handlers_file_json_maxbytes (int): Max size of JSON log file before rotation.
        handlers_file_json_backup_count (int): Number of backup log files to keep.
        root_level (LevelsEnum): Root logger level.
    """

    config_file: Path | None = None
    third_party_filter_level: int = 20
    handlers_stdout_level: LevelsEnum = LevelsEnum.warning
    handlers_file_json_level: LevelsEnum = LevelsEnum.debug
    handlers_file_json_filename: str = "fintl.log.jsonl"
    handlers_file_json_maxbytes: int = 10_000_000
    handlers_file_json_backup_count: int = 3
    root_level: LevelsEnum = LevelsEnum.debug

    @property
    def handlers_file_json_filename_expanded(self) -> Path:
        """Returns the expanded path of the JSON log file.

        Returns:
            Path: The expanded path.
        """
        return Path(self.handlers_file_json_filename).expanduser()

    @field_validator("config_file")
    @classmethod
    def path_valid(cls, path: Path) -> Path | None:
        """Validates that the config file path is sane and normalized.

        Args:
            path (Path): The path to validate.

        Returns:
            Path | None: The normalized path if valid, otherwise None.
        """
        if path is None:
            return path
        path = normalize_path(path)
        sanity_check_path(path)
        return path

    def get_config_dict(self) -> dict:
        """Generates a logging configuration dictionary from the Logging object.

        Returns:
            dict: A dictionary compatible with logging.config.dictConfig.
        """
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "rich": {"format": "%(message)s", "datefmt": "%Y-%m-%dT%H:%M:%S%z"},
                "json": {
                    "()": JSONFormatter,
                    "fmt_keys": {
                        "level": "levelname",
                        "message": "message",
                        "timestamp": "timestamp",
                        "logger": "name",
                        "module": "module",
                        "function": "funcName",
                        "line": "lineno",
                        "thread_name": "threadName",
                    },
                },
            },
            "filters": {
                "third_party": {
                    "()": DependencyFilter,
                    "param": self.third_party_filter_level,
                }
            },
            "handlers": {
                "stdout": {
                    "class": rich.logging.RichHandler,
                    "formatter": "rich",
                    "level": self.handlers_stdout_level.value,
                    "filters": ["third_party"],
                },
                "file_json": {
                    "class": logging.handlers.RotatingFileHandler,
                    "level": self.handlers_file_json_level.value,
                    "formatter": "json",
                    "filename": self.handlers_file_json_filename_expanded,
                    "maxBytes": self.handlers_file_json_maxbytes,
                    "backupCount": self.handlers_file_json_backup_count,
                },
                "queue_handler": {
                    "class": logging.handlers.QueueHandler,
                    "handlers": ["file_json"],
                    "respect_handler_level": True,
                },
                "warning_buffer": {"()": WarningBufferHandler},
            },
            "root": {
                "level": self.root_level.value,
                "handlers": ["stdout", "queue_handler", "warning_buffer"],
            },
            "loggers": {},
        }


def setup_logging_from_toml(log_config: Logging):
    """Sets up logging using a Logging configuration object (TOML based).

    Args:
        log_config (Logging): The configuration object.
    """
    config_dict = log_config.get_config_dict()
    logging.config.dictConfig(config_dict)
    queue_handler = logging.getHandlerByName("queue_handler")

    if queue_handler is not None and isinstance(
        queue_handler, logging.handlers.QueueHandler
    ):
        queue_handler.listener.start()  # type: ignore
        atexit.register(queue_handler.listener.stop)  # type: ignore


def setup_logging(log_config: Logging):
    """Configures the logging system based on the provided configuration.

    Args:
        log_config (Logging): The configuration object.
    """
    if log_config.config_file:
        setup_logging_from_json(log_config.config_file)
    else:
        setup_logging_from_toml(log_config)


def _build_table(
    records: list[logging.LogRecord], strip_prefix: str | None = None
) -> Table:
    """Builds a Rich table from a list of log records.

    Args:
        records (list[logging.LogRecord]): The records to include in the table.
        strip_prefix (str | None): Optional prefix to strip from logger names.

    Returns:
        Table: A Rich Table object.
    """
    table = Table(
        show_header=True, header_style="bold", show_edge=False, padding=(0, 1)
    )
    table.add_column("Level", no_wrap=True, width=9)
    table.add_column("Logger", no_wrap=True, style="dim")
    table.add_column("Message")
    table.add_column("Location", no_wrap=True, style="dim")

    for r in records:
        logger_name = r.name
        if strip_prefix and logger_name.startswith(strip_prefix):
            logger_name = logger_name[len(strip_prefix) :]

        message = r.getMessage()
        if r.exc_info:
            exc = r.exc_info[1]
            message += f"\n  {type(exc).__name__}: {exc}"

        table.add_row(
            Text(r.levelname, style=_LEVEL_STYLES.get(r.levelname, "")),
            logger_name,
            message,
            f"{r.filename}:{r.lineno}",
        )

    return table


def print_warning_summary(records: list[logging.LogRecord], console: Console) -> None:
    """Prints a formatted summary of warnings to the console.

    Args:
        records (list[logging.LogRecord]): The buffered warning records.
        console (Console): The Rich console to use for printing.
    """
    fintl = [r for r in records if r.name.startswith("fintl")]
    third_p = [r for r in records if not r.name.startswith("fintl")]

    if fintl:
        console.print(
            Panel(
                _build_table(fintl, strip_prefix="fintl."),
                title=f"[yellow]⚠ fintl warnings+  ({len(fintl)})[/yellow]",
                border_style="yellow",
            )
        )

    if third_p:
        console.print(
            Panel(
                _build_table(third_p),
                title=f"[red]⚠ third-party warnings+  ({len(third_p)})[/red]",
                border_style="red",
            )
        )


def flush_warning_summary(enabled: bool):
    """Flushes and prints the warning buffer to the console if enabled.

    Args:
        enabled (bool): Whether to print the summary.
    """
    if not enabled:
        return

    buf = logging.getHandlerByName("warning_buffer")

    if isinstance(buf, WarningBufferHandler) and buf.records:
        stdout_handler = logging.getHandlerByName("stdout")
        console = (
            stdout_handler.console
            if isinstance(stdout_handler, rich.logging.RichHandler)
            else Console()
        )
        if enabled:
            print_warning_summary(buf.records, console)


@contextmanager
def warning_summary_scope(enabled: bool):
    """Context manager that flushes the warning summary upon exit.

    Args:
        enabled (bool): Whether to print the summary on exit.

    Yields:
        None
    """
    try:
        yield
    finally:
        flush_warning_summary(enabled)
