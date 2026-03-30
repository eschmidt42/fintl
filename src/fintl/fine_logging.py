"""Logging

Based on: https://github.com/mCodingLLC/VideosSampleCode/blob/master/videos/135_modern_logging
"""

import atexit
import datetime as dt
import json
import logging
import logging.config
import logging.handlers
from enum import Enum
from pathlib import Path
from typing import override

import rich.logging
from pydantic import BaseModel, field_validator

from fintl.path_utils import normalize_path, sanity_check_path

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


class JSONFormatter(logging.Formatter):
    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
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


class DependencyFilter(logging.Filter):
    """Filter to only keep third party logrecords above `param`.

    logrecord: https://docs.python.org/3/library/logging.html#logrecord-attributes
    logging levels: https://docs.python.org/3/library/logging.html
    custom level handling: https://docs.python.org/3/howto/logging-cookbook.html#custom-handling-of-levels
    custom filters: https://docs.python.org/3/howto/logging-cookbook.html#configuring-filters-with-dictconfig
    """

    def __init__(self, param: int):
        self.param = param

    @override
    def filter(self, record: logging.LogRecord) -> bool:
        is_1st_party = (
            record.name.startswith("apps")
            or record.name.startswith("packages")
            or record.name == "__main__"
            or record.name.startswith("receipt")
        )
        is_3rd_party = not is_1st_party
        if is_3rd_party:
            allow = record.levelno >= self.param
            return allow
        else:
            return True


def setup_logging_from_json(config_file: Path):
    with config_file.open("r") as f:
        config = json.load(f)

    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")

    if queue_handler is not None and isinstance(
        queue_handler, logging.handlers.QueueHandler
    ):
        queue_handler.listener.start()  # type: ignore
        atexit.register(queue_handler.listener.stop)  # type: ignore


class LevelsEnum(str, Enum):  # see logging._nameToLevel for available levels
    critical = "CRITICAL"
    fatal = "FATAL"
    error = "ERROR"
    warn = "WARN"
    warning = "WARNING"
    info = "INFO"
    debug = "DEBUG"
    notset = "NOTSET"


class Logging(BaseModel):
    config_file: Path | None = None
    third_party_filter_level: int = 20
    handlers_stdout_level: LevelsEnum = LevelsEnum.info
    handlers_file_json_level: LevelsEnum = LevelsEnum.debug
    handlers_file_json_filename: str = "accounts-etl.log.jsonl"
    handlers_file_json_maxbytes: int = 10_000_000
    handlers_file_json_backup_count: int = 3
    root_level: LevelsEnum = LevelsEnum.debug

    @property
    def handlers_file_json_filename_expanded(self) -> Path:
        return Path(self.handlers_file_json_filename).expanduser()

    @field_validator("config_file")
    @classmethod
    def path_valid(cls, path: Path) -> Path | None:
        if path is None:
            return path
        path = normalize_path(path)
        sanity_check_path(path)
        return path

    def get_config_dict(self) -> dict:
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
            },
            "root": {
                "level": self.root_level.value,
                "handlers": ["stdout", "queue_handler"],
            },
            "loggers": {},
        }


def setup_logging_from_toml(log_config: Logging):
    config_dict = log_config.get_config_dict()
    logging.config.dictConfig(config_dict)
    queue_handler = logging.getHandlerByName("queue_handler")

    if queue_handler is not None and isinstance(
        queue_handler, logging.handlers.QueueHandler
    ):
        queue_handler.listener.start()  # type: ignore
        atexit.register(queue_handler.listener.stop)  # type: ignore


def setup_logging(log_config: Logging):
    if log_config.config_file:
        setup_logging_from_json(log_config.config_file)
    else:
        setup_logging_from_toml(log_config)
