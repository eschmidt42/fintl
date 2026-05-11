import logging
from typing import Annotated

import rich.logging
import typer
from rich.console import Console

from fintl.common import Config
from fintl.common.logging import (
    WarningBufferHandler,
    print_warning_summary,
    setup_logging,
)
from fintl.etl import process_accounts


def run(
    summarize_warnings: Annotated[
        bool,
        typer.Option("--summarize", help="Summarize warnings at the end"),
    ] = False,
):
    """Load configuration and run the accounts ETL pipeline."""
    config = Config()
    setup_logging(config.logging)
    try:
        process_accounts.main(config)
    finally:
        buf = logging.getHandlerByName("warning_buffer")
        if isinstance(buf, WarningBufferHandler) and buf.records:
            stdout_handler = logging.getHandlerByName("stdout")
            console = (
                stdout_handler.console
                if isinstance(stdout_handler, rich.logging.RichHandler)
                else Console()
            )
            if summarize_warnings:
                print_warning_summary(buf.records, console)
