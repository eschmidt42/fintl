import logging

import rich.logging
import typer
from rich.console import Console

from fintl.accounts_etl import process_accounts
from fintl.accounts_etl.schemas import Config
from fintl.fine_logging import (
    WarningBufferHandler,
    print_warning_summary,
    setup_logging,
)

app = typer.Typer(help="Run the accounts ETL pipeline.")


@app.command()
def run():
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
            print_warning_summary(buf.records, console)


if __name__ == "__main__":
    app()
