from typing import Annotated

import typer

from fintl.common import Config
from fintl.common.logging import (
    setup_logging,
    warning_summary_scope,
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
    with warning_summary_scope(summarize_warnings):
        process_accounts.main(config)
