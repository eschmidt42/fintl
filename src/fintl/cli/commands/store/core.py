import logging
from pathlib import Path

import typer
from rich.console import Console

from fintl.cli.commands.store.helper import (
    Prompter,
    display_results_to_console,
    get_operation,
)
from fintl.common import ALL_PARSERS, Config, setup_logging, store_files

logger = logging.getLogger(__name__)


def run(
    from_dir: Path | None = typer.Option(
        None,
        "--from-dir",
        "-d",
        help="Directory to scan for downloaded files. Defaults to the current working directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Auto-confirm all matches without prompting.",
    ),
    copy: bool = typer.Option(
        False,
        "--copy",
        help="Copy files instead of moving them.",
    ),
) -> None:
    """Scan a folder for downloaded bank files and copy/move them to the right ETL input directory.

    Each file is tested against all registered parser applicability predicates.
    By default, matched files are moved. Pass --copy to retain the original files.
    For every match you are asked to confirm the proposed target directory before
    the operation occurs.  Pass --yes to confirm all matches automatically.
    """
    source_dir = from_dir or Path.cwd()
    config = Config()
    setup_logging(config.logging)

    operation, op_label = get_operation(copy)

    console = Console()
    console.print(f"[bold]Scanning:[/bold] {source_dir}")
    prompter = Prompter(yes=yes, copy=copy, op_label=op_label, config=config, console=console)

    counts = store_files(
        source_dir,
        config,
        ALL_PARSERS,
        operation=operation,
        confirm=prompter.confirm,
        choose=prompter.choose,
    )

    display_results_to_console(op_label, counts, console)
