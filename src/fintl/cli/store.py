import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.text import Text

from fintl.accounts_etl.registry import ALL_PARSERS
from fintl.accounts_etl.schemas import Config
from fintl.accounts_etl.store import store_files
from fintl.fine_logging import setup_logging

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(
    help="Store downloaded bank files into the correct ETL input directories."
)


@app.command()
def run(
    from_dir: Path = typer.Option(
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
) -> None:
    """Scan a folder for downloaded bank files and copy them to the right ETL input directory.

    Each file is tested against all registered parser applicability predicates.
    For every match you are asked to confirm the proposed target directory before
    the file is copied.  Pass --yes to confirm all matches automatically.
    """
    source_dir = from_dir or Path.cwd()
    config = Config()
    setup_logging(config.logging)

    console.print(f"[bold]Scanning:[/bold] {source_dir}")

    def confirm(prompt: str) -> bool:
        if yes:
            console.print(Text(f"✔ {prompt}", style="green"))
            return True
        console.print()
        console.print(Text(prompt, style="cyan"))
        return typer.confirm("  Copy this file?", default=False)

    counts = store_files(source_dir, config, ALL_PARSERS, confirm=confirm)

    console.print()
    console.print(
        f"[bold]Done.[/bold] "
        f"Files matched: {counts['matched']} | "
        f"Copied: {counts['copied']} | "
        f"Skipped: {counts['skipped']} | "
        f"Unmatched: {counts['unmatched']}"
    )

    if counts["unmatched"] > 0:
        console.print(
            "[yellow]Some files were not recognised by any parser. "
            "Check the filenames or add new parser definitions.[/yellow]"
        )


if __name__ == "__main__":
    app()
