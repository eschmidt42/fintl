"""Helper classes and functions for the store command."""

from pathlib import Path

import typer
from rich.console import Console
from rich.text import Text

from fintl.common import Config
from fintl.etl.common.schemas import ParserSpec
from fintl.etl.io.store import FileOperation


class Prompter:
    """Interactive prompter for confirming file operations during the store command."""

    def __init__(self, *, yes: bool, copy: bool, op_label: str, config: Config, console: Console):
        """Initialise Prompter."""
        self.yes = yes
        self.copy = copy
        self.op_label = op_label
        self.config = config
        self.console = console

    def confirm(self, prompt: str, op: FileOperation) -> bool:
        """Prompt the user to confirm a file operation, or auto-confirm when yes=True."""
        if self.yes:
            self.console.print(Text(f"✔ {self.op_label}d:", style="green", overflow="fold"))
            self.console.print(f"  | {prompt}", style="green")
            return True

        self.console.print()
        action_word = "Copy" if op == FileOperation.COPYING else "Move"
        self.console.print(Text(prompt, style="cyan"))

        return typer.confirm(f"  {action_word} this file?", default=(not self.copy))

    def choose(self, file: Path, specs: list[ParserSpec]) -> ParserSpec | None:
        """Prompt the user to select one parser when a file matches multiple, or skip."""
        if self.yes:
            self.console.print(
                Text(
                    f"⚠ {file.name}  matched {len(specs)} parsers — ambiguous, skipping.",
                    style="yellow",
                )
            )
            return None

        self.console.print()
        self.console.print(
            Text(f"⚠ {file.name} matched multiple parsers — select one:", style="yellow")
        )

        for i, spec in enumerate(specs, 1):
            source_dir_for_case = self.config.get_source_dir_from_case(spec.case)
            self.console.print(
                f"  [{i}] {spec.case.provider} / {spec.case.service} / {spec.case.parser}"
                f"  →  {source_dir_for_case}"
            )

        self.console.print("  [0] Skip this file")

        while True:
            raw = typer.prompt(f"Select parser (0–{len(specs)})", default="0")
            try:
                idx = int(raw)
            except ValueError:
                idx = -1

            if idx == 0:
                return None

            if 1 <= idx <= len(specs):
                return specs[idx - 1]

            self.console.print(f"  Please enter a number between 0 and {len(specs)}.")


def display_results_to_console(op_label: str, counts: dict[str, int], console: Console):
    """Print a summary of store operation counts to the console."""
    console.print()
    console.print(
        f"[bold]Done.[/bold] "
        f"Files matched: {counts['matched']} | "
        f"{op_label}: {counts['copied']} | "
        f"Skipped: {counts['skipped']} | "
        f"Unmatched: {counts['unmatched']} | "
        f"Ambiguous: {counts['ambiguous']}"
    )

    if counts["unmatched"] > 0:
        console.print(
            "[yellow]Some files were not recognised by any parser. "
            "Check the filenames or add new parser definitions.[/yellow]"
        )
    if counts["ambiguous"] > 0:
        console.print(
            "[yellow]Some files matched multiple parsers and were skipped. "
            "Review your parser applicability predicates.[/yellow]"
        )


def get_operation(copy: bool) -> tuple[FileOperation, str]:
    """Return the FileOperation and its label string based on the copy flag."""
    if copy:
        return FileOperation.COPYING, "Copied"
    return FileOperation.MOVING, "Moved"
