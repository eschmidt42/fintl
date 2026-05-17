"""Entry point for the fintl CLI application."""

import typer

from fintl.cli.commands import run_etl, run_plot, run_search, run_store

app = typer.Typer()

app.command(name="etl", help="Run the accounts ETL pipeline.")(run_etl)
app.command(name="plot", help="Plot bank account balances.")(run_plot)
app.command(
    name="store",
    help="Store downloaded bank files into the correct ETL input directories.",
)(run_store)
app.command(name="search", help="Interactively search bank transactions.")(run_search)


if __name__ == "__main__":  # pragma: no cover
    app()
