import typer

from fintl.cli.commands import run_etl, run_plot, run_search
from fintl.cli.commands.store import run as store

app = typer.Typer()

app.command(name="etl", help="Run the accounts ETL pipeline.")(run_etl)
app.command(name="plot", help="Plot bank account balances.")(run_plot)
app.command(
    name="store",
    help="Store downloaded bank files into the correct ETL input directories.",
)(store)
app.command(name="search", help="Interactively search bank transactions.")(run_search)


if __name__ == "__main__":
    app()
