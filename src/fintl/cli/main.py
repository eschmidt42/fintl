import typer

from fintl.cli.commands.etl import run as etl
from fintl.cli.commands.plot import run as plot
from fintl.cli.commands.search import main as search_main
from fintl.cli.commands.store import run as store

app = typer.Typer()

app.command(name="etl", help="Run the accounts ETL pipeline.")(etl)
app.command(name="plot", help="Plot bank account balances.")(plot)
app.command(
    name="store",
    help="Store downloaded bank files into the correct ETL input directories.",
)(store)


@app.command()
def search():
    """Interactively search bank transactions."""

    search_main()


if __name__ == "__main__":
    app()
