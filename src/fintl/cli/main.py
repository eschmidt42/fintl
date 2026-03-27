import typer

from fintl.cli.etl import run as etl
from fintl.cli.plot import run as plot
from fintl.cli.store import run as store

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

    from fintl.cli.search import main as search_main

    search_main()


if __name__ == "__main__":
    app()
