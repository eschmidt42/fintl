import typer

from fintl.cli.etl import app as etl_app
from fintl.cli.plot import app as plot_app
from fintl.cli.store import app as store_app

app = typer.Typer()

app.add_typer(etl_app, name="etl")
app.add_typer(plot_app, name="plot")
app.add_typer(store_app, name="store")


@app.command()
def search():
    """Interactively search bank transactions."""

    from fintl.cli.search import main as search_main

    search_main()


if __name__ == "__main__":
    app()
