import pathlib
import tempfile
import webbrowser
from typing import Annotated, Optional

import polars as pl
import typer

from fintl.accounts_etl.schemas import Config

app = typer.Typer(help="Plot bank account balances.")


@app.command()
def run(
    save: Annotated[
        Optional[pathlib.Path],
        typer.Option("--save", help="Save chart as HTML to this path"),
    ] = None,
):
    """Plot bank account balances."""
    config = Config()
    balances = pl.read_parquet(config.target_dir / "all-balances.parquet")
    balances = balances.with_columns(
        name=pl.col("provider").str.to_lowercase()
        + " "
        + pl.col("service").str.to_lowercase()
    )

    chart = balances.plot.scatter(x="date", y="amount", color="name").properties(
        width=600, height=400
    )

    if save is not None:
        chart.save(str(save))
        typer.echo(f"Chart saved to {save}")
        webbrowser.open(save.resolve().as_uri())
    else:
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            tmp = pathlib.Path(f.name)
        chart.save(str(tmp))
        webbrowser.open(tmp.resolve().as_uri())


if __name__ == "__main__":
    app()
