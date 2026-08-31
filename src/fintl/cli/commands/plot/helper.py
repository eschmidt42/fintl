"""Helper functions for loading data and rendering balance plots."""

import pathlib
import tempfile
import webbrowser
from pathlib import Path

import altair as alt
import polars as pl
import typer

from fintl.common import Config


def load_data(config: Config) -> pl.DataFrame:
    """Load the all-balances parquet file and add a display name column."""
    balances = pl.read_parquet(config.target_dir / "all-balances.parquet")
    balances = balances.with_columns(
        name=pl.col("provider").str.to_lowercase() + " " + pl.col("service").str.to_lowercase()
    )

    return balances


def display_plot(save: Path | None, chart: alt.TopLevelMixin) -> None:
    """Save the chart to disk or open it in a temporary browser tab."""
    if save is not None:
        chart.save(str(save))
        typer.echo(f"Chart saved to {save}")
        webbrowser.open(save.resolve().as_uri())
    else:
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            tmp = pathlib.Path(f.name)
        chart.save(str(tmp))
        webbrowser.open(tmp.resolve().as_uri())
