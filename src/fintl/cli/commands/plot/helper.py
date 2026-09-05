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


def display_plot(
    output_path: Path | None, chart: alt.TopLevelMixin, *, quiet: bool = False
) -> None:
    """Save the chart or open it in a temporary browser tab unless quiet."""
    if output_path is not None:
        chart.save(str(output_path))
        typer.echo(f"Chart saved to {output_path}")
        if not quiet:
            webbrowser.open(output_path.resolve().as_uri())
    elif not quiet:
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            tmp = pathlib.Path(f.name)
        chart.save(str(tmp))
        webbrowser.open(tmp.resolve().as_uri())
