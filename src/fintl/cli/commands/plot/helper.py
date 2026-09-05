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


def resolve_html_path(save_dir: Path | None, filename: str) -> Path:
    """Resolve a named output path or create a temporary HTML path."""
    output_path = save_dir / filename if save_dir is not None else None

    if output_path is None:
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            output_path = pathlib.Path(f.name)

    return output_path


def save_chart(chart: alt.TopLevelMixin, output_path: Path):
    """Save a chart to an HTML path and report the destination."""
    chart.save(str(output_path))
    typer.echo(f"Chart saved to {output_path}")


def display_plot(output_path: Path) -> None:
    """Open the chart contained in the html file in `output_path` in a browser tab."""
    webbrowser.open(output_path.resolve().as_uri())
