"""CLI command for plotting bank account balances."""

import pathlib
from typing import Annotated, Optional

import typer

from fintl.cli.commands.plot.helper import display_plot, draw_plot, load_data
from fintl.common import Config


def run(
    save: Annotated[
        Optional[pathlib.Path],
        typer.Option("--save", help="Save chart as HTML to this path"),
    ] = None,
    y_min: Annotated[
        float,
        typer.Option("--y-min", help="Lower y-axis limit"),
    ] = 0,
    y_max: Annotated[
        float,
        typer.Option("--y-max", help="Upper y-axis limit"),
    ] = 250_000,
):
    """Plot bank account balances."""
    if y_min >= y_max:
        raise typer.BadParameter("--y-min must be less than --y-max")

    config = Config()

    balances = load_data(config)

    chart = draw_plot(balances, y_min=y_min, y_max=y_max)

    display_plot(save, chart)
