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
):
    """Plot bank account balances."""
    config = Config()

    balances = load_data(config)

    chart = draw_plot(balances)

    display_plot(save, chart)
