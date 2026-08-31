"""CLI command for plotting bank account balances."""

import pathlib
from typing import Annotated, Optional

import typer

from fintl.cli.commands.plot.helper import (
    calc_month_means,
    calc_predictions,
    display_plot,
    draw_predictions,
    draw_raw_amounts,
    load_data,
)
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

    raw_balance_chart = draw_raw_amounts(balances, y_min=y_min, y_max=y_max)
    display_plot(save, raw_balance_chart)

    month_means = calc_month_means(balances)
    month_mean_diffs_chart = draw_raw_amounts(
        month_means.drop("amount").rename({"delta": "amount"})
    )
    display_plot(save, month_mean_diffs_chart)

    predictions = calc_predictions(month_means)
    predictions_chart = draw_predictions(predictions)
    display_plot(save, predictions_chart)
