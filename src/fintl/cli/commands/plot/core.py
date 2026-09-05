"""CLI command for plotting bank account balances."""

import pathlib
from typing import Annotated, Optional

import typer

from fintl.cli.commands.plot.calc import calc_month_means, calc_predictions
from fintl.cli.commands.plot.draw import draw_predictions, draw_raw_amounts
from fintl.cli.commands.plot.helper import display_plot, load_data, resolve_html_path, save_chart
from fintl.common import Config


def run(
    save_dir: Annotated[
        Optional[pathlib.Path],
        typer.Option("--save-dir", help="Save charts as HTML files in this directory"),
    ] = None,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", help="Do not open charts in the browser"),
    ] = False,
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

    month_means = calc_month_means(balances)
    month_mean_diffs_chart = draw_raw_amounts(
        month_means.drop("amount").rename({"delta": "amount"})
    )

    predictions = calc_predictions(month_means)
    predictions_chart = draw_predictions(predictions)

    charts = (
        ("balances.html", raw_balance_chart),
        ("monthly-deltas.html", month_mean_diffs_chart),
        ("predictions.html", predictions_chart),
    )
    if save_dir is not None:
        save_dir.mkdir(parents=True, exist_ok=True)

    for filename, chart in charts:
        output_path = resolve_html_path(save_dir, filename)
        save_chart(chart, output_path)
        if quiet:
            continue
        display_plot(output_path)
