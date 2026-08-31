"""Helper functions for loading data and rendering balance plots."""

import datetime
import pathlib
import tempfile
import webbrowser
from pathlib import Path
from typing import cast

import altair as alt
import polars as pl
import statsmodels.api as sm
import typer
from dateutil.relativedelta import relativedelta

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


def calc_month_means(balances: pl.DataFrame) -> pl.DataFrame:
    """Calculate the monthly means for each `name`."""
    month_means = (
        balances.sort("date")
        .group_by("name", pl.col("date").dt.month_end(), maintain_order=True)
        .agg(pl.col("amount").last())
    )
    month_means = month_means.sort(["name", "date"]).with_columns(
        **{"delta": pl.col("amount").diff().over("name")}
    )
    return month_means


def calc_predictions(
    month_means: pl.DataFrame,
    *,
    n_predicted_months: int = 3,
    n_months_history: int = 6,
    order: tuple[int, int, int] = (1, 1, 0),
    trend: str = "n",
) -> pl.DataFrame:
    """Calculate time series predictions for each `name`."""
    prediction_dfs: list[pl.DataFrame] = []

    for name, _means in month_means.group_by("name", maintain_order=True):
        if len(_means) < 5:
            continue

        _means = _means.sort("date", descending=False)
        training_input = _means.tail(n_months_history)

        forecast_model = sm.tsa.SARIMAX(
            training_input["amount"].to_pandas(), order=order, trend=trend
        )
        fitted_model = forecast_model.fit()
        forecast = fitted_model.get_forecast(steps=n_predicted_months)
        forecast_mean = forecast.predicted_mean
        forecast_ci = forecast.conf_int(alpha=0.03)

        last_observed_date = cast(datetime.date, _means["date"].max())
        first_predicted_date = last_observed_date
        first_predicted_date += relativedelta(months=1)
        last_predicted_date = first_predicted_date + relativedelta(months=n_predicted_months - 1)
        last_observed_date, first_predicted_date, last_predicted_date

        _predictions = pl.DataFrame(
            {
                "name": name[0],
                "date": pl.date_range(
                    first_predicted_date, last_predicted_date, interval="1mo", eager=True
                ).dt.month_end(),
                "mean": pl.Series(forecast_mean),
                "lb": pl.Series(forecast_ci["lower amount"]),
                "ub": pl.Series(forecast_ci["upper amount"]),
            }
        )

        _res = _means.join(_predictions, on=["date", "name"], how="full", coalesce=True)

        prediction_dfs.append(_res)

    predictions = pl.concat(prediction_dfs, how="vertical")
    return predictions


def draw_raw_amounts(balances: pl.DataFrame, y_min: float = 0, y_max: float = 250_000) -> alt.Chart:
    """Build an Altair scatter chart of balances over time."""
    chart = (
        balances.plot.scatter(x="date", y="amount", color="name")
        .properties(width=600, height=400)
        .encode(
            y=alt.Y(
                "amount:Q",
                scale=alt.Scale(domain=[y_min, y_max]),
            )
        )
        .interactive()
    )
    return chart


def draw_predictions(
    balances: pl.DataFrame,
    y_min: float = 0,
    y_max: float = 250_000,
) -> alt.LayerChart:
    """Build an Altair balance chart with prediction intervals."""
    y_scale = alt.Scale(domain=[y_min, y_max])

    base: alt.Chart = alt.Chart(balances).encode(
        x=alt.X("date:T", title="Date"),
        color=alt.Color("name:N", title="Account", scale=alt.Scale(scheme="tableau10")),
    )

    prediction_interval = cast(alt.Chart, base.mark_area(opacity=0.15)).encode(
        y=alt.Y(
            "lb:Q",
            title="Amount",
            scale=y_scale,
        ),
        y2=alt.Y2("ub:Q"),
    )

    prediction = cast(alt.Chart, base.mark_line(strokeDash=[5, 5])).encode(
        y=alt.Y("mean:Q", scale=y_scale),
    )

    observations = cast(alt.Chart, base.mark_point()).encode(
        y=alt.Y("amount:Q", scale=y_scale),
    )

    return cast(
        alt.LayerChart,
        alt.layer(prediction_interval, prediction, observations)
        .properties(width=600, height=400)
        .interactive(),
    )
