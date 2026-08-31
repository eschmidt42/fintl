"""Calculation methods to be used in the plot command."""

import datetime
from typing import cast

import polars as pl
import statsmodels.api as sm
from dateutil.relativedelta import relativedelta


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

    predictions = pl.concat(prediction_dfs, how="vertical").sort("name", "date")
    return predictions
