"""Functions to draw altair plots for the plot command."""

from typing import cast

import altair as alt
import polars as pl


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
