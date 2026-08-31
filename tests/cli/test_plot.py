"""Tests for the fintl plot CLI command."""

import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from typer.testing import CliRunner

from fintl.cli.commands.plot import calc, helper
from fintl.cli.commands.plot.calc import calc_month_means, calc_predictions
from fintl.cli.commands.plot.draw import draw_predictions, draw_raw_amounts
from fintl.cli.commands.plot.helper import (
    display_plot,
    load_data,
)
from fintl.cli.main import app
from fintl.common import Provider, Sources
from fintl.etl.common.schemas import BALANCE_SCHEMA

from .conftest import make_config


def _write_balances(target_dir: Path) -> None:
    """Write a minimal all-balances.parquet fixture to target_dir."""
    df = pl.DataFrame(
        {
            "date": [datetime.date(2024, month, 1) for month in range(1, 7)],
            "amount": [1000.0 + month * 100 for month in range(6)],
            "currency": ["EUR"] * 6,
            "provider": ["dkb"] * 6,
            "service": ["giro"] * 6,
            "parser": ["giro0"] * 6,
            "file": [f"{month}.csv" for month in range(1, 7)],
        },
        schema=BALANCE_SCHEMA,
    )
    df.write_parquet(target_dir / "all-balances.parquet")


def _plot_config(tmp_path: Path, logger_config_path: Path):
    """Return a Config with a pre-written balances parquet for plot tests."""
    src = tmp_path / "sources" / "dkb" / "giro"
    src.mkdir(parents=True)
    config = make_config(tmp_path, Sources(dkb=Provider(giro=src)), logger_config_path)
    _write_balances(config.target_dir)
    return config


def test_load_data_adds_normalized_account_name(tmp_path: Path, logger_config_path: Path) -> None:
    """Test that loaded balances include a normalized provider/service name."""
    config = _plot_config(tmp_path, logger_config_path)

    balances = load_data(config)

    assert balances["name"].to_list() == ["dkb giro"] * 6


def test_calc_month_means_uses_last_balance_and_calculates_deltas() -> None:
    """Test monthly aggregation and per-account month-over-month changes."""
    balances = pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 31),
                datetime.date(2024, 2, 1),
                datetime.date(2024, 2, 29),
                datetime.date(2024, 1, 15),
            ],
            "amount": [100.0, 125.0, 130.0, 150.0, 200.0],
            "name": ["checking", "checking", "checking", "checking", "savings"],
        }
    )

    result = calc_month_means(balances)

    expected = pl.DataFrame(
        {
            "name": ["checking", "checking", "savings"],
            "date": [
                datetime.date(2024, 1, 31),
                datetime.date(2024, 2, 29),
                datetime.date(2024, 1, 31),
            ],
            "amount": [125.0, 150.0, 200.0],
            "delta": [None, 25.0, None],
        },
        schema={"name": pl.String, "date": pl.Date, "amount": pl.Float64, "delta": pl.Float64},
    )
    assert result.equals(expected)


def test_calc_predictions_skips_short_histories_and_builds_forecast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test prediction filtering, model arguments, and forecast output."""
    month_means = pl.DataFrame(
        {
            "name": ["checking"] * 6 + ["savings"] * 4,
            "date": [datetime.date(2024, month, 1) for month in range(1, 7)]
            + [datetime.date(2024, month, 1) for month in range(1, 5)],
            "amount": [100.0 + month * 10 for month in range(6)]
            + [200.0 + month * 10 for month in range(4)],
        }
    )
    forecast = MagicMock(
        predicted_mean=[160.0, 170.0],
        conf_int=MagicMock(
            return_value={
                "lower amount": [155.0, 165.0],
                "upper amount": [165.0, 175.0],
            }
        ),
    )
    fitted_model = MagicMock(get_forecast=MagicMock(return_value=forecast))
    sarimax = MagicMock(return_value=MagicMock(fit=MagicMock(return_value=fitted_model)))
    monkeypatch.setattr(calc.sm.tsa, "SARIMAX", sarimax)

    result = calc_predictions(
        month_means,
        n_predicted_months=2,
        n_months_history=5,
        order=(0, 1, 1),
        trend="c",
    )

    (model_input,) = sarimax.call_args.args
    assert model_input.to_list() == [110.0, 120.0, 130.0, 140.0, 150.0]
    assert sarimax.call_args.kwargs == {"order": (0, 1, 1), "trend": "c"}
    assert result["name"].unique().to_list() == ["checking"]
    assert result.filter(pl.col("mean").is_not_null()).sort("date")["date"].to_list() == [
        datetime.date(2024, 7, 31),
        datetime.date(2024, 8, 31),
    ]
    assert result.filter(pl.col("mean").is_not_null())["mean"].to_list() == [160.0, 170.0]


def test_display_plot_saves_and_opens_existing_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test saving a chart opens the resolved output path."""
    chart = MagicMock()
    save_path = tmp_path / "chart.html"
    open_browser = MagicMock()
    monkeypatch.setattr(helper.webbrowser, "open", open_browser)

    display_plot(save_path, chart)

    chart.save.assert_called_once_with(str(save_path))
    open_browser.assert_called_once_with(save_path.resolve().as_uri())


def test_display_plot_uses_temporary_path_when_not_saving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test opening a chart without a save path uses a temporary HTML file."""
    chart = MagicMock()
    temporary_file = MagicMock(name=str(tmp_path / "temporary.html"))
    temporary_file.name = str(tmp_path / "temporary.html")
    temporary_file.__enter__.return_value = temporary_file
    open_browser = MagicMock()
    monkeypatch.setattr(
        helper.tempfile,
        "NamedTemporaryFile",
        MagicMock(return_value=temporary_file),
    )
    monkeypatch.setattr(helper.webbrowser, "open", open_browser)

    display_plot(None, chart)

    chart.save.assert_called_once_with(temporary_file.name)
    open_browser.assert_called_once_with(Path(temporary_file.name).resolve().as_uri())


def test_draw_predictions_has_three_layers_and_axis_bounds() -> None:
    """Test prediction chart layers and configured y-axis bounds."""
    balances = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 31)],
            "name": ["checking"],
            "amount": [100.0],
            "mean": [110.0],
            "lb": [90.0],
            "ub": [130.0],
        }
    )

    chart = draw_predictions(balances, y_min=-100.0, y_max=500.0)
    chart_dict = chart.to_dict()

    assert len(chart_dict["layer"]) == 3
    assert chart_dict["layer"][0]["encoding"]["y"]["scale"]["domain"] == [-100.0, 500.0]


def test_run_save_writes_html(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    logger_config_path: Path,
):
    """Test that fintl plot --save writes an HTML file and opens the browser."""
    config = _plot_config(tmp_path, logger_config_path)
    monkeypatch.setattr("fintl.cli.commands.plot.core.Config", lambda: config)
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.helper.webbrowser.open", mock_open)

    save_path = tmp_path / "chart.html"
    result = cli_runner.invoke(app, ["plot", "--save", str(save_path)])

    assert result.exit_code == 0, result.output
    assert save_path.exists()
    assert mock_open.call_count == 3


def test_run_without_save_opens_browser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    logger_config_path: Path,
):
    """Test that fintl plot without --save opens the browser directly."""
    config = _plot_config(tmp_path, logger_config_path)
    monkeypatch.setattr("fintl.cli.commands.plot.core.Config", lambda: config)
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.helper.webbrowser.open", mock_open)

    result = cli_runner.invoke(app, ["plot"])

    assert result.exit_code == 0, result.output
    assert mock_open.call_count == 3


def test_draw_plot_uses_default_y_axis_bounds() -> None:
    """Test that the chart uses the default y-axis bounds."""
    config = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 1)],
            "amount": [1000.0],
            "name": ["dkb giro"],
        }
    )

    y_scale = draw_raw_amounts(config).to_dict()["encoding"]["y"]["scale"]

    assert y_scale["domain"] == [0, 250_000]


def test_run_accepts_custom_y_axis_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    logger_config_path: Path,
) -> None:
    """Test that custom y-axis bounds are passed to the chart."""
    config = _plot_config(tmp_path, logger_config_path)
    monkeypatch.setattr("fintl.cli.commands.plot.core.Config", lambda: config)
    display_mock = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.core.display_plot", display_mock)

    save_path = tmp_path / "chart.html"
    result = cli_runner.invoke(
        app,
        ["plot", "--save", str(save_path), "--y-min", "-100.5", "--y-max", "5000.25"],
    )

    assert result.exit_code == 0, result.output
    assert display_mock.call_count == 3
    first_chart = display_mock.call_args_list[0].args[1]
    assert first_chart.to_dict()["encoding"]["y"]["scale"]["domain"] == [-100.5, 5000.25]


def test_run_rejects_invalid_y_axis_bounds(cli_runner: CliRunner) -> None:
    """Test that the lower y-axis bound must be less than the upper bound."""
    result = cli_runner.invoke(app, ["plot", "--y-min", "100", "--y-max", "100"])

    assert result.exit_code == 2
