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
    resolve_html_path,
    save_chart,
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


def test_resolve_html_path_uses_save_directory(tmp_path: Path) -> None:
    """Test that an explicit save directory produces the named output path."""
    save_path = tmp_path / "chart.html"

    assert resolve_html_path(tmp_path, "chart.html") == save_path


def test_resolve_html_path_uses_temporary_html_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that no save directory produces a temporary HTML path."""
    temporary_file = MagicMock()
    temporary_file.name = str(tmp_path / "temporary.html")
    temporary_file.__enter__.return_value = temporary_file
    named_temporary_file = MagicMock(return_value=temporary_file)
    monkeypatch.setattr(helper.tempfile, "NamedTemporaryFile", named_temporary_file)

    output_path = resolve_html_path(None, "chart.html")

    assert output_path == Path(temporary_file.name)
    named_temporary_file.assert_called_once_with(suffix=".html", delete=False)


def test_save_chart_saves_chart_and_reports_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test that chart saving delegates to Altair and reports the destination."""
    chart = MagicMock()
    save_path = tmp_path / "chart.html"

    save_chart(chart, save_path)

    chart.save.assert_called_once_with(str(save_path))
    assert capsys.readouterr().out == f"Chart saved to {save_path}\n"


def test_display_plot_opens_existing_html_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that display_plot opens the resolved output path."""
    save_path = tmp_path / "chart.html"
    open_browser = MagicMock()
    monkeypatch.setattr(helper.webbrowser, "open", open_browser)

    display_plot(save_path)

    open_browser.assert_called_once_with(save_path.resolve().as_uri())


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


def test_run_save_dir_writes_named_html_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    logger_config_path: Path,
):
    """Test that fintl plot --save-dir writes all named HTML files."""
    config = _plot_config(tmp_path, logger_config_path)
    monkeypatch.setattr("fintl.cli.commands.plot.core.Config", lambda: config)
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.helper.webbrowser.open", mock_open)

    save_dir = tmp_path / "charts" / "nested"
    result = cli_runner.invoke(app, ["plot", "--save-dir", str(save_dir)])

    assert result.exit_code == 0, result.output
    assert {path.name for path in save_dir.iterdir()} == {
        "balances.html",
        "monthly-deltas.html",
        "predictions.html",
    }
    assert mock_open.call_count == 3


def test_run_quiet_suppresses_browser_opening(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    logger_config_path: Path,
) -> None:
    """Test that quiet mode saves charts without opening the browser."""
    config = _plot_config(tmp_path, logger_config_path)
    monkeypatch.setattr("fintl.cli.commands.plot.core.Config", lambda: config)
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.helper.webbrowser.open", mock_open)

    save_dir = tmp_path / "charts"
    result = cli_runner.invoke(app, ["plot", "--save-dir", str(save_dir), "--quiet"])

    assert result.exit_code == 0, result.output
    assert {path.name for path in save_dir.iterdir()} == {
        "balances.html",
        "monthly-deltas.html",
        "predictions.html",
    }
    mock_open.assert_not_called()


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


def test_run_quiet_without_save_does_not_open_browser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    logger_config_path: Path,
) -> None:
    """Test that quiet mode also suppresses temporary browser charts."""
    config = _plot_config(tmp_path, logger_config_path)
    monkeypatch.setattr("fintl.cli.commands.plot.core.Config", lambda: config)
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.helper.webbrowser.open", mock_open)

    result = cli_runner.invoke(app, ["plot", "--quiet"])

    assert result.exit_code == 0, result.output
    mock_open.assert_not_called()


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
    save_mock = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.core.save_chart", save_mock)
    monkeypatch.setattr("fintl.cli.commands.plot.core.display_plot", MagicMock())

    save_dir = tmp_path / "charts"
    result = cli_runner.invoke(
        app,
        ["plot", "--save-dir", str(save_dir), "--y-min", "-100.5", "--y-max", "5000.25"],
    )

    assert result.exit_code == 0, result.output
    assert save_mock.call_count == 3
    first_chart = save_mock.call_args_list[0].args[0]
    assert first_chart.to_dict()["encoding"]["y"]["scale"]["domain"] == [-100.5, 5000.25]


def test_run_rejects_invalid_y_axis_bounds(cli_runner: CliRunner) -> None:
    """Test that the lower y-axis bound must be less than the upper bound."""
    result = cli_runner.invoke(app, ["plot", "--y-min", "100", "--y-max", "100"])

    assert result.exit_code == 2
