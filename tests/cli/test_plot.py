"""Tests for the fintl plot CLI command."""

import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from typer.testing import CliRunner

from fintl.cli.commands.plot.helper import draw_plot
from fintl.cli.main import app
from fintl.common import Provider, Sources
from fintl.etl.common.schemas import BALANCE_SCHEMA

from .conftest import make_config


def _write_balances(target_dir: Path) -> None:
    """Write a minimal all-balances.parquet fixture to target_dir."""
    df = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            "amount": [1000.0, 1100.0],
            "currency": ["EUR", "EUR"],
            "provider": ["dkb", "dkb"],
            "service": ["giro", "giro"],
            "parser": ["giro0", "giro0"],
            "file": ["a.csv", "b.csv"],
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
    mock_open.assert_called_once()


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
    mock_open.assert_called_once()


def test_draw_plot_uses_default_y_axis_bounds() -> None:
    """Test that the chart uses the default y-axis bounds."""
    config = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 1)],
            "amount": [1000.0],
            "name": ["dkb giro"],
        }
    )

    y_scale = draw_plot(config).to_dict()["encoding"]["y"]["scale"]

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
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.commands.plot.helper.webbrowser.open", mock_open)

    save_path = tmp_path / "chart.html"
    result = cli_runner.invoke(
        app,
        ["plot", "--save", str(save_path), "--y-min", "-100.5", "--y-max", "5000.25"],
    )

    assert result.exit_code == 0, result.output
    assert '"domain": [-100.5, 5000.25]' in save_path.read_text()


def test_run_rejects_invalid_y_axis_bounds(cli_runner: CliRunner) -> None:
    """Test that the lower y-axis bound must be less than the upper bound."""
    result = cli_runner.invoke(app, ["plot", "--y-min", "100", "--y-max", "100"])

    assert result.exit_code == 2
    assert "--y-min must be less than --y-max" in result.output
