import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from fintl.accounts_etl.schemas import BALANCE_SCHEMA, Provider, Sources
from fintl.cli.main import app

from .conftest import make_config


def _write_balances(target_dir: Path) -> None:
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


def _plot_config(tmp_path: Path):
    src = tmp_path / "sources" / "dkb" / "giro"
    src.mkdir(parents=True)
    config = make_config(tmp_path, Sources(dkb=Provider(giro=src)))
    _write_balances(config.target_dir)
    return config


def test_run_save_writes_html(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner
):
    config = _plot_config(tmp_path)
    monkeypatch.setattr("fintl.cli.plot.Config", lambda: config)
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.plot.webbrowser.open", mock_open)

    save_path = tmp_path / "chart.html"
    result = cli_runner.invoke(app, ["plot", "--save", str(save_path)])

    assert result.exit_code == 0, result.output
    assert save_path.exists()
    mock_open.assert_called_once()


def test_run_without_save_opens_browser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner
):
    config = _plot_config(tmp_path)
    monkeypatch.setattr("fintl.cli.plot.Config", lambda: config)
    mock_open = MagicMock()
    monkeypatch.setattr("fintl.cli.plot.webbrowser.open", mock_open)

    result = cli_runner.invoke(app, ["plot"])

    assert result.exit_code == 0, result.output
    mock_open.assert_called_once()
