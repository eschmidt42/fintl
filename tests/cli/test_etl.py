from pathlib import Path

import polars as pl
import pytest

from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources
from fintl.cli.main import app

from .conftest import _LOGGER_PATH

_FILES = Path(__file__).parent.parent / "accounts_etl" / "files"
_CSV = _FILES / "csv_files"
_HTML = _FILES / "html_files"


def _all_provider_config(tmp_path: Path) -> Config:
    target = tmp_path / "target"
    target.mkdir(parents=True, exist_ok=True)
    return Config(
        target_dir=target,
        sources=Sources(
            dkb=Provider(
                giro=_CSV / "DKB" / "kontoauszug",
                tagesgeld=_CSV / "DKB" / "tagesgeld",
                credit=_CSV / "DKB" / "credit",
            ),
            postbank=Provider(giro=_CSV / "Postbank"),
            scalable=Provider(broker=_HTML / "Scalable-Capital"),
            gls=Provider(
                giro=_CSV / "GLS" / "giro",
                credit=_CSV / "GLS" / "credit",
            ),
        ),
        logging=Logging(config_file=_LOGGER_PATH),
    )


def test_run_exits_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner):
    config = _all_provider_config(tmp_path)
    monkeypatch.setattr("fintl.cli.etl.Config", lambda: config)
    result = cli_runner.invoke(app, ["etl"])
    assert result.exit_code == 0, result.output


def _provider_services(path: Path) -> set[tuple[str, str]]:
    df = pl.read_parquet(path)
    return set(df.select(["provider", "service"]).rows())


def test_run_writes_parquet_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner
):
    config = _all_provider_config(tmp_path)
    monkeypatch.setattr("fintl.cli.etl.Config", lambda: config)
    cli_runner.invoke(app, ["etl"])

    tx_path = config.target_dir / "all-transactions.parquet"
    bal_path = config.target_dir / "all-balances.parquet"
    assert tx_path.exists()
    assert bal_path.exists()

    # scalable broker produces only balances, not transactions
    expected_tx_provider_services = {
        ("dkb", "giro"),
        ("dkb", "tagesgeld"),
        ("dkb", "credit"),
        ("postbank", "giro"),
        ("gls", "giro"),
        ("gls", "credit"),
    }
    expected_bal_provider_services = expected_tx_provider_services | {
        ("scalable", "broker"),
    }
    assert _provider_services(tx_path) == expected_tx_provider_services
    assert _provider_services(bal_path) == expected_bal_provider_services
