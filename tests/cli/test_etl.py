from pathlib import Path

import polars as pl
import pytest

from fintl.cli.main import app

from .conftest import _LOGGER_PATH

_FILES = Path(__file__).parent.parent / "accounts_etl" / "files"
_CSV = _FILES / "csv_files"
_HTML = _FILES / "html_files"


def _write_config_toml(tmp_path: Path) -> Path:
    target = tmp_path / "target"
    target.mkdir(parents=True, exist_ok=True)
    toml_path = tmp_path / "fintl.toml"
    toml_path.write_text(f"""\
target_dir = "{target}"

[sources.dkb]
giro      = "{_CSV / "DKB" / "kontoauszug"}"
tagesgeld = "{_CSV / "DKB" / "tagesgeld"}"
credit    = "{_CSV / "DKB" / "credit"}"

[sources.postbank]
giro = "{_CSV / "Postbank"}"

[sources.scalable]
broker = "{_HTML / "Scalable-Capital"}"

[sources.gls]
giro   = "{_CSV / "GLS" / "giro"}"
credit = "{_CSV / "GLS" / "credit"}"

[logging]
config_file = "{_LOGGER_PATH}"
""")
    return toml_path


def _provider_services(path: Path) -> set[tuple[str, str]]:
    df = pl.read_parquet(path)
    return set(df.select(["provider", "service"]).rows())


def test_run_exits_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner):
    toml_path = _write_config_toml(tmp_path)
    monkeypatch.setenv("FINTL_CONFIG", str(toml_path))
    result = cli_runner.invoke(app, ["etl"])
    assert result.exit_code == 0, result.output


def test_run_writes_parquet_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner
):
    toml_path = _write_config_toml(tmp_path)
    monkeypatch.setenv("FINTL_CONFIG", str(toml_path))
    cli_runner.invoke(app, ["etl"])

    target = tmp_path / "target"
    tx_path = target / "all-transactions.parquet"
    bal_path = target / "all-balances.parquet"
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
