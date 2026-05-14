from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from fintl.cli.main import app


def _write_config_toml(tmp_path: Path, files_root_path: Path, logger_path: Path) -> Path:
    target = tmp_path / "target"
    target.mkdir(parents=True, exist_ok=True)
    toml_path = tmp_path / "fintl.toml"

    csv_dir = files_root_path / "csv_files"
    assert csv_dir.exists()

    artefacts_dir = files_root_path / "artefacts"
    assert artefacts_dir.exists()

    toml_path.write_text(f"""\
target_dir = "{target}"

[sources.dkb]
giro      = "{csv_dir / "DKB" / "kontoauszug"}"
tagesgeld = "{csv_dir / "DKB" / "tagesgeld"}"
credit    = "{csv_dir / "DKB" / "credit"}"

[sources.postbank]
giro = "{csv_dir / "Postbank"}"

[sources.scalable]
broker = "{artefacts_dir / "Scalable-Capital"}"

[sources.gls]
giro   = "{csv_dir / "GLS" / "giro"}"
credit = "{csv_dir / "GLS" / "credit"}"

[logging]
config_file = "{logger_path}"
""")
    return toml_path


def _provider_services(path: Path) -> set[tuple[str, str]]:
    df = pl.read_parquet(path)
    return set(df.select(["provider", "service"]).rows())


def test_run_exits_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    files_root_path: Path,
    logger_config_path: Path,
):
    toml_path = _write_config_toml(tmp_path, files_root_path, logger_config_path)
    monkeypatch.setenv("FINTL_CONFIG", str(toml_path))
    result = cli_runner.invoke(app, ["etl"])
    assert result.exit_code == 0, result.output


def test_run_writes_parquet_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
    files_root_path: Path,
    logger_config_path: Path,
):
    toml_path = _write_config_toml(tmp_path, files_root_path, logger_config_path)
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
