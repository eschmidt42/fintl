import logging
from pathlib import Path

import polars as pl
import pytest

from fintl.common import Case, Config, Provider, Sources
from fintl.common.logging import Logging
from fintl.etl.io.parquets import concatenate_parquets


def _config(tmp_path: Path, logger_config_path: Path) -> Config:

    src_dir = tmp_path / "src" / "dkb" / "giro"
    src_dir.mkdir(parents=True, exist_ok=True)

    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True, exist_ok=True)

    return Config(
        target_dir=target_dir,
        sources=Sources(dkb=Provider(giro=src_dir)),
        logging=Logging(config_file=logger_config_path),
    )


def _create_empty_transactions_parquet(config: Config, case: Case):
    parser_dir = config.get_parser_dir(case)
    parser_dir.mkdir(parents=True, exist_ok=True)

    pl.DataFrame({"amount": pl.Series([], dtype=pl.Float64)}).write_parquet(
        parser_dir / "transactions.parquet"
    )


def test_concatenate_parquets_warns_on_empty_parquet(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, logger_config_path: Path
):
    config = _config(tmp_path, logger_config_path)

    case = Case(provider="dkb", service="giro", parser="giro0")

    _create_empty_transactions_parquet(config, case)

    with caplog.at_level(logging.WARNING, logger="fintl.etl.io.parquets"):
        result = concatenate_parquets("transactions.parquet", config, [case], ["amount"])

    assert result is None
    assert any("n_rows=0" in m for m in caplog.messages)


def test_concatenate_parquets_no_warning_on_empty_scalable_broker(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, logger_config_path: Path
):
    config = _config(tmp_path, logger_config_path)

    case = Case(provider="scalable", service="broker", parser="broker0")

    _create_empty_transactions_parquet(config, case)

    with caplog.at_level(logging.WARNING, logger="fintl.etl.io.parquets"):
        result = concatenate_parquets("transactions.parquet", config, [case], ["amount"])

    assert result is None
    assert not any("n_rows=0" in m for m in caplog.messages)
