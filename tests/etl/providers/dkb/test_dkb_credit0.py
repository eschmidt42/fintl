"""Tests for the DKB credit0 parser."""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from fintl.common import Config, Provider, Sources
from fintl.common.logging import Logging
from fintl.etl.common.exceptions import (
    ExtractBalanceError,
    ExtractTransactionsError,
)
from fintl.etl.io.files.filenames import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.etl.providers.dkb import credit0 as credit


@pytest.fixture
def csv_fname() -> str:
    """Return the DKB credit CSV fixture filename."""
    return "2022-03-15_to_2022-04-15_1234________5678.csv"


@pytest.fixture
def csv_file(files_root_path: Path, csv_fname: str) -> Path:
    """Return the path to the DKB credit CSV fixture file."""
    return files_root_path / "csv_files" / "DKB" / "credit" / csv_fname


def test_files_exist(files_root_path: Path, csv_file: Path):
    """Test that the required fixture files exist on disk."""
    assert files_root_path.exists()
    assert csv_file.exists()


def get_time(path: Path) -> float:
    """Return the modification time of the given path."""
    return path.stat().st_mtime


def test_main(tmp_path: Path, csv_file: Path):
    """Test that credit0.main parses files and skips already-processed ones."""
    credit_source_dir = csv_file.parent
    assert credit_source_dir.exists()

    logger_path = Path(__file__).parent.parent.parent.parent / "logger-config.json"
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(credit=credit_source_dir)),
        logging=Logging(config_file=logger_path),
    )

    # paths
    raw_dir = config.get_raw_dir(credit.CASE)
    file = Path(csv_file.name)
    copied_file_path = raw_dir / file

    parsed_dir = config.get_parsed_dir(credit.CASE)
    path_balance_json_single = parsed_dir / balance_csv_name_to_json(file)
    path_balance_parquet_single = parsed_dir / balance_csv_name_to_parquet(file)
    path_transactions_parquet_single = parsed_dir / transaction_csv_name_to_parquet(file)
    path_transactions_xlsx_single = parsed_dir / transaction_csv_name_to_xlsx(file)

    parser_dir = config.get_parser_dir(credit.CASE)
    path_balances_xlsx_parser = parser_dir / "balances.xlsx"
    path_balances_parquet_parser = parser_dir / "balances.parquet"
    path_transactions_parquet_parser = parser_dir / "transactions.parquet"
    path_transactions_xlsx_parser = parser_dir / "transactions.xlsx"

    # nothing should exist yet
    assert not path_balance_json_single.exists()
    assert not path_balance_parquet_single.exists()
    assert not path_transactions_parquet_single.exists()
    assert not path_transactions_xlsx_single.exists()

    assert not path_balances_xlsx_parser.exists()
    assert not path_balances_parquet_parser.exists()
    assert not path_transactions_parquet_parser.exists()
    assert not path_transactions_xlsx_parser.exists()

    # running the processing
    credit.main(config)

    # make sure the new raw file was copied as expected
    assert raw_dir.exists()
    assert copied_file_path.exists()

    # make sure the new raw fille was parsed as expected
    assert parsed_dir.exists()
    assert path_balance_json_single.exists()
    assert path_balance_parquet_single.exists()
    assert path_transactions_parquet_single.exists()
    assert path_transactions_xlsx_single.exists()

    assert path_balances_xlsx_parser.exists()
    assert path_balances_parquet_parser.exists()
    assert path_transactions_parquet_parser.exists()
    assert path_transactions_xlsx_parser.exists()

    t_raw = get_time(copied_file_path)
    t_balance_json_single = get_time(path_balance_json_single)
    t_balance_parquet_single = get_time(path_balance_parquet_single)
    t_transactions_parquet_single = get_time(path_transactions_parquet_single)
    t_transactions_xlsx_single = get_time(path_transactions_xlsx_single)

    n_balances = len(pl.read_parquet(path_balances_parquet_parser))
    n_transactions = len(pl.read_parquet(path_transactions_parquet_parser))

    # running the process again ensuring nothing happens because all files are already present
    credit.main(config)

    assert t_raw == get_time(copied_file_path)
    assert t_balance_json_single == get_time(path_balance_json_single)
    assert t_balance_parquet_single == get_time(path_balance_parquet_single)
    assert t_transactions_parquet_single == get_time(path_transactions_parquet_single)
    assert t_transactions_xlsx_single == get_time(path_transactions_xlsx_single)

    n_balances_new = len(pl.read_parquet(path_balances_parquet_parser))
    n_transactions_new = len(pl.read_parquet(path_transactions_parquet_parser))

    assert n_balances == n_balances_new
    assert n_transactions == n_transactions_new

    # running the process again ensuring only parsed files are created that are missing
    path_balance_json_single.unlink()
    path_balance_parquet_single.unlink()
    path_transactions_parquet_single.unlink()
    path_transactions_xlsx_single.unlink()

    credit.main(config)

    assert t_raw == get_time(copied_file_path)
    assert t_balance_json_single < get_time(path_balance_json_single)
    assert t_balance_parquet_single < get_time(path_balance_parquet_single)
    assert t_transactions_parquet_single < get_time(path_transactions_parquet_single)
    assert t_transactions_xlsx_single < get_time(path_transactions_xlsx_single)


def test_parse_csv_file_raises_extract_transactions_exception(csv_file: Path):
    """Test that parse_csv_file raises ExtractTransactionsException on bad transactions."""
    with patch(
        "fintl.etl.providers.dkb.credit0.extract_transactions",
        side_effect=ValueError("malformed transactions"),
    ):
        with pytest.raises(ExtractTransactionsError) as exc_info:
            credit.parse_csv_file(credit.CASE, csv_file)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_parse_csv_file_raises_extract_balance_exception(csv_file: Path):
    """Test that parse_csv_file raises ExtractBalanceException on bad balance data."""
    with patch(
        "fintl.etl.providers.dkb.credit0.extract_balance",
        side_effect=ValueError("malformed balance"),
    ):
        with pytest.raises(ExtractBalanceError) as exc_info:
            credit.parse_csv_file(credit.CASE, csv_file)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_parse_new_files_skips_failing_file_and_continues(tmp_path: Path):
    """Test that parse_new_files skips a failing file and processes the remaining ones."""
    good_file = tmp_path / "good.csv"
    bad_file = tmp_path / "bad.csv"
    good_file.touch()
    bad_file.touch()

    parsed_dir = tmp_path / "parsed"
    good_transactions = pl.DataFrame()
    good_balance = object()

    def _parse_csv_file(case, file_path):
        if file_path == bad_file:
            raise ExtractTransactionsError("bad file")
        return good_transactions, good_balance

    with (
        patch(
            "fintl.etl.providers.dkb.credit0.parse_csv_file",
            side_effect=_parse_csv_file,
        ),
        patch("fintl.etl.engine.parse_utils.store_transactions") as mock_store_t,
        patch("fintl.etl.engine.parse_utils.store_balance") as mock_store_b,
    ):
        credit.parse_new_files(credit.CASE, [bad_file, good_file], parsed_dir)

    mock_store_t.assert_called_once_with(parsed_dir, good_file, good_transactions)
    mock_store_b.assert_called_once_with(parsed_dir, good_file, good_balance)
