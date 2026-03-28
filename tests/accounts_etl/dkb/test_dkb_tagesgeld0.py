from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from fintl.accounts_etl.dkb import tagesgeld0 as tagesgeld
from fintl.accounts_etl.dkb.files import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.accounts_etl.exceptions import (
    ExtractBalanceException,
    ExtractTransactionsException,
)
from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources

_FIXTURE_CSV = (
    Path(__file__).parent.parent
    / "files"
    / "csv_files"
    / "DKB"
    / "tagesgeld"
    / "0123456789_2023-04-16.csv"
)


def get_time(path: Path) -> float:
    return path.stat().st_mtime


def test_main(tmp_path: Path):
    tagesgeld_source_dir = (
        Path(__file__).parent.parent / "files" / "csv_files" / "DKB" / "tagesgeld"
    )
    assert tagesgeld_source_dir.exists()

    logger_path = (
        Path(__file__).parent.parent.parent / "fine_logging" / "logger-config.json"
    )
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(tagesgeld=tagesgeld_source_dir)),
        logging=Logging(config_file=logger_path),
    )

    # paths
    raw_dir = config.get_raw_dir(tagesgeld.CASE)
    file = Path("0123456789_2023-04-16.csv")
    copied_file_path = raw_dir / file

    parsed_dir = config.get_parsed_dir(tagesgeld.CASE)
    path_balance_json_single = parsed_dir / balance_csv_name_to_json(file)
    path_balance_parquet_single = parsed_dir / balance_csv_name_to_parquet(file)
    path_transactions_parquet_single = parsed_dir / transaction_csv_name_to_parquet(
        file
    )
    path_transactions_xlsx_single = parsed_dir / transaction_csv_name_to_xlsx(file)

    parser_dir = config.get_parser_dir(tagesgeld.CASE)
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
    tagesgeld.main(config)

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
    tagesgeld.main(config)

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

    tagesgeld.main(config)

    assert t_raw == get_time(copied_file_path)
    assert t_balance_json_single < get_time(path_balance_json_single)
    assert t_balance_parquet_single < get_time(path_balance_parquet_single)
    assert t_transactions_parquet_single < get_time(path_transactions_parquet_single)
    assert t_transactions_xlsx_single < get_time(path_transactions_xlsx_single)


def test_extract_transactions_returns_empty_dataframe_when_no_data_rows(tmp_path: Path):
    """When the header line is the last line (no transaction rows), extract_transactions
    must return an empty DataFrame rather than raising."""
    from fintl.accounts_etl.dkb.tagesgeld0 import CASE, extract_transactions

    # The header pattern is '^("?Buchungstag";"Wertstellung")'
    lines = [
        '"Buchungstag";"Wertstellung";"Buchungstext";"Auftraggeber / Begünstigter";"Verwendungszweck";"Kontonummer";"BLZ";"Betrag (EUR)";"Gläubiger-ID";"Mandatsreferenz";"Kundenreferenz"\n',
    ]
    file_path = tmp_path / "empty.csv"
    file_path.write_text("".join(lines))

    df = extract_transactions(CASE, file_path, lines, "utf-8")

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


def test_parse_csv_file_raises_extract_transactions_exception():
    with patch(
        "fintl.accounts_etl.dkb.tagesgeld0.extract_transactions",
        side_effect=ValueError("malformed transactions"),
    ):
        with pytest.raises(ExtractTransactionsException) as exc_info:
            tagesgeld.parse_csv_file(tagesgeld.CASE, _FIXTURE_CSV)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_parse_csv_file_raises_extract_balance_exception():
    with patch(
        "fintl.accounts_etl.dkb.tagesgeld0.extract_balance",
        side_effect=ValueError("malformed balance"),
    ):
        with pytest.raises(ExtractBalanceException) as exc_info:
            tagesgeld.parse_csv_file(tagesgeld.CASE, _FIXTURE_CSV)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_parse_new_files_skips_failing_file_and_continues(tmp_path: Path):
    good_file = tmp_path / "good.csv"
    bad_file = tmp_path / "bad.csv"
    good_file.touch()
    bad_file.touch()

    parsed_dir = tmp_path / "parsed"
    good_transactions = pl.DataFrame()
    good_balance = object()

    def _parse_csv_file(case, file_path):
        if file_path == bad_file:
            raise ExtractTransactionsException("bad file")
        return good_transactions, good_balance

    with (
        patch(
            "fintl.accounts_etl.dkb.tagesgeld0.parse_csv_file",
            side_effect=_parse_csv_file,
        ),
        patch("fintl.accounts_etl.dkb.tagesgeld0.store_transactions") as mock_store_t,
        patch("fintl.accounts_etl.dkb.tagesgeld0.store_balance") as mock_store_b,
    ):
        tagesgeld.parse_new_files(tagesgeld.CASE, [bad_file, good_file], parsed_dir)

    mock_store_t.assert_called_once_with(parsed_dir, good_file, good_transactions)
    mock_store_b.assert_called_once_with(parsed_dir, good_file, good_balance)
