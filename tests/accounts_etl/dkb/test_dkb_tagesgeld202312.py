import datetime
from pathlib import Path

import polars as pl

from fintl.accounts_etl.dkb import tagesgeld202312 as tagesgeld
from fintl.accounts_etl.dkb.files import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources


def test_extract_balance_with_xa0_character(tmp_path: Path):
    """Regression test: extract_balance must handle \xa0 (non-breaking space) in the total field."""
    # \xa0 between amount and currency, as produced by some DKB exports
    lines = [
        '""',
        '"Kontostand vom 02.12.2023:";"1.123,45\xa0EUR"',
        '""',
    ]
    result = tagesgeld.extract_balance(tagesgeld.CASE, tmp_path / "dummy.csv", lines)

    assert result.date == datetime.date(2023, 12, 2)
    assert result.amount == 1123.45
    assert result.currency == "EUR"


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
    files = [
        Path("02-12-2023_Umsatzliste_Tagesgeld_DE01234567890123456789.csv"),
        Path("24-02-2024_Umsatzliste_Tagesgeld_DE01234567890123456789.csv"),
    ]
    copied_file_paths = [raw_dir / f for f in files]

    parsed_dir = config.get_parsed_dir(tagesgeld.CASE)
    paths_balance_json_single = [
        parsed_dir / balance_csv_name_to_json(f) for f in files
    ]
    paths_balance_parquet_single = [
        parsed_dir / balance_csv_name_to_parquet(f) for f in files
    ]
    paths_transactions_parquet_single = [
        parsed_dir / transaction_csv_name_to_parquet(f) for f in files
    ]
    paths_transactions_xlsx_single = [
        parsed_dir / transaction_csv_name_to_xlsx(f) for f in files
    ]

    parser_dir = config.get_parser_dir(tagesgeld.CASE)
    path_balances_xlsx_parser = parser_dir / "balances.xlsx"
    path_balances_parquet_parser = parser_dir / "balances.parquet"
    path_transactions_parquet_parser = parser_dir / "transactions.parquet"
    path_transactions_xlsx_parser = parser_dir / "transactions.xlsx"

    # nothing should exist yet
    for i in range(len(files)):
        assert not paths_balance_json_single[i].exists()
        assert not paths_balance_parquet_single[i].exists()
        assert not paths_transactions_parquet_single[i].exists()
        assert not paths_transactions_xlsx_single[i].exists()

    assert not path_balances_xlsx_parser.exists()
    assert not path_balances_parquet_parser.exists()
    assert not path_transactions_parquet_parser.exists()
    assert not path_transactions_xlsx_parser.exists()

    # running the processing
    tagesgeld.main(config)

    # make sure the new raw file was copied as expected
    assert raw_dir.exists()
    for f in copied_file_paths:
        assert f.exists()

    # make sure the new raw fille was parsed as expected
    assert parsed_dir.exists()
    for i in range(len(files)):
        assert paths_balance_json_single[i].exists()
        assert paths_balance_parquet_single[i].exists()
        assert paths_transactions_parquet_single[i].exists()
        assert paths_transactions_xlsx_single[i].exists()

    assert path_balances_xlsx_parser.exists()
    assert path_balances_parquet_parser.exists()
    assert path_transactions_parquet_parser.exists()
    assert path_transactions_xlsx_parser.exists()

    ts_raw = [get_time(f) for f in copied_file_paths]
    ts_balance_json_single = [get_time(f) for f in paths_balance_json_single]
    ts_balance_parquet_single = [get_time(f) for f in paths_balance_parquet_single]
    ts_transactions_parquet_single = [
        get_time(f) for f in paths_transactions_parquet_single
    ]
    ts_transactions_xlsx_single = [get_time(f) for f in paths_transactions_xlsx_single]

    n_balances = len(pl.read_parquet(path_balances_parquet_parser))
    n_transactions = len(pl.read_parquet(path_transactions_parquet_parser))

    # running the process again ensuring nothing happens because all files are already present
    tagesgeld.main(config)

    for i, (
        f_raw,
        f_balance_json,
        f_balance_parquet,
        f_trans_parquet,
        f_trans_xlsx,
    ) in enumerate(
        zip(
            copied_file_paths,
            paths_balance_json_single,
            paths_balance_parquet_single,
            paths_transactions_parquet_single,
            paths_transactions_xlsx_single,
            strict=True,
        )
    ):
        assert ts_raw[i] == get_time(f_raw)
        assert ts_balance_json_single[i] == get_time(f_balance_json)
        assert ts_balance_parquet_single[i] == get_time(f_balance_parquet)
        assert ts_transactions_parquet_single[i] == get_time(f_trans_parquet)
        assert ts_transactions_xlsx_single[i] == get_time(f_trans_xlsx)

    n_balances_new = len(pl.read_parquet(path_balances_parquet_parser))
    n_transactions_new = len(pl.read_parquet(path_transactions_parquet_parser))

    assert n_balances == n_balances_new
    assert n_transactions == n_transactions_new

    # running the process again ensuring only parsed files are created that are missing
    [f.unlink() for f in paths_balance_json_single]
    [f.unlink() for f in paths_balance_parquet_single]
    [f.unlink() for f in paths_transactions_parquet_single]
    [f.unlink() for f in paths_transactions_xlsx_single]

    tagesgeld.main(config)

    for i, (
        f_raw,
        f_balance_json,
        f_balance_parquet,
        f_trans_parquet,
        f_trans_xlsx,
    ) in enumerate(
        zip(
            copied_file_paths,
            paths_balance_json_single,
            paths_balance_parquet_single,
            paths_transactions_parquet_single,
            paths_transactions_xlsx_single,
            strict=True,
        )
    ):
        assert ts_raw[i] == get_time(f_raw)
        assert ts_balance_json_single[i] < get_time(f_balance_json)
        assert ts_balance_parquet_single[i] < get_time(f_balance_parquet)
        assert ts_transactions_parquet_single[i] < get_time(f_trans_parquet)
        assert ts_transactions_xlsx_single[i] < get_time(f_trans_xlsx)
