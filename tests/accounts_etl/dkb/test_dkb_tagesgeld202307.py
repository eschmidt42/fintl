from pathlib import Path

import polars as pl

from fintl.accounts_etl.dkb import tagesgeld202307 as tagesgeld
from fintl.accounts_etl.dkb.files import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources


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
    file = Path("11-11-2023_Umsatzliste_Tagesgeld_DE01234567890123456789.csv")
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
