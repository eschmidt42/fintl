import datetime
from pathlib import Path

import polars as pl
import pytest
from inline_snapshot import snapshot

from fintl.accounts_etl.dkb.files import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.accounts_etl.gls import credit0 as credit
from fintl.accounts_etl.gls import helper
from fintl.accounts_etl.gls.credit0 import CASE
from fintl.accounts_etl.schemas import (
    BalanceInfo,
    Config,
    GLSCreditParserEnum,
    Logging,
    Provider,
    ProviderEnum,
    ServiceEnum,
    Sources,
)


@pytest.fixture
def config(tmp_path: Path) -> Config:
    credit_source_dir = (
        Path(__file__).parent.parent / "files" / "csv_files" / "GLS" / "credit"
    )
    assert credit_source_dir.exists()

    logger_path = Path(__file__).parent.parent.parent / "logger-config.json"
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(gls=Provider(credit=credit_source_dir)),
        logging=Logging(config_file=logger_path),
    )
    return config


def get_time(path: Path) -> float:
    return path.stat().st_mtime


def get_files() -> list[Path]:
    files = [
        Path("Umsaetze_DE01234567890123456789_2024.03.23.csv"),
        Path("Umsaetze_DE01234567890123456789_2024.04.13.csv"),
    ]
    return files


def test_main(config: Config):
    raw_dir = config.get_raw_dir(credit.CASE)

    files = get_files()

    copied_file_paths = [raw_dir / f for f in files]

    parsed_dir = config.get_parsed_dir(credit.CASE)
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

    parser_dir = config.get_parser_dir(credit.CASE)
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
    credit.main(config)

    # make sure the new raw files were copied as expected
    assert raw_dir.exists()
    for f in copied_file_paths:
        assert f.exists()

    # make sure the new raw files were parsed as expected
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
    ts_balance_json_single = [
        get_time(f) for f in paths_balance_json_single if f.exists()
    ]
    ts_balance_parquet_single = [
        get_time(f) for f in paths_balance_parquet_single if f.exists()
    ]
    ts_transactions_parquet_single = [
        get_time(f) for f in paths_transactions_parquet_single
    ]
    ts_transactions_xlsx_single = [get_time(f) for f in paths_transactions_xlsx_single]

    n_balances = len(pl.read_parquet(path_balances_parquet_parser))
    n_transactions = len(pl.read_parquet(path_transactions_parquet_parser))

    # running the process again ensuring nothing happens because all files are already present
    credit.main(config)

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

    credit.main(config)

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


def test_extract_transactions_valid_data(config: Config):
    files = get_files()

    file_path = config.get_source_dir("gls", "credit") / files[1]

    lines = file_path.read_text().splitlines()
    encoding = "utf-8"  # Assuming UTF-8 encoding for simplicity

    transactions_df = helper.extract_transactions(CASE, file_path, lines, encoding)

    # Assertions to validate the extracted data
    assert isinstance(transactions_df, pl.DataFrame)
    assert len(transactions_df) == snapshot(2)
    assert "amount" in transactions_df.columns
    assert "description" in transactions_df.columns
    assert transactions_df["amount"].to_list() == snapshot([21.0, -42.0])
    assert transactions_df["description"].to_list() == snapshot(
        [
            "Abrechnung vom 18.03.2024  MC Hauptkarte",
            "Jahresbeitrag              Umsatz vom 18.03.2024      MC Hauptkarte",
        ]
    )
    assert transactions_df["date"].to_list() == snapshot(
        [datetime.date(2024, 3, 26), datetime.date(2024, 3, 18)]
    )


def test_extract_balance_normal(config: Config):
    files = get_files()
    file_path = config.get_source_dir("gls", "credit") / files[1]
    lines = file_path.read_text().splitlines()
    encoding = "utf-8"
    transactions_df = helper.extract_transactions(CASE, file_path, lines, encoding)

    balance_info = helper.extract_balance(CASE, transactions_df, file_path)

    assert isinstance(balance_info, BalanceInfo)
    assert balance_info.amount == snapshot(0.0)
    assert balance_info.date == snapshot(datetime.date(2024, 3, 26))
    assert balance_info.currency == snapshot("EUR")
    assert balance_info.provider == ProviderEnum.gls.value
    assert balance_info.service == ServiceEnum.credit.value
    assert balance_info.parser == GLSCreditParserEnum.credit0.value
    assert balance_info.file == str(file_path)
