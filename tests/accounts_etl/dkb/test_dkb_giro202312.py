import logging
from pathlib import Path

import polars as pl
import pytest
from inline_snapshot import snapshot

from fintl.accounts_etl.dkb import giro202312
from fintl.accounts_etl.dkb import giro202312 as giro
from fintl.accounts_etl.dkb.files import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.accounts_etl.dkb.giro202312 import (
    CASE,
    check_if_parser_applies,
    detect_encoding,
    detect_separator,
    extract_transactions,
    load_lines,
)
from fintl.accounts_etl.schemas import (
    Config,
    DKBGiroParserEnum,
    Logging,
    Provider,
    ProviderEnum,
    ServiceEnum,
    Sources,
)


def get_time(path: Path) -> float:
    return path.stat().st_mtime


@pytest.fixture
def config(tmp_path: Path) -> Config:
    giro_source_dir = (
        Path(__file__).parent.parent / "files" / "csv_files" / "DKB" / "kontoauszug"
    )
    assert giro_source_dir.exists()

    logger_path = (
        Path(__file__).parent.parent.parent / "fine_logging" / "logger-config.json"
    )
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(giro=giro_source_dir)),
        logging=Logging(config_file=logger_path),
    )
    return config


def get_files() -> list[Path]:
    files = [
        Path("09-12-2023_Umsatzliste_Girokonto_DE01234567890123456789.csv"),
        Path("24-02-2024_Umsatzliste_Girokonto_DE01234567890123456789.csv"),
    ]
    return files


def test_main(config: Config):
    raw_dir = config.get_raw_dir(giro.CASE)

    files = get_files()

    copied_file_paths = [raw_dir / f for f in files]

    parsed_dir = config.get_parsed_dir(giro.CASE)
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

    parser_dir = config.get_parser_dir(giro.CASE)
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
    giro.main(config)

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
    giro.main(config)

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

    giro.main(config)

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


def test_detect_separator_semicolon():
    lines = ['"yp";"IBAN";"Betrag (€)";"Glä"']
    assert detect_separator(lines) == ";"


def test_detect_separator_comma():
    lines = ['"yp","IBAN","Betrag (€)","Glä"']
    assert detect_separator(lines) == ","


def test_detect_separator_none():
    lines = ["some random line"]
    assert detect_separator(lines) is None


def test_check_if_parser_applies_true(tmp_path: Path):
    file_path = tmp_path / "DE12345678901234567890.csv"
    file_path.write_text('"yp";"IBAN";"Betrag (€)";"Glä"')
    assert check_if_parser_applies(file_path) is True


def test_check_if_parser_applies_false_filename(tmp_path: Path):
    file_path = tmp_path / "wrong_filename.csv"
    file_path.write_text('"yp";"IBAN";"Betrag (€)";"Glä"')
    assert check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_false_content(tmp_path: Path):
    file_path = tmp_path / "DE12345678901234567890.csv"
    file_path.write_text("some random content")
    assert check_if_parser_applies(file_path) is False


def test_extract_transactions(config: Config, caplog):
    caplog.set_level(logging.DEBUG)
    files = get_files()
    file_path = config.get_source_dir("dkb", "giro") / files[0]

    encoding = detect_encoding(file_path)
    lines = load_lines(file_path, encoding)

    df = extract_transactions(CASE, file_path, lines, encoding)

    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, len(giro202312.TRANSACTION_COLUMNS))
    assert df["amount"].to_list() == snapshot([12.34, -11.77])
    assert df["description"].to_list() == snapshot(
        ["2023-12-09T01:23 VISA", "2023-12-12T34:56 VISA"]
    )
    assert df["recipient"].to_list() == snapshot(["myself", "YOURFAVSUPERMARKET"])


def test_extract_transactions_invalid_date(tmp_path: Path):
    file_path = tmp_path / "test.csv"
    file_path.write_text(
        """""
"Kontostand vom 09.12.2023:";"1123,45 EUR"
""
"Buchungsdatum";"Wertstellung";"Status";"Zahlungspflichtige*r";"Zahlungsempfänger*in";"Verwendungszweck";"Umsatztyp";"IBAN";"Betrag (€)";"Gläubiger-ID";"Mandatsreferenz";"Kundenreferenz"
"11.22.23";"";"Vorgemerkt";"ISSUER";"AMZN";"2023-12-09T01:23 VISA";"Ausgang";"DExxxxxxxxxxxxx";"12,34";"";"";"1234567"
"22.12.23";"";"Vorgemerkt";"ISSUER";"YOURFAVSUPERMARKET";"2023-12-12T34:56 VISA";"Ausgang";"DExxxxxxxxxxxxx";"-11,77";"";"";"1234567"

""".strip()
    )
    lines = file_path.read_text().splitlines()
    with pytest.raises(pl.exceptions.InvalidOperationError) as excinfo:
        extract_transactions(CASE, file_path, lines, "utf-8")

    assert (
        "conversion from `str` to `date` failed in column 'Buchungsdatum' for 1 out of 1 values: [\"11.22.23\"]"
        in str(excinfo.value)
    )


def test_case_enum():
    assert CASE.provider == ProviderEnum.dkb.value
    assert CASE.service == ServiceEnum.giro.value
    assert CASE.parser == DKBGiroParserEnum.giro202312.value
