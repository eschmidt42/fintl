"""Tests for the DKB giro202312 parser."""

import logging
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from inline_snapshot import snapshot

from fintl.common import Config, Provider, Sources
from fintl.common.logging import Logging
from fintl.etl.common.exceptions import (
    ExtractBalanceError,
    ExtractTransactionsError,
)
from fintl.etl.common.schemas import (
    DKBGiroParserEnum,
    ProviderEnum,
    ServiceEnum,
)
from fintl.etl.io.files.detect import detect_encoding
from fintl.etl.io.files.filenames import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.etl.providers.dkb import giro202312
from fintl.etl.providers.dkb import giro202312 as giro
from fintl.etl.providers.dkb.giro202312 import (
    CASE,
    check_if_parser_applies,
    detect_separator,
    extract_transactions,
    load_lines,
)


@pytest.fixture
def csv_fname() -> str:
    """Return the DKB giro202312 CSV fixture filename."""
    return "09-12-2023_Umsatzliste_Girokonto_DE01234567890123456789.csv"


@pytest.fixture
def csv_file(files_root_path: Path, csv_fname: str) -> Path:
    """Return the path to the DKB giro202312 CSV fixture file."""
    return files_root_path / "csv_files" / "DKB" / "kontoauszug" / csv_fname


def test_files_exist(files_root_path: Path, csv_file: Path):
    """Test that the required fixture files exist on disk."""
    assert files_root_path.exists()
    assert csv_file.exists()


def get_time(path: Path) -> float:
    """Return the modification time of the given path."""
    return path.stat().st_mtime


@pytest.fixture
def config(tmp_path: Path, csv_file: Path, logger_config_path: Path) -> Config:
    """Return a Config pointing at the giro202312 fixture source directory."""
    giro_source_dir = csv_file.parent
    assert giro_source_dir.exists()

    logger_path = logger_config_path
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(giro=giro_source_dir)),
        logging=Logging(config_file=logger_path),
    )
    return config


def get_files() -> list[Path]:
    """Return the list of giro202312 CSV fixture file paths."""
    files = [
        Path("09-12-2023_Umsatzliste_Girokonto_DE01234567890123456789.csv"),
        Path("24-02-2024_Umsatzliste_Girokonto_DE01234567890123456789.csv"),
    ]
    return files


def test_main(config: Config):
    """Test that giro202312.main parses files and skips already-processed ones."""
    raw_dir = config.get_raw_dir(giro.CASE)

    files = get_files()

    copied_file_paths = [raw_dir / f for f in files]

    parsed_dir = config.get_parsed_dir(giro.CASE)
    paths_balance_json_single = [parsed_dir / balance_csv_name_to_json(f) for f in files]
    paths_balance_parquet_single = [parsed_dir / balance_csv_name_to_parquet(f) for f in files]
    paths_transactions_parquet_single = [
        parsed_dir / transaction_csv_name_to_parquet(f) for f in files
    ]
    paths_transactions_xlsx_single = [parsed_dir / transaction_csv_name_to_xlsx(f) for f in files]

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
    ts_transactions_parquet_single = [get_time(f) for f in paths_transactions_parquet_single]
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
    """Test that detect_separator returns semicolon for semicolon-delimited lines."""
    lines = ['"yp";"IBAN";"Betrag (€)";"Glä"']
    assert detect_separator(lines) == ";"


def test_detect_separator_comma():
    """Test that detect_separator returns comma for comma-delimited lines."""
    lines = ['"yp","IBAN","Betrag (€)","Glä"']
    assert detect_separator(lines) == ","


def test_detect_separator_none():
    """Test that detect_separator returns None when no recognized delimiter is found."""
    lines = ["some random line"]
    assert detect_separator(lines) is None


def test_check_if_parser_applies_true(tmp_path: Path):
    """Test that check_if_parser_applies returns True for a valid giro202312 file."""
    file_path = tmp_path / "DE12345678901234567890.csv"
    file_path.write_text('"yp";"IBAN";"Betrag (€)";"Glä"')
    assert check_if_parser_applies(file_path) is True


def test_check_if_parser_applies_false_filename(tmp_path: Path):
    """Test that check_if_parser_applies returns False for a wrong filename."""
    file_path = tmp_path / "wrong_filename.csv"
    file_path.write_text('"yp";"IBAN";"Betrag (€)";"Glä"')
    assert check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_false_content(tmp_path: Path):
    """Test that check_if_parser_applies returns False when file content does not match."""
    file_path = tmp_path / "DE12345678901234567890.csv"
    file_path.write_text("some random content")
    assert check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_non_csv_file(tmp_path: Path):
    """Passing a non-CSV file (e.g. PNG) returns False without reading file content."""
    file_path = tmp_path / "Screenshot 2026-03-09 at 14.30.53.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00binary")
    assert check_if_parser_applies(file_path) is False


def test_extract_transactions(config: Config, caplog):
    """Test that extract_transactions parses the giro202312 CSV into the expected DataFrame."""
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
    """Test that extract_transactions raises InvalidOperationError for unparseable dates."""
    file_path = tmp_path / "test.csv"
    file_path.write_text(
        """""
"Kontostand vom 09.12.2023:";"1123,45 EUR"
""
"Buchungsdatum";"Wertstellung";"Status";"Zahlungspflichtige*r";"Zahlungsempfänger*in";"Verwendungszweck";"Umsatztyp";"IBAN";"Betrag (€)";"Gläubiger-ID";"Mandatsreferenz";"Kundenreferenz"
"11.22.23";"";"Vorgemerkt";"ISSUER";"AMZN";"2023-12-09T01:23 VISA";"Ausgang";"DExxxxxxxxxxxxx";"12,34";"";"";"1234567"
"22.12.23";"";"Vorgemerkt";"ISSUER";"YOURFAVSUPERMARKET";"2023-12-12T34:56 VISA";"Ausgang";"DExxxxxxxxxxxxx";"-11,77";"";"";"1234567"

""".strip()  # noqa: E501
    )
    lines = file_path.read_text().splitlines()
    with pytest.raises(pl.exceptions.InvalidOperationError) as excinfo:
        extract_transactions(CASE, file_path, lines, "utf-8")

    assert (
        "conversion from `str` to `date` failed in column 'Buchungsdatum' for 1 out of 1 values: [\"11.22.23\"]"  # noqa: E501
        in str(excinfo.value)
    )


def test_case_enum():
    """Test that CASE enum values match the expected provider, service, and parser."""
    assert CASE.provider == ProviderEnum.dkb.value
    assert CASE.service == ServiceEnum.giro.value
    assert CASE.parser == DKBGiroParserEnum.giro202312.value


def test_extract_transactions_raises_when_separator_is_none(tmp_path: Path):
    """extract_transactions must raise ValueError when no separator is found in lines."""
    from unittest.mock import patch

    file_path = tmp_path / "no_separator.csv"
    # Include the transaction header so find_line_with_pattern succeeds
    lines = ['"Buchungsdatum";"Wertstellung";"Status";"Zahlungspflichtige*r"\n']
    file_path.write_text("".join(lines))

    with patch.object(giro202312, "detect_separator", return_value=None):
        with pytest.raises(ValueError, match="separator=None"):
            extract_transactions(CASE, file_path, lines, "utf-8")


def test_parse_csv_file_raises_extract_transactions_exception(csv_file: Path):
    """Test that parse_csv_file raises ExtractTransactionsException on bad transactions."""
    with patch(
        "fintl.etl.providers.dkb.giro202312.extract_transactions",
        side_effect=ValueError("malformed transactions"),
    ):
        with pytest.raises(ExtractTransactionsError) as exc_info:
            giro.parse_csv_file(giro.CASE, csv_file)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_parse_csv_file_raises_extract_balance_exception(csv_file: Path):
    """Test that parse_csv_file raises ExtractBalanceException on bad balance data."""
    with patch(
        "fintl.etl.providers.dkb.giro202312.extract_balance",
        side_effect=ValueError("malformed balance"),
    ):
        with pytest.raises(ExtractBalanceError) as exc_info:
            giro.parse_csv_file(giro.CASE, csv_file)
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
            "fintl.etl.providers.dkb.giro202312.parse_csv_file",
            side_effect=_parse_csv_file,
        ),
        patch("fintl.etl.providers.dkb.giro202312.store_transactions") as mock_store_t,
        patch("fintl.etl.providers.dkb.giro202312.store_balance") as mock_store_b,
    ):
        giro.parse_new_files(giro.CASE, [bad_file, good_file], parsed_dir)

    mock_store_t.assert_called_once_with(parsed_dir, good_file, good_transactions)
    mock_store_b.assert_called_once_with(parsed_dir, good_file, good_balance)
