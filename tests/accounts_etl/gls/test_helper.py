from pathlib import Path

import fintl.accounts_etl.gls.helper


def test_detect_separator_semicolon():
    lines = ["Bezeichnung Auftragskonto;IBAN Auftragskonto;..."]
    assert fintl.accounts_etl.gls.helper.detect_separator(lines) == ";"


def test_detect_separator_no_match():
    lines = ["Some other header"]
    assert fintl.accounts_etl.gls.helper.detect_separator(lines) is None


def test_detect_separator_empty_lines():
    lines = ["", ""]
    assert fintl.accounts_etl.gls.helper.detect_separator(lines) is None


def test_detect_separator_mixed_lines():
    lines = ["Some other header", "Bezeichnung Auftragskonto;IBAN Auftragskonto;..."]
    assert fintl.accounts_etl.gls.helper.detect_separator(lines) == ";"


def test_detect_separator_empty_file():
    lines: list[str] = []
    assert fintl.accounts_etl.gls.helper.detect_separator(lines) is None


def test_check_if_parser_applies_valid_file(tmp_path: Path):
    # Create a dummy file with the expected name and content
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text(
        "Bezeichnung Auftragskonto;IBAN Auftragskonto;...\nBetrag (€);..."
    )

    assert fintl.accounts_etl.gls.helper.check_if_parser_applies(file_path) is True


def test_check_if_parser_applies_invalid_file_name(tmp_path: Path):
    # Create a dummy file with an invalid name
    file_path = tmp_path / "invalid_file_name.csv"
    file_path.write_text(
        "Bezeichnung Auftragskonto;IBAN Auftragskonto;...\nBetrag (€);..."
    )

    assert fintl.accounts_etl.gls.helper.check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_invalid_separator(tmp_path: Path):
    # Create a dummy file with the expected name but an invalid separator
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text(
        "Bezeichnung Auftragskonto,IBAN Auftragskonto,...\nBetrag (€),..."
    )

    assert fintl.accounts_etl.gls.helper.check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_empty_file(tmp_path: Path):
    # Create an empty dummy file with the expected name
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("")

    assert fintl.accounts_etl.gls.helper.check_if_parser_applies(file_path) is False


from unittest.mock import patch

import polars as pl
import pytest

import fintl.accounts_etl.gls.helper as gls_helper
from fintl.accounts_etl.schemas import Case

_CASE = Case(provider="gls", service="giro", parser="giro0")


def test_extract_transactions_raises_when_separator_is_none(tmp_path: Path):
    """extract_transactions must raise ValueError when detect_separator returns None."""
    lines = ["Bezeichnung Auftragskonto;IBAN Auftragskonto;...\n", "data;row\n"]
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("".join(lines))

    with patch.object(gls_helper, "detect_separator", return_value=None):
        with pytest.raises(ValueError, match="separator"):
            gls_helper.extract_transactions(_CASE, file_path, lines, "utf-8")


def test_extract_transactions_raises_on_invalid_date(tmp_path: Path):
    """extract_transactions must re-raise InvalidOperationError when date parsing fails."""
    header = "Bezeichnung Auftragskonto;IBAN Auftragskonto;BIC Auftragskonto;Bankname Auftragskonto;Buchungstag;Valutadatum;Name Zahlungsbeteiligter;IBAN Zahlungsbeteiligter;BIC (SWIFT-Code) Zahlungsbeteiligter;Buchungstext;Verwendungszweck;Betrag;Waehrung;Saldo nach Buchung;Bemerkung;Kategorie;Steuerrelevant;Glaeubiger ID;Mandatsreferenz\n"
    data_row = "My Bank;DE00000000000000000000;BIC;Bank;NOT-A-DATE;NOT-A-DATE;Alice;DE111;BIC2;text;desc;-1,00;EUR;100,00;;;;\n"
    lines = [header, data_row]
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("".join(lines))

    with pytest.raises(pl.exceptions.InvalidOperationError):
        gls_helper.extract_transactions(_CASE, file_path, lines, "utf-8")


def test_extract_balance_raises_when_date_is_not_datetime_date(tmp_path: Path):
    """extract_balance must raise ValueError when the date column entry is not
    a datetime.date instance."""

    schema = {
        "date": pl.Utf8,  # wrong type — will not be a datetime.date
        "source": pl.Utf8,
        "recipient": pl.Utf8,
        "amount": pl.Float64,
        "description": pl.Utf8,
        "hash": pl.UInt64,
        "provider": pl.Utf8,
        "service": pl.Utf8,
        "parser": pl.Utf8,
        "file": pl.Utf8,
        "Saldo nach Buchung": pl.Float64,
        "Waehrung": pl.Utf8,
    }
    transactions = pl.DataFrame(
        {
            "date": ["2024-01-01"],
            "source": ["Alice"],
            "recipient": ["Bob"],
            "amount": [-1.0],
            "description": ["desc"],
            "hash": pl.Series([123], dtype=pl.UInt64),
            "provider": ["gls"],
            "service": ["giro"],
            "parser": ["giro0"],
            "file": ["f.csv"],
            "Saldo nach Buchung": [100.0],
            "Waehrung": ["EUR"],
        }
    )

    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("dummy")

    with pytest.raises(ValueError, match="datetime.date"):
        gls_helper.extract_balance(_CASE, transactions, file_path)
