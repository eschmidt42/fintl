"""Tests for gls.helper utilities."""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

import fintl.etl.providers.gls.helper
import fintl.etl.providers.gls.helper as gls_helper
from fintl.common import Case

_CASE = Case(provider="gls", service="giro", parser="giro0")


def test_detect_separator_semicolon():
    """Test that detect_separator returns semicolon for a GLS header line."""
    lines = ["Bezeichnung Auftragskonto;IBAN Auftragskonto;..."]
    assert fintl.etl.providers.gls.helper.detect_separator(lines) == ";"


def test_detect_separator_no_match():
    """Test that detect_separator returns None when no known header is found."""
    lines = ["Some other header"]
    assert fintl.etl.providers.gls.helper.detect_separator(lines) is None


def test_detect_separator_empty_lines():
    """Test that detect_separator returns None for a list of empty strings."""
    lines = ["", ""]
    assert fintl.etl.providers.gls.helper.detect_separator(lines) is None


def test_detect_separator_mixed_lines():
    """Test that detect_separator finds the separator even when not on the first line."""
    lines = ["Some other header", "Bezeichnung Auftragskonto;IBAN Auftragskonto;..."]
    assert fintl.etl.providers.gls.helper.detect_separator(lines) == ";"


def test_detect_separator_empty_file():
    """Test that detect_separator returns None for an empty file."""
    lines: list[str] = []
    assert fintl.etl.providers.gls.helper.detect_separator(lines) is None


def test_check_if_parser_applies_valid_file(tmp_path: Path):
    """Test that check_if_parser_applies returns True for a valid GLS file."""
    # Create a dummy file with the expected name and content
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("Bezeichnung Auftragskonto;IBAN Auftragskonto;...\nBetrag (€);...")

    assert fintl.etl.providers.gls.helper.check_if_parser_applies(file_path) is True


def test_check_if_parser_applies_invalid_file_name(tmp_path: Path):
    """Test that check_if_parser_applies returns False for a file with an invalid name."""
    # Create a dummy file with an invalid name
    file_path = tmp_path / "invalid_file_name.csv"
    file_path.write_text("Bezeichnung Auftragskonto;IBAN Auftragskonto;...\nBetrag (€);...")

    assert fintl.etl.providers.gls.helper.check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_invalid_separator(tmp_path: Path):
    """Test that check_if_parser_applies returns False when the separator is wrong."""
    # Create a dummy file with the expected name but an invalid separator
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("Bezeichnung Auftragskonto,IBAN Auftragskonto,...\nBetrag (€),...")

    assert fintl.etl.providers.gls.helper.check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_empty_file(tmp_path: Path):
    """Test that check_if_parser_applies returns False for an empty file."""
    # Create an empty dummy file with the expected name
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("")

    assert fintl.etl.providers.gls.helper.check_if_parser_applies(file_path) is False


def test_check_if_parser_applies_non_csv_file(tmp_path: Path):
    """Passing a non-CSV file (e.g. PNG) returns False without reading file content."""
    file_path = tmp_path / "Screenshot 2026-03-09 at 14.30.53.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00binary")
    assert fintl.etl.providers.gls.helper.check_if_parser_applies(file_path) is False


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
    header = "Bezeichnung Auftragskonto;IBAN Auftragskonto;BIC Auftragskonto;Bankname Auftragskonto;Buchungstag;Valutadatum;Name Zahlungsbeteiligter;IBAN Zahlungsbeteiligter;BIC (SWIFT-Code) Zahlungsbeteiligter;Buchungstext;Verwendungszweck;Betrag;Waehrung;Saldo nach Buchung;Bemerkung;Kategorie;Steuerrelevant;Glaeubiger ID;Mandatsreferenz\n"  # noqa: E501
    data_row = "My Bank;DE00000000000000000000;BIC;Bank;NOT-A-DATE;NOT-A-DATE;Alice;DE111;BIC2;text;desc;-1,00;EUR;100,00;;;;\n"  # noqa: E501
    lines = [header, data_row]
    file_path = tmp_path / "DE12345678901234567890_2023.10.26.csv"
    file_path.write_text("".join(lines))

    with pytest.raises(pl.exceptions.InvalidOperationError):
        gls_helper.extract_transactions(_CASE, file_path, lines, "utf-8")


def test_extract_balance_raises_when_date_is_not_datetime_date(tmp_path: Path):
    """extract_balance must raise ValueError "date" is not a datetime.date instance."""
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
