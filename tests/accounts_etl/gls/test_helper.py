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
