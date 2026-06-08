"""Tests for the check_applies helper in fintl.etl.io.files.applies."""

from pathlib import Path
from unittest.mock import patch

from fintl.etl.io.files.applies import check_applies


def test_returns_false_for_non_matching_filename(tmp_path: Path):
    """Returns False immediately when the filename does not match — no file is read."""
    file_path = tmp_path / "Screenshot 2026-03-09 at 14.30.53.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00binary")

    # Patching detect_encoding to assert it is never called
    with patch("fintl.etl.io.files.applies.detect_encoding") as mock_enc:
        result = check_applies(file_path, r"DE\d{20}\.csv$", lambda lines: True)

    assert result is False
    mock_enc.assert_not_called()


def test_returns_true_when_filename_and_content_match(tmp_path: Path):
    """Returns True when the filename matches and the content check passes."""
    file_path = tmp_path / "DE12345678901234567890.csv"
    file_path.write_text('"Umsatztyp";"Betrag";"Gläubiger-ID"')

    result = check_applies(
        file_path,
        r"DE\d{20}\.csv$",
        lambda lines: any("Umsatztyp" in line for line in lines),
    )

    assert result is True


def test_returns_false_when_filename_matches_but_content_check_fails(tmp_path: Path):
    """Returns False when the filename matches but the content check does not."""
    file_path = tmp_path / "DE12345678901234567890.csv"
    file_path.write_text("some unrelated content")

    result = check_applies(
        file_path,
        r"DE\d{20}\.csv$",
        lambda lines: any("Umsatztyp" in line for line in lines),
    )

    assert result is False
