"""Tests for DKB ETL utility functions."""

from pathlib import Path

import pytest

from fintl.etl.io.files.detect import find_line_with_pattern, is_match


def test_is_match():
    """Test that is_match returns True when a pattern matches and False otherwise."""
    assert is_match(r"^\d+$", "123") is True
    assert is_match(r"^\d+$", "123a") is False
    assert is_match(r"abc", "xyzabcxyz") is True
    assert is_match(r"^abc", "xyzabcxyz") is False


def test_find_line_with_pattern():
    """Test that find_line_with_pattern returns the first matching line or raises ValueError."""
    lines = ["abc", "123", "def456", "ghi"]
    pattern = r"\d+"
    ix, line = find_line_with_pattern(lines, pattern)
    assert ix == 1
    assert line == "123"

    lines = ["abc", "def", "ghi"]
    pattern = r"\d+"
    with pytest.raises(ValueError):
        find_line_with_pattern(lines, pattern)


def test_detect_encoding_fallback_when_chardet_returns_none(tmp_path: Path):
    """When chardet cannot detect an encoding, detect_encoding must fall back to 'utf-8'."""
    from unittest.mock import patch

    from fintl.etl.io.files.detect import detect_encoding

    dummy_file = tmp_path / "dummy.csv"
    dummy_file.write_bytes(b"some bytes")

    with patch("chardet.detect", return_value={"encoding": None}):
        result = detect_encoding(dummy_file)

    assert result == "utf-8"
