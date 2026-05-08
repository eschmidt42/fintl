from pathlib import Path

import polars as pl
import pytest

from fintl.accounts_etl.common.transactions import (
    hash_transactions,
    verify_transactions,
)
from fintl.accounts_etl.io.files.detect import find_line_with_pattern, is_match


def test_is_match():
    assert is_match(r"^\d+$", "123") is True
    assert is_match(r"^\d+$", "123a") is False
    assert is_match(r"abc", "xyzabcxyz") is True
    assert is_match(r"^abc", "xyzabcxyz") is False


def test_find_line_with_pattern():
    lines = ["abc", "123", "def456", "ghi"]
    pattern = r"\d+"
    ix, line = find_line_with_pattern(lines, pattern)
    assert ix == 1
    assert line == "123"

    lines = ["abc", "def", "ghi"]
    pattern = r"\d+"
    with pytest.raises(ValueError):
        find_line_with_pattern(lines, pattern)


def test_hash_transactions():
    data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    transactions = pl.DataFrame(data)
    hash_columns = ["col1", "col2"]
    hashed_transactions = hash_transactions(transactions, hash_columns)
    assert "hash" in hashed_transactions.columns
    assert len(hashed_transactions) == 3


def test_verify_transactions(tmp_path: Path):
    data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    transactions = pl.DataFrame(data)
    transaction_columns = ["col1", "col2"]
    verify_transactions(transaction_columns, transactions, tmp_path)

    transaction_columns = ["col1", "col2", "col3"]
    with pytest.raises(ValueError):
        verify_transactions(transaction_columns, transactions, tmp_path)


def test_detect_encoding_fallback_when_chardet_returns_none(tmp_path: Path):
    """When chardet cannot detect an encoding (returns None), detect_encoding
    must fall back to the default encoding ('utf-8')."""
    from unittest.mock import patch

    from fintl.accounts_etl.io.files.detect import detect_encoding

    dummy_file = tmp_path / "dummy.csv"
    dummy_file.write_bytes(b"some bytes")

    with patch("chardet.detect", return_value={"encoding": None}):
        result = detect_encoding(dummy_file)

    assert result == "utf-8"
