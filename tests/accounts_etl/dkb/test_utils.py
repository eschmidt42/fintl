from pathlib import Path

import polars as pl
import pytest

from fintl.accounts_etl.utils import (
    GermanNumberParsingError,
    check_if_german_number,
    find_line_with_pattern,
    german_string_numbers_to_floats,
    hash_transactions,
    is_match,
    verify_transactions,
)


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


def test_check_if_german_number():
    assert check_if_german_number("1.234,56") is True
    assert check_if_german_number("1,234.56") is False
    assert check_if_german_number("1.234") is True
    assert check_if_german_number("1,23") is True
    assert check_if_german_number("1.23") is False
    assert check_if_german_number("1234") is True
    assert check_if_german_number("12") is True
    assert check_if_german_number("1.2") is False


def test_german_string_numbers_to_floats():
    assert german_string_numbers_to_floats("1.234,56") == 1234.56
    assert german_string_numbers_to_floats("1.000.000,00") == 1000000.00
    assert german_string_numbers_to_floats("1,23") == 1.23
    assert german_string_numbers_to_floats(123) == 123
    assert german_string_numbers_to_floats(123.45) == 123.45

    assert german_string_numbers_to_floats("1.234") == 1_234
    assert german_string_numbers_to_floats("1,23") == 1.23
    assert german_string_numbers_to_floats("1234") == 1_234
    assert german_string_numbers_to_floats("12") == 12

    with pytest.raises(GermanNumberParsingError):
        german_string_numbers_to_floats("1,234.56")

    assert (
        german_string_numbers_to_floats("1.234,56 EUR", strip_currency=True) == 1234.56
    )


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
