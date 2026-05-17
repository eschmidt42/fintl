"""Tests for the search helper get_transactions function."""

import datetime
from pathlib import Path

import polars as pl
import pytest

from fintl.cli.commands.search.helper import get_transactions


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the get_transactions LRU cache before and after each test."""
    get_transactions.cache_clear()
    yield
    get_transactions.cache_clear()


def _write_transactions(tmp_path: Path, df: pl.DataFrame) -> None:
    """Write a DataFrame as all-transactions.parquet into tmp_path."""
    df.write_parquet(tmp_path / "all-transactions.parquet")


def test_get_transactions_drops_file_and_hash(tmp_path: Path):
    """Test that get_transactions removes the file and hash columns."""
    df = pl.DataFrame(
        {
            "source": ["me"],
            "recipient": ["Alice"],
            "amount": [-10.0],
            "description": ["foo"],
            "date": [datetime.date(2024, 1, 1)],
            "provider": ["DKB"],
            "service": ["giro"],
            "parser": ["giro0"],
            "file": ["/some/file.csv"],
            "hash": [123456789],
        }
    )
    _write_transactions(tmp_path, df)

    result = get_transactions(tmp_path)

    assert "file" not in result.columns
    assert "hash" not in result.columns


def test_get_transactions_sorted_descending(tmp_path: Path):
    """Test that get_transactions returns rows sorted by date descending."""
    df = pl.DataFrame(
        {
            "source": ["me", "me"],
            "recipient": ["Alice", "Bob"],
            "amount": [-10.0, 100.0],
            "description": ["foo", "bar"],
            "date": [datetime.date(2023, 1, 1), datetime.date(2024, 6, 1)],
            "provider": ["DKB", "GLS"],
            "service": ["giro", "giro"],
            "parser": ["giro0", "giro0"],
            "file": ["/a", "/b"],
            "hash": [1, 2],
        }
    )
    _write_transactions(tmp_path, df)

    result = get_transactions(tmp_path)

    dates = result["date"].to_list()
    assert dates == sorted(dates, reverse=True)
