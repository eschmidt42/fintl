"""Tests for filename utilities."""


# ── balance_name_to_parquet ───────────────────────────────────────────────────

from pathlib import Path

import pytest

from fintl.etl.io.files.filenames import balance_name_to_parquet, transaction_name_to_parquet


def test_balance_name_to_parquet_unsupported_suffix_raises():
    """Test that balance_name_to_parquet raises ValueError for unsupported file suffixes."""
    with pytest.raises(ValueError, match="Unexpected suffix"):
        balance_name_to_parquet(Path("export.txt"))


# ── transaction_name_to_parquet ───────────────────────────────────────────────


def test_transaction_name_to_parquet_unsupported_suffix_raises():
    """Test that transaction_name_to_parquet raises ValueError for unsupported file suffixes."""
    with pytest.raises(ValueError, match="Unexpected suffix"):
        transaction_name_to_parquet(Path("export.txt"))
