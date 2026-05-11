from pathlib import Path

import polars as pl
import pytest

from fintl.etl.common.transactions import (
    hash_transactions,
    verify_transactions,
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
