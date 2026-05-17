"""Helper utilities for the search command."""

from functools import cache
from pathlib import Path

import polars as pl


@cache
def get_transactions(path_root: Path) -> pl.DataFrame:
    """Load and sort transactions from parquet, caching the result."""
    path_transactions = path_root / "all-transactions.parquet"
    assert path_transactions.exists()

    df = pl.read_parquet(path_transactions)

    df = df.sort("date", descending=True)
    df = df.drop(["file", "hash"])
    return df
