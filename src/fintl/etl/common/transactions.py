import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def hash_transactions(transactions: pl.DataFrame, hash_columns: list[str]) -> pl.DataFrame:
    """Adds a hash column to a transactions DataFrame based on specific columns.

    Args:
        transactions: DataFrame containing transaction data.
        hash_columns: List of column names to include in the hash calculation.

    Returns:
        The DataFrame with an added 'hash' column.
    """
    transactions = transactions.with_columns(hash=transactions.select(hash_columns).hash_rows())
    return transactions


def verify_transactions(
    transaction_columns: list[str], transactions: pl.DataFrame, file_path: Path
):
    """Verifies that all expected columns exist in the transactions DataFrame.

    Args:
        transaction_columns: List of expected column names.
        transactions: The DataFrame to verify.
        file_path: Path to the source file (used for error messages).

    Raises:
        ValueError: If any expected column is missing from the DataFrame.
    """
    for col in transaction_columns:
        if col not in transactions.columns:
            raise ValueError(f"Expected column '{col}' in transactions parsed from {file_path=}")
