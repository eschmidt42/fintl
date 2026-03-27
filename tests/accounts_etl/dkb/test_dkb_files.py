"""Unit tests for fintl.accounts_etl.dkb.files – covering edge-case branches."""

from pathlib import Path

import polars as pl
import pytest

from fintl.accounts_etl.dkb.files import (
    balance_name_to_parquet,
    concatenate_new_transactions,
    concatenate_transactions_history,
    transaction_name_to_parquet,
)
from fintl.accounts_etl.schemas import TRANSACTION_COLUMNS

# ── balance_name_to_parquet ───────────────────────────────────────────────────


def test_balance_name_to_parquet_unsupported_suffix_raises():
    with pytest.raises(ValueError, match="Unexpected suffix"):
        balance_name_to_parquet(Path("export.txt"))


# ── transaction_name_to_parquet ───────────────────────────────────────────────


def test_transaction_name_to_parquet_unsupported_suffix_raises():
    with pytest.raises(ValueError, match="Unexpected suffix"):
        transaction_name_to_parquet(Path("export.txt"))


# ── concatenate_new_transactions ──────────────────────────────────────────────


def _make_transactions(extra_col: str | None = None) -> pl.DataFrame:
    """Return a minimal valid transactions DataFrame."""
    data: dict = {col: pl.Series([], dtype=pl.Utf8) for col in TRANSACTION_COLUMNS}
    data["date"] = pl.Series([], dtype=pl.Date)
    data["amount"] = pl.Series([], dtype=pl.Float64)
    data["hash"] = pl.Series([], dtype=pl.UInt64)
    df = pl.DataFrame(data)
    if extra_col:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(extra_col))
    return df


def test_concatenate_new_transactions_missing_parquet_is_skipped(tmp_path: Path):
    """A missing parquet file must be skipped with a warning, not crash."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    # Reference a CSV source file whose parquet counterpart doesn't exist.
    result, n_new = concatenate_new_transactions(
        parser_dir, parsed_dir, [parsed_dir / "missing.csv"]
    )

    assert result is None
    assert n_new == 0


def test_concatenate_new_transactions_empty_file_list_returns_none(tmp_path: Path):
    """An empty file list must return (None, 0)."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    result, n_new = concatenate_new_transactions(parser_dir, parsed_dir, [])

    assert result is None
    assert n_new == 0


def test_concatenate_new_transactions_column_mismatch_logs_warning(tmp_path: Path):
    """When two parsed parquets have different columns the extra columns must be
    discarded and a warning issued (no crash)."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    # Write two parquet files: first has extra column that the second lacks,
    # so common_columns is narrowed and discarded_columns is populated.
    df_a = _make_transactions(extra_col="extra_column")
    df_b = _make_transactions()

    path_a = parsed_dir / "file_a-transactions.parquet"
    path_b = parsed_dir / "file_b-transactions.parquet"
    df_a.write_parquet(path_a)
    df_b.write_parquet(path_b)

    result, n_new = concatenate_new_transactions(
        parser_dir,
        parsed_dir,
        [parsed_dir / "file_a.csv", parsed_dir / "file_b.csv"],
    )

    assert result is not None
    assert "extra_column" not in result.columns


# ── concatenate_transactions_history ─────────────────────────────────────────


def test_concatenate_transactions_history_returns_early_when_no_transactions(
    tmp_path: Path,
):
    """When concatenate_new_transactions returns None (all parquets missing),
    concatenate_transactions_history must log a warning and return without
    writing any file."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    concatenate_transactions_history(parser_dir, parsed_dir, [])

    assert not (parser_dir / "transactions.parquet").exists()
    assert not (parser_dir / "transactions.xlsx").exists()
