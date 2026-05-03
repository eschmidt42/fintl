"""Unit tests for fintl.accounts_etl.dkb.files – covering edge-case branches."""

from datetime import date
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


@pytest.fixture
def schema_and_other_cols() -> tuple[pl.Schema, list[str]]:
    other_cols = [
        col for col in TRANSACTION_COLUMNS if col not in ["date", "amount", "hash"]
    ]
    schema = pl.Schema(
        {
            "date": pl.Date,
            "amount": pl.Float64,
            "hash": pl.UInt64,
            **{c: pl.Utf8 for c in other_cols},
        }
    )
    return schema, other_cols


def test_concatenate_new_transactions_merges_with_existing(
    tmp_path: Path, schema_and_other_cols: tuple[pl.Schema, list[str]]
):
    """New transactions should be merged with existing ones, deduplicated by hash."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    schema, other_cols = schema_and_other_cols

    # Create existing transactions.parquet with 2 rows.
    df1 = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            **{col: ["old"] for col in other_cols},
        },
        schema=schema,
    )
    df2 = pl.DataFrame(
        {
            "date": ["2023-01-02"],
            "amount": [200.0],
            "hash": [456],
            **{col: ["old"] for col in other_cols},
        },
        schema=schema,
    )

    existing_df = pl.concat([df1, df2])
    existing_path = parser_dir / "transactions.parquet"
    existing_df.write_parquet(existing_path)

    # Create a new parquet with 1 new row and 1 duplicate (same hash as existing).
    df1 = pl.DataFrame(
        {
            "date": ["2023-01-03"],
            "amount": [300.0],
            "hash": [789],  # New
            **{col: ["new"] for col in other_cols},
        },
        schema=schema,
    )
    df2 = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],  # Duplicate
            **{col: ["duplicate"] for col in other_cols},
        },
        schema=schema,
    )
    new_df = pl.concat([df1, df2])
    new_path = parsed_dir / "new-transactions.parquet"
    new_df.write_parquet(new_path)

    # Run the function.
    result, n_new = concatenate_new_transactions(
        parser_dir, parsed_dir, [parsed_dir / "new.csv"]
    )

    # Assertions.
    assert result is not None
    assert len(result) == 3  # 2 old + 1 new (duplicate is removed)
    assert n_new == 1  # Only 1 new row added
    assert set(result["hash"].to_list()) == {123, 456, 789}
    assert sorted(result["date"].to_list()) == [
        date(2023, 1, 1),
        date(2023, 1, 2),
        date(2023, 1, 3),
    ]


def test_concatenate_new_transactions_no_existing_file(
    tmp_path: Path, schema_and_other_cols: tuple[pl.Schema, list[str]]
):
    """If no existing transactions.parquet, the result should only contain new data."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    schema, other_cols = schema_and_other_cols

    # Create a new parquet with 2 rows.
    df1 = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            **{col: ["new"] for col in other_cols},
        },
        schema=schema,
    )
    df2 = pl.DataFrame(
        {
            "date": ["2023-01-02"],
            "amount": [200.0],
            "hash": [456],
            **{col: ["new"] for col in other_cols},
        },
        schema=schema,
    )
    new_df = pl.concat([df1, df2])
    new_path = parsed_dir / "new-transactions.parquet"
    new_df.write_parquet(new_path)

    # Run the function.
    result, n_new = concatenate_new_transactions(
        parser_dir, parsed_dir, [parsed_dir / "new.csv"]
    )

    # Assertions.
    assert result is not None
    assert len(result) == 2
    assert n_new == 2
    assert set(result["hash"].to_list()) == {123, 456}


def test_concatenate_new_transactions_all_duplicates(
    tmp_path: Path, schema_and_other_cols: tuple[pl.Schema, list[str]]
):
    """If all new transactions are duplicates, n_new should be 0."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    schema, other_cols = schema_and_other_cols

    # Create existing transactions.parquet with 1 row.
    df1 = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            **{col: ["old"] for col in other_cols},
        },
        schema=schema,
    )
    existing_df = df1
    existing_path = parser_dir / "transactions.parquet"
    existing_df.write_parquet(existing_path)

    # Create a new parquet with 1 duplicate row.
    df1 = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],  # Duplicate
            **{col: ["duplicate"] for col in other_cols},
        },
        schema=schema,
    )
    new_df = df1
    new_path = parsed_dir / "new-transactions.parquet"
    new_df.write_parquet(new_path)

    # Run the function.
    result, n_new = concatenate_new_transactions(
        parser_dir, parsed_dir, [parsed_dir / "new.csv"]
    )

    # Assertions.
    assert result is not None
    assert len(result) == 1  # No new rows added
    assert n_new == 0


def test_concatenate_new_transactions_enforces_transaction_columns(
    tmp_path: Path, schema_and_other_cols: tuple[pl.Schema, list[str]]
):
    """The output must only contain TRANSACTION_COLUMNS, even if input has extra."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    schema, other_cols = schema_and_other_cols
    schema["extra_column"] = pl.Utf8

    # Create a new parquet with an extra column.
    df1 = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            "extra_column": ["foo"],
            **{col: ["new"] for col in other_cols},
        },
        schema=schema,
    )
    new_df = df1
    new_path = parsed_dir / "new-transactions.parquet"
    new_df.write_parquet(new_path)

    # Run the function.
    result, n_new = concatenate_new_transactions(
        parser_dir, parsed_dir, [parsed_dir / "new.csv"]
    )

    # Assertions.
    assert result is not None
    assert set(result.columns) == set(TRANSACTION_COLUMNS)
    assert "extra_column" not in result.columns


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
