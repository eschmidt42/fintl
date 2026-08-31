"""Tests for transaction file storage utilities."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from fintl.etl.common.schemas import TRANSACTION_COLUMNS
from fintl.etl.io.files.transactions import (
    load_transactions,
    merge_transactions,
    postprocess_old_and_new_transactions,
    process_new_transactions,
    stack_old_and_new_transactions,
    update_transactions_history,
)


def test_load_transactions_returns_loaded_dataframes(tmp_path: Path):
    """Test that load_transactions returns a list of DataFrames for existing parquet files."""
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    df = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            "source": ["src"],
            "recipient": ["rec"],
            "description": ["desc"],
            "provider": ["prov"],
            "service": ["svc"],
            "parser": ["pars"],
            "file": ["fi"],
        }
    )
    df.write_parquet(parsed_dir / "file-transactions.parquet")

    files = [parsed_dir / "file.csv"]
    result = load_transactions(parsed_dir, files)

    assert len(result) == 1
    assert result[0].equals(df)


def test_load_transactions_skips_missing_parquet(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    """Test that load_transactions skips missing parquet files and logs a warning."""
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    df = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            "source": ["src"],
            "recipient": ["rec"],
            "description": ["desc"],
            "provider": ["prov"],
            "service": ["svc"],
            "parser": ["pars"],
            "file": ["fi"],
        }
    )
    df.write_parquet(parsed_dir / "valid-transactions.parquet")

    result = load_transactions(parsed_dir, [parsed_dir / "valid.csv", parsed_dir / "missing.csv"])

    assert len(result) == 1
    assert "does not exist" in caplog.text
    assert "WARNING" in caplog.text


def test_load_transactions_skips_parquet_read_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """Test that load_transactions skips files that cannot be read."""
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    valid_file = Path("valid.csv")
    failed_file = Path("failed.csv")
    df = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            "source": ["src"],
            "recipient": ["rec"],
            "description": ["desc"],
            "provider": ["prov"],
            "service": ["svc"],
            "parser": ["pars"],
            "file": ["fi"],
        }
    )
    df.write_parquet(parsed_dir / "valid-transactions.parquet")
    df.write_parquet(parsed_dir / "failed-transactions.parquet")

    original_read_parquet = pl.read_parquet

    def read_parquet(path: Path, *args, **kwargs) -> pl.DataFrame:
        if path.name == "failed-transactions.parquet":
            raise pl.exceptions.ComputeError("invalid parquet")
        return original_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", read_parquet)
    result = load_transactions(parsed_dir, [valid_file, failed_file])

    assert len(result) == 1
    assert result[0].equals(df)
    assert "Failed to read" in caplog.text
    assert "invalid parquet" in caplog.text


def test_load_transactions_returns_empty_list_when_no_files(tmp_path: Path):
    """Test that load_transactions returns an empty list when no files are provided."""
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    result = load_transactions(parsed_dir, [])
    assert result == []


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
    """Return a Polars schema and the list of non-key transaction column names."""
    other_cols = [col for col in TRANSACTION_COLUMNS if col not in ["date", "amount", "hash"]]
    schema = pl.Schema(
        {
            "date": pl.Date,
            "amount": pl.Float64,
            "hash": pl.UInt64,
            **dict.fromkeys(other_cols, pl.Utf8),
        }
    )
    return schema, other_cols


def test_merge_transactions_merges_with_existing(
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
    result, n_new = merge_transactions(parser_dir, parsed_dir, [parsed_dir / "new.csv"])

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


def test_merge_transactions_no_existing_file(
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
    result, n_new = merge_transactions(parser_dir, parsed_dir, [parsed_dir / "new.csv"])

    # Assertions.
    assert result is not None
    assert len(result) == 2
    assert n_new == 2
    assert set(result["hash"].to_list()) == {123, 456}


def test_merge_transactions_all_duplicates(
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
    result, n_new = merge_transactions(parser_dir, parsed_dir, [parsed_dir / "new.csv"])

    # Assertions.
    assert result is not None
    assert len(result) == 1  # No new rows added
    assert n_new == 0


def test_merge_transactions_enforces_transaction_columns(
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
    result, n_new = merge_transactions(parser_dir, parsed_dir, [parsed_dir / "new.csv"])

    # Assertions.
    assert result is not None
    assert set(result.columns) == set(TRANSACTION_COLUMNS)
    assert "extra_column" not in result.columns


def test_merge_transactions_missing_parquet_is_skipped(tmp_path: Path):
    """A missing parquet file must be skipped with a warning, not crash."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    # Reference a CSV source file whose parquet counterpart doesn't exist.
    result, n_new = merge_transactions(parser_dir, parsed_dir, [parsed_dir / "missing.csv"])

    assert result is None
    assert n_new == 0


def test_merge_transactions_empty_file_list_returns_none(tmp_path: Path):
    """An empty file list must return (None, 0)."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    result, n_new = merge_transactions(parser_dir, parsed_dir, [])

    assert result is None
    assert n_new == 0


def test_merge_transactions_column_mismatch_logs_warning(tmp_path: Path):
    """When two parsed parquets have different columns, extra columns must be discarded.

    A warning must be issued and the function must not crash.
    """
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

    result, n_new = merge_transactions(
        parser_dir,
        parsed_dir,
        [parsed_dir / "file_a.csv", parsed_dir / "file_b.csv"],
    )

    assert result is not None
    assert "extra_column" not in result.columns


def test_postprocess_old_and_new_transactions_sorts_by_date():
    """Test that postprocess_old_and_new_transactions sorts the result by date ascending."""
    data = {
        "hash": [3, 2, 1],
        "date": [
            date(2023, 6, 1),
            date(2023, 3, 1),
            date(2023, 1, 1),
        ],
        "amount": [30.0, 20.0, 10.0],
        # remaining columns dummy
        "source": ["s", "s", "s"],
        "recipient": ["r", "r", "r"],
        "description": ["d", "d", "d"],
        "provider": ["p", "p", "p"],
        "service": ["s", "s", "s"],
        "parser": ["s", "s", "s"],
        "file": ["f", "f", "f"],
    }
    df = pl.DataFrame(data)

    result = postprocess_old_and_new_transactions(df)
    assert result["date"].to_list() == [
        date(2023, 1, 1),
        date(2023, 3, 1),
        date(2023, 6, 1),
    ]


def test_postprocess_old_and_new_transactions_removes_duplicates_by_hash():
    """Test that postprocess_old_and_new_transactions deduplicates rows by hash."""
    data = {
        "hash": [1, 1, 2],
        "date": [date(2023, 1, 1), date(2023, 6, 1), date(2023, 3, 1)],
        "amount": [10.0, 10.0, 20.0],
        "source": ["s", "s", "s"],
        "recipient": ["r", "r", "r"],
        "description": ["d", "d", "d"],
        "provider": ["p", "p", "p"],
        "service": ["s", "s", "s"],
        "parser": ["s", "s", "s"],
        "file": ["f", "f", "f"],
    }
    df = pl.DataFrame(data)

    result = postprocess_old_and_new_transactions(df)
    assert len(result) == 2
    assert result["hash"].to_list() == [1, 2]
    assert result["date"][0] == date(2023, 1, 1)  # hash=1 should keep earlier date


def test_process_new_transactions_filters_to_standard_columns():
    """Test that process_new_transactions drops non-standard columns from input frames."""
    non_standard = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            "source": ["src"],
            "recipient": ["rec"],
            "description": ["desc"],
            "provider": ["prov"],
            "service": ["svc"],
            "parser": ["pars"],
            "file": ["fi"],
            "extra_col": ["extra"],
        }
    )

    result = process_new_transactions([non_standard])
    assert "extra_col" not in result.columns
    assert set(result.columns) == set(TRANSACTION_COLUMNS)


def test_process_new_transactions_concatenates_multiple_frames():
    """Test that process_new_transactions concatenates multiple DataFrames correctly."""
    df1 = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [100.0],
            "hash": [123],
            "source": ["src1"],
            "recipient": ["rec1"],
            "description": ["desc1"],
            "provider": ["prov1"],
            "service": ["svc1"],
            "parser": ["pars1"],
            "file": ["fi1"],
        }
    )
    df2 = pl.DataFrame(
        {
            "date": ["2023-01-02"],
            "amount": [200.0],
            "hash": [456],
            "source": ["src2"],
            "recipient": ["rec2"],
            "description": ["desc2"],
            "provider": ["prov2"],
            "service": ["svc2"],
            "parser": ["pars2"],
            "file": ["fi2"],
        }
    )

    result = process_new_transactions([df1, df2])
    assert len(result) == 2
    assert result["hash"].to_list() == [123, 456]


def test_stack_old_and_new_transactions_merges_with_old(tmp_path: Path):
    """Test that stack_old_and_new_transactions merges new rows with existing data."""
    old_file = tmp_path / "transactions.parquet"
    old_df = pl.DataFrame(
        {col: ["old"] for col in TRANSACTION_COLUMNS},
        schema=dict.fromkeys(TRANSACTION_COLUMNS, pl.Utf8),
    ).with_columns(
        [
            pl.lit(date(2020, 1, 1)).alias("date"),
            pl.lit(10.0).alias("amount"),
            pl.lit(111).alias("hash"),
        ]
    )
    old_df.write_parquet(old_file)

    new_df = pl.DataFrame(
        {col: ["new"] for col in TRANSACTION_COLUMNS},
        schema=dict.fromkeys(TRANSACTION_COLUMNS, pl.Utf8),
    ).with_columns(
        [
            pl.lit(date(2023, 1, 1)).alias("date"),
            pl.lit(20.0).alias("amount"),
            pl.lit(222).alias("hash"),
        ]
    )

    res_df, n_old = stack_old_and_new_transactions(old_file, new_df)

    assert n_old == 1
    assert len(res_df) == 2
    assert set(res_df["hash"].to_list()) == {111, 222}


def test_stack_old_and_new_transactions_handles_missing_old_file(tmp_path: Path):
    """Test that stack_old_and_new_transactions handles a missing existing parquet file."""
    new_df = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "amount": [20.0],
            "hash": [222],
            "source": ["new_src"],
            "recipient": ["new_rec"],
            "description": ["new_desc"],
            "provider": ["new_prov"],
            "service": ["new_svc"],
            "parser": ["new_pars"],
            "file": ["new_fi"],
        }
    )

    missing_file = tmp_path / "missing.parquet"
    res_df, n_old = stack_old_and_new_transactions(missing_file, new_df)

    assert n_old == 0
    res_df = res_df.select(TRANSACTION_COLUMNS)
    assert pl.DataFrame(res_df).equals(new_df.select(TRANSACTION_COLUMNS))


def test_update_transactions_history_returns_early_when_no_transactions(
    tmp_path: Path,
):
    """When concatenate_new_transactions returns None, no file must be written.

    concatenate_transactions_history must log a warning and return without writing any file.
    """
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    update_transactions_history(parser_dir, parsed_dir, [])

    assert not (parser_dir / "transactions.parquet").exists()
    assert not (parser_dir / "transactions.xlsx").exists()
