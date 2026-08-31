"""Unit tests for balance I/O helpers."""

import datetime
import json
import logging
from pathlib import Path

import polars as pl
import pytest

from fintl.etl.common.schemas import BALANCE_SCHEMA, BalanceInfo
from fintl.etl.io.files.balances import (
    load_balances,
    merge_balances,
    store_balance,
    update_balances_history,
)
from fintl.etl.io.files.filenames import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
)


def _make_balance(
    date: datetime.date,
    amount: float = 100.0,
    currency: str = "EUR",
    provider: str = "dkb",
    service: str = "giro",
    parser: str = "giro0",
    file: str = "balance.csv",
) -> BalanceInfo:
    """Factory for BalanceInfo objects."""
    return BalanceInfo(
        date=date,
        amount=amount,
        currency=currency,
        provider=provider,
        service=service,
        parser=parser,
        file=file,
    )


def _write_balance_parquet(
    parsed_dir: Path, file_path: Path, balance_info: BalanceInfo | None
) -> None:
    """Writes a balance as a parquet file, mirroring store_balance behavior."""
    parquet_file = parsed_dir / balance_csv_name_to_parquet(file_path)
    if balance_info is None:
        balance_df = pl.DataFrame([], schema=BALANCE_SCHEMA)
    else:
        balance_df = pl.DataFrame([balance_info.model_dump()])
    balance_df.write_parquet(parquet_file)


def test_store_balance_writes_json_and_parquet(tmp_path: Path):
    """Tests that store_balance writes correct JSON and Parquet files for valid BalanceInfo."""
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()
    file_path = Path("balances_2023.csv")
    balance = _make_balance(datetime.date(2023, 1, 1))

    store_balance(parsed_dir, file_path, balance)

    # Verify JSON
    json_path = parsed_dir / balance_csv_name_to_json(file_path)
    assert json_path.exists()
    with json_path.open("r") as f:
        data = json.load(f)
        assert data["amount"] == 100.0
        assert data["date"] == "2023-01-01"

    # Verify Parquet
    parquet_path = parsed_dir / balance_csv_name_to_parquet(file_path)
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert len(df) == 1
    assert df[0, "amount"] == 100.0
    assert df[0, "date"] == datetime.date(2023, 1, 1)


def test_store_balance_none_writes_empty_json_and_empty_parquet(tmp_path: Path):
    """Tests that store_balance writes empty JSON and empty Parquet when balance is None."""
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()
    file_path = Path("balances_2023.csv")

    store_balance(parsed_dir, file_path, None)

    # Verify JSON is empty dict
    json_path = parsed_dir / balance_csv_name_to_json(file_path)
    assert json_path.exists()
    with json_path.open("r") as f:
        data = json.load(f)
        assert data == {}

    # Verify Parquet is empty with correct schema
    parquet_path = parsed_dir / balance_csv_name_to_parquet(file_path)
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert len(df) == 0
    assert df.schema == BALANCE_SCHEMA


def test_merge_balances_no_existing_history(tmp_path: Path):
    """Tests merge_balances when no existing balance history exists."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    # Create some new balance files
    files = [Path("f1.csv"), Path("f2.csv")]
    balances = [
        _make_balance(datetime.date(2023, 1, 1), amount=100.0),
        _make_balance(datetime.date(2023, 1, 2), amount=200.0),
    ]
    for f, b in zip(files, balances, strict=True):
        _write_balance_parquet(parsed_dir, f, b)

    merged, delta = merge_balances(parser_dir, parsed_dir, files)

    assert merged is not None
    assert len(merged) == 2
    assert delta == 2
    assert merged[0, "date"] == datetime.date(2023, 1, 1)
    assert merged[1, "date"] == datetime.date(2023, 1, 2)


def test_load_balances_skips_missing_parquet(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    """Tests that load_balances skips missing files and logs a warning."""
    caplog.set_level(logging.WARNING, logger="fintl.etl.io.files.balances")
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    valid_file = Path("valid.csv")
    _write_balance_parquet(
        parsed_dir,
        valid_file,
        _make_balance(datetime.date(2023, 1, 1)),
    )

    result = load_balances(parsed_dir, [valid_file, Path("missing.csv")])

    assert len(result) == 1
    assert len(result[0]) == 1
    assert any(
        record.levelno == logging.WARNING and "does not exist" in record.message
        for record in caplog.records
    )


def test_load_balances_skips_parquet_read_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """Tests that load_balances skips files that cannot be read."""
    caplog.set_level(logging.WARNING, logger="fintl.etl.io.files.balances")
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    valid_file = Path("valid.csv")
    failed_file = Path("failed.csv")
    _write_balance_parquet(
        parsed_dir,
        valid_file,
        _make_balance(datetime.date(2023, 1, 1)),
    )
    _write_balance_parquet(
        parsed_dir,
        failed_file,
        _make_balance(datetime.date(2023, 1, 2)),
    )

    original_read_parquet = pl.read_parquet

    def read_parquet(path: Path, *args, **kwargs) -> pl.DataFrame:
        if path.name == "failed-balance.parquet":
            raise pl.exceptions.ComputeError("invalid parquet")
        return original_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", read_parquet)
    result = load_balances(parsed_dir, [valid_file, failed_file])

    assert len(result) == 1
    assert len(result[0]) == 1
    assert any(
        record.levelno == logging.WARNING
        and "Failed to read" in record.message
        and "invalid parquet" in record.message
        for record in caplog.records
    )


def test_merge_balances_returns_none_when_no_files_are_loadable(tmp_path: Path):
    """Tests that merge_balances returns no data when generated files are missing."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    merged, delta = merge_balances(parser_dir, parsed_dir, [Path("missing.csv")])

    assert merged is None
    assert delta == 0


def test_update_balances_history_skips_missing_files(tmp_path: Path):
    """Tests that missing balance files do not create or overwrite history."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    update_balances_history(parser_dir, parsed_dir, [Path("missing.csv")])

    assert not (parser_dir / "balances.parquet").exists()
    assert not (parser_dir / "balances.xlsx").exists()


def test_merge_balances_with_existing_history(tmp_path: Path):
    """Tests merge_balances when existing balance history exists."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    # 1. Create existing history
    history_df = pl.DataFrame(
        [
            _make_balance(datetime.date(2022, 12, 31), amount=50.0).model_dump(),
        ]
    )
    history_df.write_parquet(parser_dir / "balances.parquet")

    # 2. Create new balance files
    files = [Path("f1.csv")]
    new_balance = _make_balance(datetime.date(2023, 1, 1), amount=150.0)
    _write_balance_parquet(parsed_dir, files[0], new_balance)

    merged, delta = merge_balances(parser_dir, parsed_dir, files)

    assert merged is not None
    assert len(merged) == 2
    assert delta == 1
    assert merged[0, "date"] == datetime.date(2022, 12, 31)
    assert merged[1, "date"] == datetime.date(2023, 1, 1)


def test_merge_balances_deduplicates(tmp_path: Path):
    """Tests that merge_balances deduplicates records based on (date, provider, service, parser)."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    # 1. Existing history
    balance_old = _make_balance(datetime.date(2023, 1, 1), amount=100.0)
    history_df = pl.DataFrame([balance_old.model_dump()])
    history_df.write_parquet(parser_dir / "balances.parquet")

    # 2. New balance file with SAME date/provider/service/parser but DIFFERENT amount (or same)
    files = [Path("f1.csv")]
    balance_new = _make_balance(datetime.date(2023, 1, 1), amount=100.0)
    _write_balance_parquet(parsed_dir, files[0], balance_new)

    merged, delta = merge_balances(parser_dir, parsed_dir, files)

    assert merged is not None
    # Should be deduplicated, so only 1 row remains
    assert len(merged) == 1
    assert delta == 0


def test_update_balances_history_writes_parquet_and_excel(tmp_path: Path):
    """Tests that update_balances_history writes both Parquet and Excel files."""
    parser_dir = tmp_path / "parser"
    parser_dir.mkdir()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    files = [Path("f1.csv")]
    balance = _make_balance(datetime.date(2023, 1, 1))
    _write_balance_parquet(parsed_dir, files[0], balance)

    update_balances_history(parser_dir, parsed_dir, files)

    assert (parser_dir / "balances.parquet").exists()
    assert (parser_dir / "balances.xlsx").exists()
