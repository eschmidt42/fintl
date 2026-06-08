"""Unit tests for fintl.etl.engine.parse_utils."""

import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest

from fintl.common import Case
from fintl.etl.common.schemas import BalanceInfo
from fintl.etl.engine import parse_utils
from fintl.etl.engine.parse_utils import ParseFn, parse_new_files


@pytest.fixture
def case() -> MagicMock:
    """Return a minimal Case mock."""
    return MagicMock(spec=Case)


@pytest.fixture
def good_result() -> tuple[pl.DataFrame, None]:
    """Return a (transactions, balance) pair suitable for a passing parse_fn."""
    return pl.DataFrame(), None


def _make_parse_fn(result: tuple[pl.DataFrame, BalanceInfo | None]) -> ParseFn:
    """Return a parse_fn that always returns *result*."""
    return MagicMock(return_value=result)


# ── early-return ──────────────────────────────────────────────────────────────


def test_empty_list_returns_early_without_mkdir(tmp_path: Path, case: MagicMock) -> None:
    """parse_new_files returns [] immediately and never creates parsed_dir."""
    parsed_dir = tmp_path / "parsed"
    result = parse_new_files(case, [], parsed_dir, parse_fn=MagicMock())
    assert result == []
    assert not parsed_dir.exists()


# ── directory creation ────────────────────────────────────────────────────────


def test_creates_parsed_dir_when_missing(
    tmp_path: Path, case: MagicMock, good_result: tuple[pl.DataFrame, None]
) -> None:
    """parse_new_files creates parsed_dir (with parents) when it does not exist."""
    file1 = tmp_path / "a.csv"
    file1.touch()
    parsed_dir = tmp_path / "deep" / "parsed"

    with (
        patch.object(parse_utils, "store_transactions"),
        patch.object(parse_utils, "store_balance"),
    ):
        parse_new_files(case, [file1], parsed_dir, parse_fn=_make_parse_fn(good_result))

    assert parsed_dir.is_dir()


def test_does_not_recreate_existing_parsed_dir(
    tmp_path: Path, case: MagicMock, good_result: tuple[pl.DataFrame, None]
) -> None:
    """parse_new_files does not fail when parsed_dir already exists."""
    file1 = tmp_path / "a.csv"
    file1.touch()
    parsed_dir = tmp_path / "parsed"
    parsed_dir.mkdir()

    with (
        patch.object(parse_utils, "store_transactions"),
        patch.object(parse_utils, "store_balance"),
    ):
        parse_new_files(case, [file1], parsed_dir, parse_fn=_make_parse_fn(good_result))

    assert parsed_dir.is_dir()


# ── happy path ────────────────────────────────────────────────────────────────


def test_calls_parse_fn_and_stores_results(
    tmp_path: Path, case: MagicMock, good_result: tuple[pl.DataFrame, None]
) -> None:
    """parse_new_files calls parse_fn for each file and stores transactions+balance."""
    transactions, balance = good_result
    file1 = tmp_path / "a.csv"
    file2 = tmp_path / "b.csv"
    file1.touch()
    file2.touch()
    parsed_dir = tmp_path / "parsed"
    parse_fn = _make_parse_fn(good_result)

    with (
        patch.object(parse_utils, "store_transactions") as mock_st,
        patch.object(parse_utils, "store_balance") as mock_sb,
    ):
        result = parse_new_files(case, [file1, file2], parsed_dir, parse_fn=parse_fn)

    assert result == [file1, file2]
    assert mock_st.call_args_list == [
        call(parsed_dir, file1, transactions),
        call(parsed_dir, file2, transactions),
    ]
    assert mock_sb.call_args_list == [
        call(parsed_dir, file1, balance),
        call(parsed_dir, file2, balance),
    ]


# ── error handling ────────────────────────────────────────────────────────────


def test_error_propagates_when_catch_errors_is_none(tmp_path: Path, case: MagicMock) -> None:
    """When catch_errors=None, exceptions from parse_fn propagate to the caller."""
    file1 = tmp_path / "a.csv"
    file1.touch()
    parsed_dir = tmp_path / "parsed"
    parse_fn: ParseFn = MagicMock(side_effect=ValueError("boom"))

    with pytest.raises(ValueError, match="boom"):
        parse_new_files(case, [file1], parsed_dir, parse_fn=parse_fn)


def test_skips_file_on_caught_error_silently(
    tmp_path: Path,
    case: MagicMock,
    good_result: tuple[pl.DataFrame, None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When catch_errors is set and log_parse_errors=False, the file is skipped silently."""
    file_bad = tmp_path / "bad.csv"
    file_good = tmp_path / "good.csv"
    file_bad.touch()
    file_good.touch()
    parsed_dir = tmp_path / "parsed"

    def _parse(c: Any, path: Path) -> tuple[pl.DataFrame, None]:
        if path == file_bad:
            raise ValueError("bad")
        return good_result

    with (
        patch.object(parse_utils, "store_transactions") as mock_st,
        patch.object(parse_utils, "store_balance"),
        caplog.at_level(logging.WARNING),
    ):
        result = parse_new_files(
            case,
            [file_bad, file_good],
            parsed_dir,
            parse_fn=_parse,
            catch_errors=(ValueError,),
        )

    assert result == [file_good]
    assert mock_st.call_count == 1
    assert "bad" not in caplog.text


def test_skips_file_on_caught_error_with_warning_log(
    tmp_path: Path,
    case: MagicMock,
    good_result: tuple[pl.DataFrame, None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When log_parse_errors=True, a WARNING is emitted for each skipped file."""
    file_bad = tmp_path / "bad.csv"
    file_good = tmp_path / "good.csv"
    file_bad.touch()
    file_good.touch()
    parsed_dir = tmp_path / "parsed"

    def _parse(c: Any, path: Path) -> tuple[pl.DataFrame, None]:
        if path == file_bad:
            raise ValueError("exploded")
        return good_result

    with (
        patch.object(parse_utils, "store_transactions"),
        patch.object(parse_utils, "store_balance"),
        caplog.at_level(logging.WARNING),
    ):
        result = parse_new_files(
            case,
            [file_bad, file_good],
            parsed_dir,
            parse_fn=_parse,
            catch_errors=(ValueError,),
            log_parse_errors=True,
        )

    assert result == [file_good]
    assert "bad.csv" in caplog.text


def test_only_caught_exception_types_are_suppressed(tmp_path: Path, case: MagicMock) -> None:
    """Exceptions not in catch_errors still propagate even when catch_errors is set."""
    file1 = tmp_path / "a.csv"
    file1.touch()
    parsed_dir = tmp_path / "parsed"
    parse_fn: ParseFn = MagicMock(side_effect=RuntimeError("unexpected"))

    with pytest.raises(RuntimeError, match="unexpected"):
        parse_new_files(
            case,
            [file1],
            parsed_dir,
            parse_fn=parse_fn,
            catch_errors=(ValueError,),
        )
