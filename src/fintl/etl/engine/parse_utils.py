"""Shared utility for the parse-new-files loop used by all parser modules."""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl

from fintl.common import Case
from fintl.etl.common.schemas import BalanceInfo
from fintl.etl.io.files.balances import store_balance
from fintl.etl.io.files.transactions import store_transactions

logger = logging.getLogger(__name__)

ParseFn = Callable[[Case, Path], tuple[pl.DataFrame, BalanceInfo | None]]
StoreFn = Callable[[Path, Path, Any], None]


def parse_new_files(
    case: Case,
    new_files_to_parse: list[Path],
    parsed_dir: Path,
    parse_fn: ParseFn,
    *,
    store_transactions_fn: StoreFn | None = None,
    store_balance_fn: StoreFn | None = None,
    catch_errors: tuple[type[Exception], ...] | None = None,
    log_parse_errors: bool = False,
) -> list[Path]:
    """Parse newly discovered files and store results; return successfully parsed paths.

    Args:
        case: Parser identity (provider / service / parser).
        new_files_to_parse: Raw files that have not yet been parsed.
        parsed_dir: Directory to write per-file parquet output into.
        parse_fn: Callable ``(case, file_path) -> (transactions_df, balance)``
            that does the actual parsing for one file.
        store_transactions_fn: Callable to persist the transactions DataFrame.
            Defaults to the CSV-based store; HTML/PNG parsers pass their own.
        store_balance_fn: Callable to persist the balance record.
            Defaults to the CSV-based store; HTML/PNG parsers pass their own.
        catch_errors: Exception types to catch per-file and skip silently.
            When ``None`` (default) no exceptions are suppressed.
        log_parse_errors: When ``True``, log a WARNING with traceback for each
            caught per-file error. Has no effect when ``catch_errors`` is ``None``.
    """
    _store_t: StoreFn = (
        store_transactions_fn if store_transactions_fn is not None else store_transactions
    )
    _store_b: StoreFn = store_balance_fn if store_balance_fn is not None else store_balance

    if not new_files_to_parse:
        logger.info("No new files to parse")
        return []

    if not parsed_dir.exists():
        logger.info(f"Creating {parsed_dir=}")
        parsed_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Parsing {len(new_files_to_parse):_} new files to {parsed_dir=}")

    parsed: list[Path] = []
    _catch = catch_errors or ()
    for file_path in new_files_to_parse:
        logger.debug(f"Parsing {file_path=} to {parsed_dir=}")
        try:
            transactions, balance = parse_fn(case, file_path)
        except _catch:
            if log_parse_errors:
                logger.warning("Failed to parse %s", file_path.name, exc_info=True)
            continue

        _store_t(parsed_dir, file_path, transactions)
        _store_b(parsed_dir, file_path, balance)
        parsed.append(file_path)

    logger.info(f"Finished parsing {len(parsed):_d} new files")
    return parsed
