"""Shared utility for the parse-new-files loop used by all parser modules."""

import logging
from collections.abc import Callable
from pathlib import Path

import polars as pl

from fintl.common import Case
from fintl.etl.common.schemas import BalanceInfo
from fintl.etl.io.files.balances import store_balance
from fintl.etl.io.files.transactions import store_transactions

logger = logging.getLogger(__name__)

ParseFn = Callable[[Case, Path], tuple[pl.DataFrame, BalanceInfo | None]]


def parse_new_files(
    case: Case,
    new_files_to_parse: list[Path],
    parsed_dir: Path,
    parse_fn: ParseFn,
    *,
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
        catch_errors: Exception types to catch per-file and skip silently.
            When ``None`` (default) no exceptions are suppressed.
        log_parse_errors: When ``True``, log a WARNING with traceback for each
            caught per-file error. Has no effect when ``catch_errors`` is ``None``.
    """
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

        store_transactions(parsed_dir, file_path, transactions)
        store_balance(parsed_dir, file_path, balance)
        parsed.append(file_path)

    logger.info(f"Finished parsing {len(parsed):_d} new files")
    return parsed
