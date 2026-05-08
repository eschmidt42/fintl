import logging
import typing as T
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def error_if_overlap(parser: str, known_files: set[Path], new_files: list[Path]):
    overlap = known_files.intersection(new_files)
    if len(overlap) > 0:
        msg = f"{parser=} would parse the following files that other parsers would parse as well: {overlap=}"
        logger.error(msg)
        raise ValueError(msg)


def load_lines(path: Path, encoding: str) -> T.List[str]:
    with open(path, "r", encoding=encoding) as f:
        lines = f.readlines()
    return lines


def find_common_columns(dfs: list[pl.DataFrame]) -> None:
    """Identifies columns common to all provided DataFrames and logs the results.

    Iterates through the list of DataFrames to determine which columns are
    present in all of them, while tracking columns that differ or are missing.

    Args:
        dfs: A list of Polars DataFrames to compare.
    """

    common_columns, discarded_columns = None, []

    for transaction_df in dfs:
        if common_columns is None:
            common_columns = transaction_df.columns
        else:
            new_common_columns = [
                c for c in common_columns if c in transaction_df.columns
            ]
            if len(new_common_columns) < len(common_columns):
                discarded_columns.extend(
                    list(set(common_columns).difference(new_common_columns))
                )
            discarded_columns.extend(
                list(set(transaction_df.columns).difference(new_common_columns))
            )
            common_columns = new_common_columns

    logger.info(f"Kept the columns {common_columns}")
    if len(discarded_columns) > 0:
        logger.warning(f"Discarded the columns {list(set(discarded_columns))}")
