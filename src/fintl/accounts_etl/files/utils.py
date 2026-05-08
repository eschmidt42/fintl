import logging
import shutil
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


def select_files_to_copy(
    source_files: list[Path], target_files: list[Path]
) -> list[Path]:
    target_names = [file_path.name for file_path in target_files]
    files_to_copy = [
        file_path for file_path in source_files if file_path.name not in target_names
    ]
    logger.info(
        f"Selecting {len(files_to_copy):_} files to copy comparing {len(source_files):_} source files and {len(target_files):_} target files."
    )
    return files_to_copy


def copy_new_files(raw_dir: Path, new_files_to_copy: list[Path]):
    logger.info("Copying new files")

    if len(new_files_to_copy) == 0:
        logger.info("No new files to copy")
        return
    logger.info(f"Copying {len(new_files_to_copy):_d} new files to {raw_dir=}")

    if not raw_dir.exists():
        logger.info(f"Creating {raw_dir=}")
        raw_dir.mkdir(parents=True, exist_ok=True)

    for file_path in new_files_to_copy:
        new_file_path = raw_dir / file_path.name
        logger.debug(f"Copying {file_path=} to {new_file_path=}")
        shutil.copy2(file_path, new_file_path)

    logger.info(f"Finished copying {len(new_files_to_copy):_d} new files")


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
