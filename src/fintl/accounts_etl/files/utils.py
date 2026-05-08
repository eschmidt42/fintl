import logging
import shutil
import typing as T
from pathlib import Path
from typing import Callable

import polars as pl

from fintl.accounts_etl.schemas import Case, Config

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


def detect_present_parsed_files(parsed_dir: Path) -> list[Path]:
    """Detects relevant parsed files."""
    present_parsed_files = [file_path for file_path in parsed_dir.glob("**/*.xlsx")]
    logger.info(
        f"Detected {len(present_parsed_files):_} present parsed files @ {parsed_dir=}."
    )
    return present_parsed_files


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


def detect_new_parsed_files(
    raw_dir: Path,
    parser_dir: Path,
    parsed_dir: Path,
) -> list[Path]:
    logger.info(f"Detecting newly parsed files")

    available_parsed_balance_files = list(parsed_dir.glob("*-balance.parquet"))

    all_balances_parquet_path = parser_dir / "balances.parquet"

    if all_balances_parquet_path.exists():
        all_balances = pl.read_parquet(all_balances_parquet_path)

        already_stored_files = (
            all_balances["file"].unique().to_list()
        )  # original name inlcuding .csv ending

        already_stored_files = set([Path(f).stem for f in already_stored_files])
    else:
        already_stored_files = set()

    n = len("-balance.parquet")
    newly_parsed_parquets = [
        f
        for f in available_parsed_balance_files
        if not f.name[:-n] in already_stored_files
    ]

    newly_parsed_csv_files = [
        raw_dir / f"{f.name[:-n]}.csv" for f in newly_parsed_parquets
    ]
    return newly_parsed_csv_files


def select_files_to_parse(
    present_parsed_files: list[Path], raw_files: list[Path]
) -> list[Path]:
    """Selects raw files that have not yet been parsed.

    Compares raw files against already present parsed files and returns only
    those that do not have a corresponding parsed counterpart yet.

    Args:
        present_parsed_files: List of already parsed file paths.
        raw_files: List of raw file paths to check against.

    Returns:
        A list of raw file paths that need to be parsed.
    """
    parsed_files = [file_path.name for file_path in present_parsed_files]
    files_to_parse = [
        file_path
        for file_path in raw_files
        if (file_path.name.replace(".csv", "-transactions.xlsx") not in parsed_files)
    ]
    logger.info(
        f"Selecting {len(files_to_parse):_} files to parse after comparing {len(present_parsed_files):_} present parsed files and {len(raw_files):_} raw files."
    )
    return files_to_parse


def detect_raw_files(raw_dir: Path, check_if_parser_applies: Callable) -> list[Path]:
    """Detects relevant raw CSV files in the given directory.

    Args:
        raw_dir: The directory to search for raw files.
        check_if_parser_applies: A callable that takes a file path and returns True
            if the file should be processed.

    Returns:
        A list of matched file paths.
    """
    raw_files = [
        file_path
        for file_path in raw_dir.glob("**/*.csv")
        if check_if_parser_applies(file_path)
    ]
    logger.info(f"Detected {len(raw_files):_} raw files @ {raw_dir=}.")
    return raw_files


def detect_new_raw_files(
    raw_dir: Path,
    check_if_parser_applies: Callable,
    parsed_dir: Path,
    provider: str,
    service: str,
) -> list[Path]:
    logger.info(f"Detecting new raw files for {provider=} -> {service=}")

    raw_files = detect_raw_files(raw_dir, check_if_parser_applies)
    logger.info(f"Found {len(raw_files):_d} raw files in {raw_dir=}")

    present_parsed_files = detect_present_parsed_files(parsed_dir)
    logger.info(f"Found {len(present_parsed_files):_d} matching files in {parsed_dir=}")

    new_files_to_parse = select_files_to_parse(present_parsed_files, raw_files)
    logger.info(f"Hence found {len(new_files_to_parse):_d} new files to parse")

    logger.info(f"Finished detecting files to be parsed for {provider=} -> {service=}")
    return new_files_to_parse


def detect_relevant_target_files(raw_dir: Path) -> list[Path]:
    """Detects relevant raw files in the given target directory."""
    relevant_target_files = [file_path for file_path in raw_dir.glob("**/*.csv")]
    logger.info(
        f"Detected {len(relevant_target_files):_} relevant source files @ {raw_dir=}."
    )
    return relevant_target_files


def detect_relevant_source_files(
    source_dir: Path, check_if_parser_applies: Callable
) -> list[Path]:
    """Detects relevant CSV files in the given source directory.

    Args:
        source_dir: The directory to search for source files.
        check_if_parser_applies: A callable that takes a file path and returns True
            if the file is relevant.

    Returns:
        A list of matched source file paths.
    """
    relevant_source_files = [
        file_path
        for file_path in source_dir.glob("**/*.csv")
        if check_if_parser_applies(file_path)
    ]
    logger.info(
        f"Detected {len(relevant_source_files):_} relevant source files @ {source_dir=}."
    )
    return relevant_source_files


def get_parser_source_files(
    case: Case, config: Config, check_if_parser_applies: Callable
) -> list[Path]:
    source_dir = config.get_source_dir(case.provider, case.service)
    relevant_source_files = detect_relevant_source_files(
        source_dir, check_if_parser_applies
    )
    return relevant_source_files
