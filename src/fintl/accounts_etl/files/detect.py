import logging
from pathlib import Path
from typing import Callable

import polars as pl

from fintl.accounts_etl.files.select import select_files_to_parse

logger = logging.getLogger(__name__)


def detect_present_parsed_files(parsed_dir: Path) -> list[Path]:
    """Detects relevant parsed files."""
    present_parsed_files = [file_path for file_path in parsed_dir.glob("**/*.xlsx")]
    logger.info(
        f"Detected {len(present_parsed_files):_} present parsed files @ {parsed_dir=}."
    )
    return present_parsed_files


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
