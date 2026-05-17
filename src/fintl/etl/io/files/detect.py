"""Helpers for discovering raw, parsed, and source files on disk."""

import logging
import re
from pathlib import Path
from typing import Callable

import chardet
import polars as pl

from fintl.etl.io.files.select import select_files_to_parse

logger = logging.getLogger(__name__)


def detect_present_parsed_files(parsed_dir: Path) -> list[Path]:
    """Detects existing parsed files by globbing the target directory for .xlsx files.

    Args:
        parsed_dir: The directory to search for parsed files.

    Returns:
        A list of paths to existing parsed .xlsx files.
    """
    present_parsed_files = list(parsed_dir.glob("**/*.xlsx"))
    logger.info(f"Detected {len(present_parsed_files):_} present parsed files @ {parsed_dir=}.")
    return present_parsed_files


def detect_new_parsed_files(
    raw_dir: Path,
    parser_dir: Path,
    parsed_dir: Path,
) -> list[Path]:
    """Identifies newly parsed balance files that haven't been stored yet.

    Compares parsed balance parquet files against the existing balance history
    to determine which ones are new.

    Args:
        raw_dir: Directory containing the raw CSV files.
        parser_dir: Directory containing the balance history parquet file.
        parsed_dir: Directory containing the parsed parquet files.

    Returns:
        A list of paths to new balance CSV files corresponding to newly parsed data.
    """
    logger.info("Detecting newly parsed files")

    available_parsed_balance_files = list(parsed_dir.glob("*-balance.parquet"))

    all_balances_parquet_path = parser_dir / "balances.parquet"

    if all_balances_parquet_path.exists():
        all_balances = pl.read_parquet(all_balances_parquet_path)

        already_stored_files = all_balances["file"].unique().to_list()

        already_stored_files = {Path(f).stem for f in already_stored_files}
    else:
        already_stored_files = set()

    n = len("-balance.parquet")
    newly_parsed_parquets = [
        f for f in available_parsed_balance_files if f.name[:-n] not in already_stored_files
    ]

    newly_parsed_csv_files = [raw_dir / f"{f.name[:-n]}.csv" for f in newly_parsed_parquets]
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
        file_path for file_path in raw_dir.glob("**/*.csv") if check_if_parser_applies(file_path)
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
    """Identifies new raw CSV files that need to be parsed.

    Finds CSV files in the raw directory that match the parser criteria and
    are not yet represented in the parsed directory.

    Args:
        raw_dir: The directory to search for raw CSV files.
        check_if_parser_applies: A callable that validates if a file matches
            the parser's criteria.
        parsed_dir: Directory containing already processed parsed files.
        provider: The financial provider name.
        service: The financial service name.

    Returns:
        A list of paths to new raw files that need to be parsed.
    """
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
    relevant_target_files = list(raw_dir.glob("**/*.csv"))
    logger.info(f"Detected {len(relevant_target_files):_} relevant source files @ {raw_dir=}.")
    return relevant_target_files


def detect_relevant_source_files(source_dir: Path, check_if_parser_applies: Callable) -> list[Path]:
    """Detects relevant CSV files in the given source directory.

    Args:
        source_dir: The directory to search for source files.
        check_if_parser_applies: A callable that takes a file path and returns True
            if the file is relevant.

    Returns:
        A list of matched source file paths.
    """
    relevant_source_files = [
        file_path for file_path in source_dir.glob("**/*.csv") if check_if_parser_applies(file_path)
    ]
    logger.info(f"Detected {len(relevant_source_files):_} relevant source files @ {source_dir=}.")
    return relevant_source_files


def detect_encoding(path: Path, encoding_default: str = "utf-8") -> str:
    """Detects the file encoding using chardet.

    Args:
        path: Path to the file to detect encoding for.
        encoding_default: Fallback encoding if detection fails.

    Returns:
        The detected encoding string, or the default if detection fails.
    """
    with open(path, "rb") as f:
        res = chardet.detect(f.read())
    logger.debug(f"detect encoding: {res}")
    enc = res["encoding"]
    if enc is None:
        logger.warning(f"Failed to detect encoding, defaulting to {encoding_default}")
        enc = encoding_default
    return enc


def is_match(pattern: str, x: str) -> bool:
    """Checks if a string matches a given regex pattern.

    Args:
        pattern: The regex pattern to match against.
        x: The string to test.

    Returns:
        True if the pattern matches the string, False otherwise.
    """
    return re.search(pattern, x) is not None


def find_line_with_pattern(lines: list[str], pattern: str) -> tuple[int, str]:
    """Finds the first line in a list that matches a given pattern.

    Args:
        lines: A list of strings to search through.
        pattern: The regex pattern to match.

    Returns:
        A tuple containing the line index (0-based) and the matched line string.

    Raises:
        ValueError: If no line matches the pattern.
    """
    ix_match = None
    matched_line = ""
    for i, line in enumerate(lines):
        if is_match(pattern, line):
            ix_match = i
            matched_line = line
            break

    if ix_match is None:
        logger.warning(f"Could not find line matching {pattern=}")

    if ix_match is None:
        raise ValueError(
            f"Unexpectedly failed to find the first index with {pattern=} in {lines[:10]=}"
        )

    return ix_match, matched_line
