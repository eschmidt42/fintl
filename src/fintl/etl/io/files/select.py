"""Helpers for selecting files that still need to be copied or parsed."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def select_files_to_copy(source_files: list[Path], target_files: list[Path]) -> list[Path]:
    """Selects source files that do not yet exist in the target directory.

    Compares source files against already present target files by filename and returns
    only those that are not already copied.

    Args:
        source_files: List of source file paths to check.
        target_files: List of target file paths to compare against.

    Returns:
        A list of source file paths that need to be copied.
    """
    target_names = [file_path.name for file_path in target_files]
    files_to_copy = [file_path for file_path in source_files if file_path.name not in target_names]
    logger.info(
        f"Selecting {len(files_to_copy):_} files to copy comparing {len(source_files):_} source files and {len(target_files):_} target files."  # noqa: E501
    )
    return files_to_copy


def select_files_to_parse(present_parsed_files: list[Path], raw_files: list[Path]) -> list[Path]:
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
        f"Selecting {len(files_to_parse):_} files to parse after comparing {len(present_parsed_files):_} present parsed files and {len(raw_files):_} raw files."  # noqa: E501
    )
    return files_to_parse
