"""Utilities for copying new files into the raw data directory."""

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def copy_new_files(raw_dir: Path, new_files_to_copy: list[Path]) -> None:
    """Copies new files from the parsed directory to the raw directory.

    Args:
        raw_dir: The target directory where files will be copied.
        new_files_to_copy: List of file paths to copy.

    Returns:
        None
    """
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
