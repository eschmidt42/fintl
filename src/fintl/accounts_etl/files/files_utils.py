import logging
import shutil
import typing as T
from pathlib import Path

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
