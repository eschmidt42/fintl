import logging
from pathlib import Path
from typing import Callable

import polars as pl

from fintl.accounts_etl.files import detect_present_parsed_files
from fintl.accounts_etl.schemas import BalanceInfo, Case, Config

logger = logging.getLogger(__name__)


def transaction_htm_name_to_xlsx(file: Path) -> str:
    return file.name.replace(file.suffix, "-transactions.xlsx")


def transaction_htm_name_to_parquet(file: Path) -> str:
    return file.name.replace(file.suffix, "-transactions.parquet")


def balance_htm_name_to_json(file: Path) -> str:
    return file.name.replace(file.suffix, "-balance.json")


def balance_htm_name_to_parquet(file: Path) -> str:
    return file.name.replace(file.suffix, "-balance.parquet")


def detect_relevant_source_files(
    source_dir: Path, check_if_parser_applies: Callable
) -> list[Path]:
    """Detects relevant raw files in the given source directory."""
    relevant_source_files = [
        file_path
        for pattern in ["**/*.htm", "**/*.html", "**/*.png"]
        for file_path in source_dir.glob(pattern)
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


def detect_relevant_target_files(raw_dir: Path) -> list[Path]:
    """Detects relevant raw files in the given target directory."""
    relevant_target_files = relevant_target_files = [
        file_path
        for pattern in ["**/*.htm", "**/*.html", "**/*.png"]
        for file_path in raw_dir.glob(pattern)
    ]
    logger.info(
        f"Detected {len(relevant_target_files):_} relevant source files @ {raw_dir=}."
    )
    return relevant_target_files


def detect_raw_files(raw_dir: Path, check_if_parser_applies: Callable) -> list[Path]:
    """Detects relevant raw files."""
    raw_files = [
        file_path
        for pattern in ["**/*.htm", "**/*.html", "**/*.png"]
        for file_path in raw_dir.glob(pattern)
        if check_if_parser_applies(file_path)
    ]
    logger.info(f"Detected {len(raw_files):_} raw files @ {raw_dir=}.")
    return raw_files


def select_files_to_parse(present_parsed_files: list[Path], raw_files: list[Path]):
    parsed_files = [file_path.name for file_path in present_parsed_files]
    files_to_parse = [
        file_path
        for file_path in raw_files
        if transaction_htm_name_to_xlsx(file_path) not in parsed_files
    ]
    logger.info(
        f"Selecting {len(files_to_parse):_} files to parse after comparing {len(present_parsed_files):_} present parsed files and {len(raw_files):_} raw files."
    )
    return files_to_parse


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


def store_transactions(parsed_dir: Path, file_path: Path, transactions: pl.DataFrame):
    excel_file = parsed_dir / transaction_htm_name_to_xlsx(file_path)
    logger.debug(f"Writing {excel_file=}")
    transactions.write_excel(excel_file)

    parquet_file = parsed_dir / transaction_htm_name_to_parquet(file_path)
    logger.debug(f"Writing {parquet_file=}")
    transactions.write_parquet(parquet_file)


def store_balance(parsed_dir: Path, file_path: Path, balance: BalanceInfo):
    json_file = parsed_dir / balance_htm_name_to_json(file_path)
    logger.debug(f"Writing {json_file=}")
    d = balance.model_dump_json(indent=4)
    with json_file.open("w") as f:
        f.write(d)

    parquet_file = parsed_dir / balance_htm_name_to_parquet(file_path)
    logger.debug(f"Writing {parquet_file=}")
    balance_df = pl.DataFrame([balance.model_dump()])
    balance_df.write_parquet(parquet_file)
