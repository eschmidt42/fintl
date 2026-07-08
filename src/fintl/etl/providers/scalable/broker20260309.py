"""Scalable broker account parser for PNG screenshots from 2026-03-09 onwards (broker20260309)."""

import datetime
import logging
import re
from pathlib import Path

import polars as pl

from fintl.common import Case, Config, OllamaConfig
from fintl.etl.common.schemas import (
    BalanceInfo,
    ProviderEnum,
    ScalableBrokerParserEnum,
    ServiceEnum,
)
from fintl.etl.engine import parse_utils
from fintl.etl.io.files.copy import copy_new_files
from fintl.etl.io.files.orchestrator import (
    update_history,
)
from fintl.etl.io.files.select import select_files_to_copy
from fintl.etl.providers.scalable.broker0 import extract_transactions
from fintl.etl.providers.scalable.extraction.errors import (
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)
from fintl.etl.providers.scalable.extraction.ollama import (
    _check_model_available,
    _check_ollama_availability,
    _get_lm_extraction,
    _get_ollama_client,
)
from fintl.etl.providers.scalable.files import (
    detect_new_raw_files,
    detect_relevant_target_files,
    get_parser_source_files,
    store_balance,
    store_transactions,
)

logger = logging.getLogger(__name__)

CASE = Case(
    provider=ProviderEnum.scalable.value,
    service=ServiceEnum.broker.value,
    parser=ScalableBrokerParserEnum.broker20260309.value,
)


def check_if_parser_applies(file_path: Path) -> bool:
    """Return True if the filename matches the expected screenshot pattern."""
    pattern_result = re.search(r"^Screenshot \d{4}-\d{2}-\d{2}.*\.png$", str(file_path.name))
    is_file_name_match = pattern_result is not None

    return is_file_name_match


def get_date_from_string(name: str) -> datetime.date:
    """Extract a date from a screenshot filename string."""
    date_match = re.match(r"^Screenshot (\d{4}-\d{2}-\d{2}).*\.png$", name)
    if date_match:
        date = date_match.group(1)
        date = [int(v) for v in date.split("-")]
        date = datetime.date(date[0], date[1], date[2])
        return date
    else:
        raise ValueError(f"Could not extract date from {name=}")


def extract_balance(case: Case, file_path: Path, *, ollama_config: OllamaConfig) -> BalanceInfo:
    """Extract balance information from a PNG screenshot using ollama."""
    extraction_client = _get_ollama_client(
        model=ollama_config.model, ollama_base_url=ollama_config.base_url
    )

    extraction = _get_lm_extraction(file_path, extraction_client)

    # date from the file name
    date = get_date_from_string(file_path.name)

    return BalanceInfo(
        date=date,
        amount=extraction.amount,
        currency=extraction.currency,
        provider=case.provider,
        service=case.service,
        parser=case.parser,
        file=str(file_path),
    )


def parse_image_file(
    case: Case, file_path: Path, *, ollama_config: OllamaConfig
) -> tuple[pl.DataFrame, BalanceInfo]:
    """Parse a single PNG file and return transactions and balance."""
    transactions = extract_transactions()
    balance = extract_balance(case, file_path, ollama_config=ollama_config)

    return transactions, balance


def parse_new_files(
    case: Case,
    new_files_to_parse: list[Path],
    parsed_dir: Path,
    *,
    ollama_config: OllamaConfig | None,
) -> list[Path]:
    """Parse PNG files and return the list of files that were successfully parsed."""
    if not new_files_to_parse:
        logger.info("No new files to parse")
        return []

    if ollama_config is None:
        logger.warning(
            "Ollama is not configured. Skipping PNG parsing for %d file(s).",
            len(new_files_to_parse),
        )
        return []

    try:
        _check_ollama_availability(ollama_config.base_url)
    except OllamaUnavailableError as exc:
        logger.warning("Ollama is not available, aborting PNG parsing: %s", exc)
        return []

    try:
        _check_model_available(ollama_config.base_url, ollama_config.model)
    except OllamaModelUnavailableError as exc:
        logger.warning(
            "Ollama model (%s) not available, aborting PNG parsing: %s",
            ollama_config.model,
            exc,
        )
        return []

    return parse_utils.parse_new_files(
        case,
        new_files_to_parse,
        parsed_dir,
        parse_fn=lambda c, path: parse_image_file(c, path, ollama_config=ollama_config),
        store_transactions_fn=store_transactions,
        store_balance_fn=store_balance,
        catch_errors=(Exception,),
        log_parse_errors=True,
    )


def main(config: Config):
    """Run the full ETL pipeline for this parser."""
    logger.info(f"Processing {CASE=}")

    # scan source files
    relevant_source_files = get_parser_source_files(CASE, config, check_if_parser_applies)

    # scan target files
    raw_dir = config.get_raw_dir(CASE)
    relevant_target_files = detect_relevant_target_files(raw_dir)

    # select new source files to be processed
    new_files_to_copy = select_files_to_copy(relevant_source_files, relevant_target_files)

    # copy new source files
    copy_new_files(raw_dir, new_files_to_copy)

    # detect new raw files
    parsed_dir = config.get_parsed_dir(CASE)
    new_files_to_parse = detect_new_raw_files(
        raw_dir, check_if_parser_applies, parsed_dir, CASE.provider, CASE.service
    )

    # parse new files to parquet -> transactions & balance
    actually_parsed = parse_new_files(
        CASE, new_files_to_parse, parsed_dir, ollama_config=config.ollama
    )

    # extend pre-existing parquets for this parser
    parser_dir = config.get_parser_dir(CASE)
    update_history(parser_dir, parsed_dir, actually_parsed)

    logger.info(f"Done processing {CASE=}")
