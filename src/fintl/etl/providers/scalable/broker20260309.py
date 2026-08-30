"""Scalable broker account parser for PNG screenshots from 2026-03-09 onwards (broker20260309)."""

import datetime
import logging
import re
from pathlib import Path

import polars as pl

from fintl.common import Case, Config
from fintl.common.extraction.availability import check_llama_swap_ok, check_ollama_ok
from fintl.common.extraction.constants import ModelProvider
from fintl.common.extraction.errors import (
    InferenceError,
)
from fintl.common.extraction.llama_swap import (
    LlamaSwapExtractionModel,
)
from fintl.common.extraction.ollama import (
    OllamaExtractionModel,
)
from fintl.common.extraction.unload import unload_llama_swap, unload_ollama
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


def extract_balance(case: Case, file_path: Path, config: Config) -> BalanceInfo:
    """Extract balance information from a PNG screenshot using ollama."""
    match config.model_provider:
        case ModelProvider.ollama:
            estimator = OllamaExtractionModel(
                config.ollama.model,  # ty: ignore[unresolved-attribute]
                base_url=config.ollama.base_url,  # ty: ignore[unresolved-attribute]
                timeout=config.model_timeout,
            )
        case ModelProvider.llama_swap:
            estimator = LlamaSwapExtractionModel(
                config.llama_swap.model,  # ty: ignore[unresolved-attribute]
                base_url=config.llama_swap.base_url,  # ty: ignore[unresolved-attribute]
                timeout=config.model_timeout,
            )

    _o = estimator.predict(file_path)

    if _o.ok:
        if _o.completion is None:
            msg = "_completion is unexpectedly None"
            raise ValueError(msg)

        elif _o.completion.usage is None:
            msg = "_completion.usage is unexpectedly None"
            raise ValueError(msg)

        if _o.completion.usage.completion_tokens_details is None:
            msg = "_completion.usage.completion_tokens_details is unexpectedly None"
            raise ValueError(msg)
    else:
        raise InferenceError(_o.error_message)

    # date from the file name
    date = get_date_from_string(file_path.name)

    return BalanceInfo(
        date=date,
        amount=_o.extraction.amount,  # ty: ignore[unresolved-attribute]
        currency=_o.extraction.currency,  # ty: ignore[unresolved-attribute]
        provider=case.provider,
        service=case.service,
        parser=case.parser,
        file=str(file_path),
    )


def parse_image_file(
    case: Case, file_path: Path, config: Config
) -> tuple[pl.DataFrame, BalanceInfo]:
    """Parse a single PNG file and return transactions and balance."""
    transactions = extract_transactions()
    balance = extract_balance(case, file_path, config=config)

    return transactions, balance


def parse_new_files(
    case: Case,
    new_files_to_parse: list[Path],
    parsed_dir: Path,
    *,
    config: Config,
) -> list[Path]:
    """Parse PNG files and return the list of files that were successfully parsed."""
    if not new_files_to_parse:
        logger.info("No new files to parse")
        return []

    match config.model_provider:
        case ModelProvider.ollama:
            unload_llama_swap(config.llama_swap, config.model_timeout)
            if not check_ollama_ok(config.ollama):
                return []
        case ModelProvider.llama_swap:
            unload_ollama(config.ollama, config.model_timeout)
            if not check_llama_swap_ok(config, do_inference_check=True):
                return []

    return parse_utils.parse_new_files(
        case,
        new_files_to_parse,
        parsed_dir,
        parse_fn=lambda c, path: parse_image_file(c, path, config),
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
    actually_parsed = parse_new_files(CASE, new_files_to_parse, parsed_dir, config=config)

    # extend pre-existing parquets for this parser
    parser_dir = config.get_parser_dir(CASE)
    update_history(parser_dir, parsed_dir, actually_parsed)

    logger.info(f"Done processing {CASE=}")
