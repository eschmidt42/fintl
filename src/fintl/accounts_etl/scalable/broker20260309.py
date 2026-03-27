import datetime
import logging
import re
from pathlib import Path

import httpx
import instructor
import polars as pl
from instructor.processing.multimodal import Image
from pydantic import BaseModel

from fintl.accounts_etl.file_helper import concatenate_new_information_to_history
from fintl.accounts_etl.files import copy_new_files, select_files_to_copy
from fintl.accounts_etl.scalable.broker0 import extract_transactions
from fintl.accounts_etl.scalable.files import (
    detect_new_raw_files,
    detect_relevant_target_files,
    get_parser_source_files,
    store_balance,
    store_transactions,
)
from fintl.accounts_etl.schemas import (
    BalanceInfo,
    Case,
    Config,
    OllamaConfig,
    ProviderEnum,
    ScalableBrokerParserEnum,
    ServiceEnum,
)

logger = logging.getLogger(__name__)

CASE = Case(
    provider=ProviderEnum.scalable.value,
    service=ServiceEnum.broker.value,
    parser=ScalableBrokerParserEnum.broker20260309.value,
)


class OllamaUnavailableError(Exception):
    """Raised when the ollama server cannot be reached."""


class OllamaModelUnavailableError(Exception):
    """Raised when the requested model is not present in the ollama instance."""


def check_if_parser_applies(file_path: Path) -> bool:
    "Example: Screenshot 2026-03-02 at 14.30.53.png"
    pattern_result = re.search(
        r"^Screenshot \d{4}-\d{2}-\d{2}.*\.png$", str(file_path.name)
    )
    is_file_name_match = pattern_result is not None

    return is_file_name_match


class _BalanceInfoExtract(BaseModel):
    amount: float
    currency: str


def get_date_from_string(name: str) -> datetime.date:
    date_match = re.match(r"^Screenshot (\d{4}-\d{2}-\d{2}).*\.png$", name)
    if date_match:
        date = date_match.group(1)
        date = [int(v) for v in date.split("-")]
        date = datetime.date(date[0], date[1], date[2])
        return date
    else:
        raise ValueError(f"Could not extract date from {name=}")


def _check_ollama_availability(base_url: str) -> None:
    """Check that the ollama server is reachable.

    Strips the ``/v1`` suffix (if present) to reach the ollama root endpoint
    and performs a GET with a short timeout.

    Raises:
        OllamaUnavailableError: when the server cannot be reached.
    """
    root_url = base_url.rstrip("/")
    if root_url.endswith("/v1"):
        root_url = root_url[:-3]
    try:
        httpx.get(root_url, timeout=5.0).raise_for_status()
    except Exception as exc:
        raise OllamaUnavailableError(
            f"Ollama is not reachable at {base_url}: {exc}"
        ) from exc


def _check_model_available(base_url: str, model: str) -> None:
    """Check that *model* has been pulled into the local ollama instance.

    Calls ``GET {root}/api/tags`` and inspects the returned model list.
    Model names returned by ollama may include a tag suffix (e.g. ``":latest"``);
    if *model* contains no ``:``, a bare-name match against the part before
    ``:`` is also accepted.

    Raises:
        OllamaModelUnavailableError: when the model is not found.
    """
    root_url = base_url.rstrip("/")
    if root_url.endswith("/v1"):
        root_url = root_url[:-3]
    try:
        response = httpx.get(f"{root_url}/api/tags", timeout=5.0)
        response.raise_for_status()
        available = [m["name"] for m in response.json().get("models", [])]
    except Exception as exc:
        raise OllamaModelUnavailableError(
            f"Could not retrieve model list from ollama at {base_url}: {exc}"
        ) from exc

    # exact match first; then fall back to bare-name match when model has no tag
    if model in available:
        return
    if ":" not in model:
        bare_names = {m.split(":")[0] for m in available}
        if model in bare_names:
            return

    raise OllamaModelUnavailableError(
        f"Model '{model}' is not available in ollama. "
        f"Pull it first with: ollama pull {model}"
    )


def _get_ollama_client(
    *, model: str, ollama_base_url: str = "http://localhost:11434/v1"
) -> instructor.Instructor:
    return instructor.from_provider(
        f"ollama/{model}",
        base_url=ollama_base_url,
        mode=instructor.Mode.JSON,
        async_client=False,
    )


_SYSTEM_PROMPT = (
    "You are a Scraper for data contained in a screenshot of a broker web app."
)


def _get_lm_extraction(
    file_path: Path, extraction_client: instructor.Instructor
) -> _BalanceInfoExtract:
    return extraction_client.create(  # type: ignore
        response_model=_BalanceInfoExtract,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    "Please extract data from the following image",
                    Image.from_path(file_path),
                ],
            },  # type: ignore[arg-type]
        ],
    )


def extract_balance(
    case: Case, file_path: Path, *, ollama_config: OllamaConfig
) -> BalanceInfo:
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
    transactions = extract_transactions()
    balance = extract_balance(case, file_path, ollama_config=ollama_config)

    return transactions, balance


def parse_new_files(
    case: Case,
    new_files_to_parse: list[Path],
    parsed_dir: Path,
    *,
    ollama_config: OllamaConfig | None,
):
    if len(new_files_to_parse) == 0:
        logger.info("No new files to parse")
        return

    if ollama_config is None:
        logger.warning(
            "Ollama is not configured. Skipping PNG parsing for %d file(s).",
            len(new_files_to_parse),
        )
        return

    try:
        _check_ollama_availability(ollama_config.base_url)
    except OllamaUnavailableError as exc:
        logger.warning("Ollama is not available, aborting PNG parsing: %s", exc)
        return

    try:
        _check_model_available(ollama_config.base_url, ollama_config.model)
    except OllamaModelUnavailableError as exc:
        logger.warning(
            "Ollama model (%s) not available, aborting PNG parsing: %s",
            ollama_config.model,
            exc,
        )
        return

    if not parsed_dir.exists():
        logger.info(f"Creating {parsed_dir=}")
        parsed_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Parsing {len(new_files_to_parse):_} new files to {parsed_dir=}")

    for file_path in new_files_to_parse:
        logger.debug(f"Parsing {file_path=} to {parsed_dir=}")
        try:
            transactions, balance = parse_image_file(
                case, file_path, ollama_config=ollama_config
            )
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", file_path.name, exc)
            continue

        store_transactions(parsed_dir, file_path, transactions)
        store_balance(parsed_dir, file_path, balance)

    logger.info(f"Finished parsing {len(new_files_to_parse):_d} new files")


def main(config: Config):
    logger.info(f"Processing {CASE=}")

    # scan source files
    relevant_source_files = get_parser_source_files(
        CASE, config, check_if_parser_applies
    )

    # scan target files
    raw_dir = config.get_raw_dir(CASE)
    relevant_target_files = detect_relevant_target_files(raw_dir)

    # select new source files to be processed
    new_files_to_copy = select_files_to_copy(
        relevant_source_files, relevant_target_files
    )

    # copy new source files
    copy_new_files(raw_dir, new_files_to_copy)

    # detect new raw files
    parsed_dir = config.get_parsed_dir(CASE)
    new_files_to_parse = detect_new_raw_files(
        raw_dir, check_if_parser_applies, parsed_dir, CASE.provider, CASE.service
    )

    # parse new files to parquet -> transactions & balance
    parse_new_files(CASE, new_files_to_parse, parsed_dir, ollama_config=config.ollama)

    # extend pre-existing parquets for this parser
    parser_dir = config.get_parser_dir(CASE)
    concatenate_new_information_to_history(parser_dir, parsed_dir, new_files_to_parse)

    logger.info(f"Done processing {CASE=}")
