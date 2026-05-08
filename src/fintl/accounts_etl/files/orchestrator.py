import logging
from pathlib import Path
from typing import Callable

from fintl.accounts_etl.files.balances import concatenate_balances_history
from fintl.accounts_etl.files.detect import detect_relevant_source_files
from fintl.accounts_etl.files.transactions import concatenate_transactions_history
from fintl.accounts_etl.schemas import Case, Config

logger = logging.getLogger(__name__)


def concatenate_new_information_to_history(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
):
    "Concatenates new files to history / old files in data/{provider}/{service}/{parser}/{transactions,balances}.{xlsx,parquet}"
    logger.info("Concatenating new information to history")

    if len(new_files_to_parse) == 0:
        logger.info("There were no new files parsed, returning.")
        return

    concatenate_transactions_history(parser_dir, parsed_dir, new_files_to_parse)

    concatenate_balances_history(parser_dir, parsed_dir, new_files_to_parse)

    logger.info("Done concatenating information to history")


def get_parser_source_files(
    case: Case, config: Config, check_if_parser_applies: Callable
) -> list[Path]:
    source_dir = config.get_source_dir(case.provider, case.service)
    relevant_source_files = detect_relevant_source_files(
        source_dir, check_if_parser_applies
    )
    return relevant_source_files
