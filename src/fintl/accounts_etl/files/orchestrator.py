import logging
from pathlib import Path
from typing import Callable

from fintl.accounts_etl.common.schemas import Case, Config
from fintl.accounts_etl.files.balances import concatenate_balances_history
from fintl.accounts_etl.files.detect import detect_relevant_source_files
from fintl.accounts_etl.files.transactions import concatenate_transactions_history

logger = logging.getLogger(__name__)


def concatenate_new_information_to_history(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
) -> None:
    """Concatenates new parsed files to the existing transaction and balance history.

    Appends newly parsed data to the existing Parquet and Excel history files
    for both transactions and balances.

    Args:
        parser_dir: Directory containing the history files to update.
        parsed_dir: Directory containing the newly parsed files.
        new_files_to_parse: List of paths to the new files to append.

    Returns:
        None
    """
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
    """Retrieves relevant source CSV files for a given case.

    Determines the source directory from the config and filters files using
    the provided parser checker.

    Args:
        case: The case containing provider and service information.
        config: Application configuration object.
        check_if_parser_applies: A callable to validate if a file matches the parser.

    Returns:
        A list of paths to relevant source files.
    """
    source_dir = config.get_source_dir(case.provider, case.service)
    relevant_source_files = detect_relevant_source_files(
        source_dir, check_if_parser_applies
    )
    return relevant_source_files
