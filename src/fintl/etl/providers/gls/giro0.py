import logging
from pathlib import Path

from fintl.common import Case, Config
from fintl.etl.common.schemas import (
    GLSGiroParserEnum,
    ProviderEnum,
    ServiceEnum,
)
from fintl.etl.io.files.balances import store_balance
from fintl.etl.io.files.copy import (
    copy_new_files,
)
from fintl.etl.io.files.detect import (
    detect_new_parsed_files,
    detect_new_raw_files,
    detect_relevant_target_files,
)
from fintl.etl.io.files.orchestrator import (
    concatenate_new_information_to_history,
    get_parser_source_files,
)
from fintl.etl.io.files.select import select_files_to_copy
from fintl.etl.io.files.transactions import store_transactions
from fintl.etl.providers.gls.helper import (
    check_if_parser_applies,
    parse_csv_file,
)

logger = logging.getLogger(__name__)

CASE = Case(
    provider=ProviderEnum.gls.value,
    service=ServiceEnum.giro.value,
    parser=GLSGiroParserEnum.giro0.value,
)


def parse_new_files(
    case: Case,
    new_files_to_parse: list[Path],
    parsed_dir: Path,
):
    if len(new_files_to_parse) == 0:
        logger.info("No new files to parse")
        return

    if not parsed_dir.exists():
        logger.info(f"Creating {parsed_dir=}")
        parsed_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Parsing {len(new_files_to_parse):_} new files to {parsed_dir=}")

    for file_path in new_files_to_parse:
        logger.debug(f"Parsing {file_path=} to {parsed_dir=}")
        transactions, balance = parse_csv_file(case, file_path)

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
    parse_new_files(CASE, new_files_to_parse, parsed_dir)

    # extend pre-existing parquets for this parser
    parser_dir = config.get_parser_dir(CASE)
    new_parsed_files = detect_new_parsed_files(raw_dir, parser_dir, parsed_dir)
    concatenate_new_information_to_history(parser_dir, parsed_dir, new_parsed_files)

    logger.info(f"Done processing {CASE=}")
