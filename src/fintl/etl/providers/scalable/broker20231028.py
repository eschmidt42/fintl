"""Scalable broker account parser for HTML files from 2023-10-28 onwards (broker20231028)."""

import datetime
import logging
import re
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup, element

from fintl.common import Case, Config
from fintl.etl.common.schemas import (
    BalanceInfo,
    ProviderEnum,
    ScalableBrokerParserEnum,
    ServiceEnum,
)
from fintl.etl.engine import parse_utils
from fintl.etl.io.files.copy import copy_new_files
from fintl.etl.io.files.detect import detect_encoding
from fintl.etl.io.files.orchestrator import (
    update_history,
)
from fintl.etl.io.files.select import select_files_to_copy
from fintl.etl.io.files.utils import (
    load_lines,
)
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
    parser=ScalableBrokerParserEnum.broker20231028.value,
)


def check_if_parser_applies(file_path: Path) -> bool:
    """Return True if this parser handles the given file."""
    pattern_result = re.search(r"^(\d{4}-\d{2}-\d{2}\.html?)$", str(file_path.name))
    is_file_name_match = pattern_result is not None

    is_content_match = False
    if is_file_name_match:
        date = re.search(r"(\d{4})-(\d{2})-(\d{2})", file_path.name)
        if date is None:
            raise ValueError(f"{date=} is None but should be a regex match.")
        date = [int(v) for v in date.groups()]
        date = datetime.date(date[0], date[1], date[2])
        is_file_name_match = date >= datetime.date(2023, 10, 28)

        with file_path.open("r") as f:
            lines = f.readlines()

        is_content_match = any("€" in line for line in lines)

    return is_file_name_match and is_content_match


def extract_balance(
    case: Case,
    file_path: Path,
    lines: list[str],
) -> BalanceInfo:
    """Extract balance information from parsed file."""
    with file_path.open("r") as f:
        soup = BeautifulSoup(f, "html.parser")

    tag = soup.find("div", {"data-testid": "product-list-item"})

    if not isinstance(tag, element.Tag):
        raise ValueError

    div = [v for v in tag.find_all("div") if "€" in v.text][-1]
    text = div.text
    num, currency = text.split()

    amount = float(num.strip().replace(",", ""))
    currency = currency.strip()

    # date from the file name
    date = file_path.stem.split("-")
    date = [int(v) for v in date]
    date = datetime.date(date[0], date[1], date[2])

    return BalanceInfo(
        date=date,
        amount=amount,
        currency=currency,
        provider=case.provider,
        service=case.service,
        parser=case.parser,
        file=str(file_path),
    )


def parse_html_file(case: Case, file_path: Path) -> tuple[pl.DataFrame, BalanceInfo]:
    """Parse a single file and return transactions and balance."""
    encoding = detect_encoding(file_path)
    logger.debug(f"{file_path=} has {encoding=}")

    lines = load_lines(file_path, encoding)
    transactions = extract_transactions()
    balance = extract_balance(case, file_path, lines)

    return transactions, balance


def parse_new_files(
    case: Case,
    new_files_to_parse: list[Path],
    parsed_dir: Path,
):
    """Parse all newly discovered files for this account type."""
    return parse_utils.parse_new_files(
        case,
        new_files_to_parse,
        parsed_dir,
        parse_fn=parse_html_file,
        store_transactions_fn=store_transactions,
        store_balance_fn=store_balance,
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
    parse_new_files(CASE, new_files_to_parse, parsed_dir)

    # extend pre-existing parquets for this parser
    parser_dir = config.get_parser_dir(CASE)
    update_history(parser_dir, parsed_dir, new_files_to_parse)

    logger.info(f"Done processing {CASE=}")
