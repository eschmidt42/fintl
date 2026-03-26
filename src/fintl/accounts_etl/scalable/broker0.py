import datetime
import logging
import re
import typing as T
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup, element

from fintl.accounts_etl.file_helper import concatenate_new_information_to_history
from fintl.accounts_etl.files import copy_new_files, load_lines, select_files_to_copy
from fintl.accounts_etl.scalable.files import (
    detect_new_raw_files,
    detect_relevant_target_files,
    get_parser_source_files,
    store_balance,
    store_transactions,
)
from fintl.accounts_etl.schemas import (
    TRANSACTION_COLUMNS,
    BalanceInfo,
    Case,
    Config,
    ProviderEnum,
    ScalableBrokerParserEnum,
    ServiceEnum,
)
from fintl.accounts_etl.utils import detect_encoding

logger = logging.getLogger(__name__)

CASE = Case(
    provider=ProviderEnum.scalable.value,
    service=ServiceEnum.broker.value,
    parser=ScalableBrokerParserEnum.broker0.value,
)


def check_if_parser_applies(file_path: Path) -> bool:
    pattern_result = re.search(r"^(\d{4}-\d{2}-\d{2}\.html?)$", str(file_path.name))
    is_file_name_match = pattern_result is not None

    is_content_match = False
    if is_file_name_match:
        date = re.search(r"(\d{4})-(\d{2})-(\d{2})", file_path.name)
        if date is None:
            raise ValueError(f"{date=} is None but should be a regex match.")
        date = [int(v) for v in date.groups()]
        date = datetime.date(date[0], date[1], date[2])
        is_file_name_match = date < datetime.date(2023, 10, 28)

        with file_path.open("r") as f:
            lines = f.readlines()

        is_content_match = any(["€" in line for line in lines])

    return is_file_name_match and is_content_match


def extract_transactions() -> pl.DataFrame:
    schema = {
        "date": pl.Date,
        "source": pl.Utf8,
        "recipient": pl.Utf8,
        "amount": pl.Float64,
        "description": pl.Utf8,
        "hash": pl.UInt64,
        "provider": pl.Utf8,
        "service": pl.Utf8,
        "parser": pl.Utf8,
        "file": pl.Utf8,
    }

    transactions = pl.DataFrame(schema=schema)

    transactions = transactions.select(TRANSACTION_COLUMNS)

    return transactions


def extract_balance(
    case: Case,
    file_path: Path,
    lines: T.List[str],
) -> BalanceInfo:
    with file_path.open("r") as f:
        soup = BeautifulSoup(f, "html.parser")

    # example for element containing euros, cents and currency
    # <div class="MuiGrid-root jss94 jss96 MuiGrid-container MuiGrid-wrap-xs-nowrap" style="font-size:56px" data-testid="large-price"><div class="MuiGrid-root jss91 jss92 jss99 MuiGrid-item" data-testid="formatted-number">1,234</div><div class="MuiGrid-root jss88 MuiGrid-item"><div class="MuiGrid-root jss93 MuiGrid-container MuiGrid-direction-xs-column MuiGrid-justify-content-xs-space-between"><div class="MuiGrid-root jss92 jss98 MuiGrid-item" data-testid="decimal">69</div><div class="MuiGrid-root jss95 jss92 MuiGrid-item" data-testid="suffix"><div class="jss98">€</div></div></div></div></div>
    # element = soup.find(
    #     "div",
    #     class_="MuiGrid-root jss94 jss96 MuiGrid-container MuiGrid-wrap-xs-nowrap",
    # )

    # euros
    # example: <div class="MuiGrid-root jss91 jss92 jss99 MuiGrid-item" data-testid="formatted-number">1,234</div>
    tag = soup.find("div", {"data-testid": "large-price"})
    if not isinstance(tag, element.Tag) or tag.div is None or tag.div.string is None:
        raise ValueError
    val_before_decimal = tag.div.string

    # cents
    # example: <div class="MuiGrid-root jss92 jss98 MuiGrid-item" data-testid="decimal">69</div>
    tag = soup.find("div", {"data-testid": "decimal"})
    if not isinstance(tag, element.Tag):
        raise ValueError
    val_after_decimal = tag.string

    # currency
    # example: <div class="MuiGrid-root jss95 jss92 MuiGrid-item" data-testid="suffix"><div class="jss98">€</div></div>
    tag = soup.find("div", {"data-testid": "suffix"})
    if not isinstance(tag, element.Tag) or tag.div is None:
        raise ValueError

    currency = tag.div.string
    if currency is None:
        currency = ""

    val_before_decimal = val_before_decimal.replace(",", "").strip()
    amount = f"{val_before_decimal}.{val_after_decimal}"
    amount = float(amount)

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
    if len(new_files_to_parse) == 0:
        logger.info("No new files to parse")
        return

    if not parsed_dir.exists():
        logger.info(f"Creating {parsed_dir=}")
        parsed_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Parsing {len(new_files_to_parse):_} new files to {parsed_dir=}")

    for file_path in new_files_to_parse:
        logger.debug(f"Parsing {file_path=} to {parsed_dir=}")
        transactions, balance = parse_html_file(case, file_path)

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
    concatenate_new_information_to_history(parser_dir, parsed_dir, new_files_to_parse)

    logger.info(f"Done processing {CASE=}")
