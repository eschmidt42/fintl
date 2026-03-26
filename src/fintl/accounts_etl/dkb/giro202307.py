import datetime
import logging
import re
import typing as T
from pathlib import Path

import polars as pl

from fintl.accounts_etl.file_helper import (
    concatenate_new_information_to_history,
    detect_new_raw_files,
    detect_relevant_target_files,
    get_parser_source_files,
    store_balance,
    store_transactions,
)
from fintl.accounts_etl.files import copy_new_files, load_lines, select_files_to_copy
from fintl.accounts_etl.schemas import (
    HASH_COLUMNS,
    TRANSACTION_COLUMNS,
    BalanceInfo,
    Case,
    Config,
    DKBGiroParserEnum,
    ProviderEnum,
    ServiceEnum,
)
from fintl.accounts_etl.utils import (
    detect_encoding,
    find_line_with_pattern,
    german_string_numbers_to_floats,
    hash_transactions,
    verify_transactions,
)

logger = logging.getLogger(__name__)

CASE = Case(
    provider=ProviderEnum.dkb.value,
    service=ServiceEnum.giro.value,
    parser=DKBGiroParserEnum.giro202307.value,
)


def check_if_parser_applies(file_path: Path) -> bool:
    is_file_name_match = re.search(r"(DE\d{20}\.csv$)", str(file_path.name)) is not None

    encoding = detect_encoding(file_path)
    lines = load_lines(file_path, encoding)
    is_header_match = any(
        re.search(r'("Umsatztyp";"Betrag";"Glä)', line) for line in lines
    )

    return is_file_name_match and is_header_match


def extract_transactions(
    case: Case, file_path: Path, lines: T.List[str], encoding: str
) -> pl.DataFrame:
    transaction_pattern: str = '^("?Buchungsdatum)'

    date_format: str = "%d.%m.%y"
    date_cols: list = ["Buchungsdatum"]

    ix_start_transactions, transactions_header = find_line_with_pattern(
        lines, pattern=transaction_pattern
    )
    logger.debug(
        f"{file_path=} has {ix_start_transactions=} and {transactions_header=}"
    )

    schema = {
        "Buchungsdatum": pl.Utf8,
        "Wertstellung": pl.Utf8,
        "Status": pl.Utf8,
        "Zahlungspflichtige*r": pl.Utf8,
        "Zahlungsempfänger*in": pl.Utf8,
        "Verwendungszweck": pl.Utf8,
        "Umsatztyp": pl.Utf8,
        "Betrag": pl.Utf8,
        "Gläubiger-ID": pl.Utf8,
        "Mandatsreferenz": pl.Utf8,
        "Kundenreferenz": pl.Utf8,
    }
    transactions = pl.read_csv(
        file_path,
        skip_rows=ix_start_transactions,
        separator=";",
        truncate_ragged_lines=True,
        encoding=encoding,
        schema=schema,
    )
    transactions = transactions.with_columns(
        [pl.col(col).str.to_date(date_format) for col in date_cols],
    )

    transactions = transactions.with_columns(
        pl.col("Betrag")
        .str.replace("€", "")
        .str.strip_chars_end()
        .map_elements(german_string_numbers_to_floats, return_dtype=pl.Float64),
    )
    transactions = transactions.with_columns(
        amount=pl.col("Betrag"),
        description=pl.col("Verwendungszweck"),
        date=pl.col("Buchungsdatum"),
        source=pl.when(pl.col("Betrag") > 0)
        .then(pl.col("Zahlungspflichtige*r"))
        .otherwise(pl.lit("myself")),
        recipient=pl.when(pl.col("Betrag") < 0)
        .then(pl.col("Zahlungsempfänger*in"))
        .otherwise(pl.lit("myself")),
        provider=pl.lit(case.provider),
        service=pl.lit(case.service),
        parser=pl.lit(case.parser),
        file=pl.lit(str(file_path)),
    )
    transactions = hash_transactions(transactions, HASH_COLUMNS)

    verify_transactions(TRANSACTION_COLUMNS, transactions, file_path)

    # col_order = list(TRANSACTION_COLUMNS)
    # col_order += [col for col in transactions.columns if col not in col_order]
    transactions = transactions.select(TRANSACTION_COLUMNS)

    return transactions


def extract_balance(case: Case, file_path: Path, lines: T.List[str]) -> BalanceInfo:
    balance_info_pattern: str = '^("?Kontostand vom)'  # start of balance info
    ix_start_balance, balance_line = find_line_with_pattern(
        lines, pattern=balance_info_pattern
    )

    logger.debug(f"{file_path=} has {ix_start_balance=} and {balance_line=}")

    balance_line = balance_line.replace("\xa0", " ")
    pattern = r'"Kontostand vom (\d{2}\.\d{2}\.\d{4}):"[;|,]"([\d.*\.?]+,\d{2}) (\S+)"'
    match = re.search(pattern, balance_line)

    if match:
        date = match.group(1)
        amount = match.group(2)
        currency = match.group(3)
    else:
        raise ValueError(f"Could not match {balance_line=} using pattern {pattern=}")

    date = [int(v) for v in date.split(".")]
    date = datetime.date(date[2], date[1], date[0])
    amount = german_string_numbers_to_floats(amount)

    return BalanceInfo(
        date=date,
        amount=amount,
        currency=currency,
        provider=case.provider,
        service=case.service,
        parser=case.parser,
        file=str(file_path),
    )


def parse_csv_file(case: Case, file_path: Path) -> tuple[pl.DataFrame, BalanceInfo]:
    encoding = detect_encoding(file_path)
    logger.debug(f"{file_path=} has {encoding=}")

    lines = load_lines(file_path, encoding)
    transactions = extract_transactions(case, file_path, lines, encoding)
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
    concatenate_new_information_to_history(parser_dir, parsed_dir, new_files_to_parse)

    logger.info(f"Done processing {CASE=}")
