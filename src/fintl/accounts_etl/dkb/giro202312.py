import logging
import re
import typing as T
from pathlib import Path

import polars as pl

from fintl.accounts_etl.dkb.giro202307 import extract_balance
from fintl.accounts_etl.exceptions import (
    ExtractBalanceException,
    ExtractTransactionsException,
)
from fintl.accounts_etl.file_helper import (
    concatenate_new_information_to_history,
    detect_new_parsed_files,
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
    parser=DKBGiroParserEnum.giro202312.value,
)


def detect_separator(lines: list[str]) -> str | None:
    separator = None
    is_header_match_semicolon = any(
        re.search(r'(yp";"IBAN";"Betrag \(€\)";"Glä)', line) for line in lines
    )
    logger.debug(f"{is_header_match_semicolon=}")
    if is_header_match_semicolon:
        separator = ";"

    is_header_match_comma = any(
        re.search(r'(yp","IBAN","Betrag \(€\)","Glä)', line) for line in lines
    )
    logger.debug(f"{is_header_match_comma=}")
    if is_header_match_comma:
        separator = ","

    return separator


def check_if_parser_applies(file_path: Path) -> bool:
    is_file_name_match = re.search(r"(DE\d{20}\.csv$)", str(file_path.name)) is not None
    logger.debug(f"{is_file_name_match=}")

    # check if the csv file at file_path contains "Betrag (€)"
    encoding = detect_encoding(file_path)
    lines = load_lines(file_path, encoding)

    separator = detect_separator(lines)
    is_expected_separator = separator is not None and separator in [",", ";"]
    return is_file_name_match and is_expected_separator


def extract_transactions(
    case: Case, file_path: Path, lines: T.List[str], encoding: str
) -> pl.DataFrame:
    transaction_pattern: str = '^("?Buchungsdatum)'  # start of transactions

    date_format: str = "%d.%m.%y"
    date_cols: list = ["Buchungsdatum"]

    ix_start_transactions, transactions_header = find_line_with_pattern(
        lines, pattern=transaction_pattern
    )
    is_empty_1st_line = len(lines[0].strip()) == 0
    logger.debug(
        f"{file_path=} ({is_empty_1st_line=}) has {ix_start_transactions=} and {transactions_header=}"
    )

    schema = {
        "Buchungsdatum": pl.Utf8,
        "Wertstellung": pl.Utf8,
        "Status": pl.Utf8,
        "Zahlungspflichtige*r": pl.Utf8,
        "Zahlungsempfänger*in": pl.Utf8,
        "Verwendungszweck": pl.Utf8,
        "Umsatztyp": pl.Utf8,
        "IBAN": pl.Utf8,
        "Betrag": pl.Utf8,
        "Gläubiger-ID": pl.Utf8,
        "Mandatsreferenz": pl.Utf8,
        "Kundenreferenz": pl.Utf8,
    }
    separator = detect_separator(lines)
    if separator is None:
        raise ValueError(
            f"{separator=} but it is not allowed to be None in the following."
        )

    transactions = pl.read_csv(
        file_path,
        skip_rows=ix_start_transactions - 1
        if is_empty_1st_line
        else ix_start_transactions,
        separator=separator,
        truncate_ragged_lines=True,
        encoding=encoding,
        schema=schema,
    )

    try:
        transactions = transactions.with_columns(
            [pl.col(col).str.to_date(date_format) for col in date_cols],
        )
    except pl.exceptions.InvalidOperationError as ex:
        logger.error(f"{separator=}")
        logger.error(f"{len(transactions)=:_}")
        logger.error(f"{transactions[date_cols[0]].to_list()=}")
        msg = f"{file_path=}: Failed to convert dates for values in one of the columns:"
        for col in date_cols:
            for v in transactions[col].unique():
                s = pl.Series([v])
                try:
                    s.str.to_date(date_format)
                except:  # noqa: E722
                    msg += f"\ncolumn '{col}' failed for value '{v}'"
        logger.error(msg)
        raise ex

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

    transactions = transactions.select(TRANSACTION_COLUMNS)

    return transactions


def parse_csv_file(case: Case, file_path: Path) -> tuple[pl.DataFrame, BalanceInfo]:
    encoding = detect_encoding(file_path)
    logger.debug(f"{file_path=} has {encoding=}")

    lines = load_lines(file_path, encoding)

    try:
        transactions = extract_transactions(case, file_path, lines, encoding)
    except Exception as e:
        msg = f"failed to parse {case=} transactions: {file_path=}"
        logger.error(msg)
        raise ExtractTransactionsException(msg) from e

    try:
        balance = extract_balance(case, file_path, lines)
    except Exception as e:
        msg = f"failed to parse {case=} balance: {file_path=}"
        logger.error(msg)
        raise ExtractBalanceException(msg) from e

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
        try:
            transactions, balance = parse_csv_file(case, file_path)
        except (ExtractBalanceException, ExtractTransactionsException):
            continue  # already logged in parse_csv_file

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
