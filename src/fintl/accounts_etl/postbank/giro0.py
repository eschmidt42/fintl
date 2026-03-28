import datetime
import logging
import re
import typing as T
from pathlib import Path

import polars as pl

from fintl.accounts_etl.exceptions import (
    ExtractBalanceException,
    ExtractTransactionsException,
)
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
    PostbankGiroParserEnum,
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
    provider=ProviderEnum.postbank.value,
    service=ServiceEnum.giro.value,
    parser=PostbankGiroParserEnum.giro0.value,
)


def check_if_parser_applies(file_path: Path) -> bool:
    is_file_name_match = (
        re.search(
            r"(Umsatzauskunft_KtoNr\d{10}_\d{2}-\d{2}-\d{4}_\d{2}-\d{2}-\d{2}.csv)$",
            str(file_path.name),
        )
        is not None
    )
    return is_file_name_match


def extract_transactions(
    case: Case,
    file_path: Path,
    lines: T.List[str],
    encoding: str,
) -> pl.DataFrame:
    transaction_pattern: str = "^(Buchungsdatum;Wertstellung)"  # start of transactions

    date_format: str = "%d.%m.%Y"
    date_cols: list = ["Buchungsdatum", "Wertstellung"]

    ix_start_transactions, transactions_header = find_line_with_pattern(
        lines, pattern=transaction_pattern
    )
    logger.debug(
        f"{file_path=} has {ix_start_transactions=} and {transactions_header=}"
    )

    schema = {
        "Buchungsdatum": pl.Utf8,
        "Wertstellung": pl.Utf8,
        "Umsatzart": pl.Utf8,
        "Buchungsdetails": pl.Utf8,  # TODO: add to output
        "Auftraggeber": pl.Utf8,
        "Empfänger": pl.Utf8,
        "Betrag (€)": pl.Utf8,
        "Saldo (€)": pl.Utf8,
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
        pl.col("Betrag (€)")
        .str.replace("€", "")
        .str.strip_chars_end()
        .map_elements(german_string_numbers_to_floats, return_dtype=pl.Float64),
    )
    transactions = transactions.with_columns(
        amount=pl.col("Betrag (€)"),
        description=pl.col("Buchungsdetails"),
        date=pl.col("Buchungsdatum"),
        source=pl.col("Auftraggeber"),
        recipient=pl.col("Empfänger"),
        provider=pl.lit(case.provider),
        service=pl.lit(case.service),
        parser=pl.lit(case.parser),
        file=pl.lit(str(file_path)),
    )

    transactions = hash_transactions(transactions, hash_columns=HASH_COLUMNS)

    verify_transactions(TRANSACTION_COLUMNS, transactions, file_path)

    transactions = transactions.select(TRANSACTION_COLUMNS)

    return transactions


def extract_balance(
    case: Case,
    file_path: Path,
    lines: T.List[str],
) -> BalanceInfo:
    balance_info_pattern: str = '^("?Aktueller Kontostand)'  # start of balance info
    ix_start_balance, balance_line = find_line_with_pattern(
        lines, pattern=balance_info_pattern
    )

    logger.debug(f"{file_path=} has {ix_start_balance=} and {balance_line=}")

    amount_line = balance_line.split(";")

    total = amount_line[1]
    total = total.strip(";").strip(":").strip('"').split(" ")
    amount, currency = total[0], total[1]
    amount = german_string_numbers_to_floats(amount)

    transaction_pattern: str = "^(Buchungsdatum;Wertstellung)"  # start of transactions
    start_line, _ = find_line_with_pattern(lines, transaction_pattern)

    start_line += 1  # selecting the first (and latest) transaction in file
    date_line = lines[start_line]
    date = date_line.split(";")[0]
    date = date.strip(";")
    date = [int(v) for v in date.split(".")]
    date = datetime.date(date[2], date[1], date[0])
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
    concatenate_new_information_to_history(parser_dir, parsed_dir, new_files_to_parse)

    logger.info(f"Done processing {CASE=}")
