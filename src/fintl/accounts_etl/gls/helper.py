import datetime
import logging
import re
import typing as T
from pathlib import Path

import polars as pl

from fintl.accounts_etl.files import load_lines
from fintl.accounts_etl.schemas import (
    HASH_COLUMNS,
    TRANSACTION_COLUMNS,
    BalanceInfo,
    Case,
)
from fintl.accounts_etl.utils import (
    detect_encoding,
    find_line_with_pattern,
    german_string_numbers_to_floats,
    hash_transactions,
    verify_transactions,
)

logger = logging.getLogger(__name__)


def detect_separator(lines: list[str]) -> str | None:
    separator = None
    is_header_match_semicolon = any(
        re.search(r"(Bezeichnung Auftragskonto;IBAN Auftragskonto)", line)
        for line in lines
    )
    logger.debug(f"{is_header_match_semicolon=}")
    if is_header_match_semicolon:
        separator = ";"

    return separator


def check_if_parser_applies(file_path: Path) -> bool:
    is_file_name_match = (
        re.search(r"(DE\d{20}_\d{4}\.\d{2}\.\d{2}\.csv$)", str(file_path.name))
        is not None
    )
    logger.debug(f"{is_file_name_match=}")

    # check if the csv file at file_path contains "Betrag (€)"
    encoding = detect_encoding(file_path)
    lines = load_lines(file_path, encoding)

    separator = detect_separator(lines)
    is_expected_separator = separator is not None and separator == ";"
    return is_file_name_match and is_expected_separator


def extract_transactions(
    case: Case, file_path: Path, lines: T.List[str], encoding: str
) -> pl.DataFrame:
    transaction_pattern: str = (
        "^(Bezeichnung Auftragskonto;IBAN)"  # start of transactions
    )

    date_format: str = "%d.%m.%Y"
    date_cols: list = ["Valutadatum"]

    ix_start_transactions, transactions_header = find_line_with_pattern(
        lines, pattern=transaction_pattern
    )
    is_empty_1st_line = len(lines[0].strip()) == 0
    logger.debug(
        f"{file_path=} ({is_empty_1st_line=}) has {ix_start_transactions=} and {transactions_header=}"
    )

    schema = {
        "Bezeichnung Auftragskonto": pl.Utf8,
        "IBAN Auftragskonto": pl.Utf8,
        "BIC Auftragskonto": pl.Utf8,
        "Bankname Auftragskonto": pl.Utf8,
        "Buchungstag": pl.Utf8,
        "Valutadatum": pl.Utf8,
        "Name Zahlungsbeteiligter": pl.Utf8,
        "IBAN Zahlungsbeteiligter": pl.Utf8,
        "BIC (SWIFT-Code) Zahlungsbeteiligter": pl.Utf8,
        "Buchungstext": pl.Utf8,
        "Verwendungszweck": pl.Utf8,
        "Betrag": pl.Utf8,
        "Waehrung": pl.Utf8,
        "Saldo nach Buchung": pl.Utf8,
        "Bemerkung": pl.Utf8,
        "Kategorie": pl.Utf8,
        "Steuerrelevant": pl.Utf8,
        "Glaeubiger ID": pl.Utf8,
        "Mandatsreferenz": pl.Utf8,
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
        .str.strip_chars_end()
        .map_elements(german_string_numbers_to_floats, return_dtype=pl.Float64),
        pl.col("Saldo nach Buchung")
        .str.strip_chars_end()
        .map_elements(german_string_numbers_to_floats, return_dtype=pl.Float64),
    )
    transactions = transactions.with_columns(
        amount=pl.col("Betrag"),
        description=pl.col("Verwendungszweck"),
        date=pl.col("Valutadatum"),
        source=pl.col("Name Zahlungsbeteiligter"),
        recipient=pl.col(
            "Name Zahlungsbeteiligter"
        ),  # TODO: better solution for this and and the recipient column?
        provider=pl.lit(case.provider),
        service=pl.lit(case.service),
        parser=pl.lit(case.parser),
        file=pl.lit(str(file_path)),
    )
    transactions = hash_transactions(transactions, HASH_COLUMNS)

    verify_transactions(TRANSACTION_COLUMNS, transactions, file_path)

    transactions = transactions.select(
        TRANSACTION_COLUMNS + ["Saldo nach Buchung", "Waehrung"]
    )

    return transactions


def extract_balance(
    case: Case, transactions: pl.DataFrame, file_path: Path
) -> BalanceInfo | None:
    if len(transactions) == 0:
        return None

    transactions = transactions.sort("date", descending=True)

    latest_entry = transactions.row(0, named=True)

    date = latest_entry["date"]
    if not isinstance(date, datetime.date):
        raise ValueError(f"{date=} is not of type datetime.date")

    # date = datetime.date(date[2], date[1], date[0])

    amount = latest_entry["Saldo nach Buchung"]
    currency = latest_entry["Waehrung"]

    return BalanceInfo(
        date=date,
        amount=amount,
        currency=currency,
        provider=case.provider,
        service=case.service,
        parser=case.parser,
        file=str(file_path),
    )


def parse_csv_file(
    case: Case, file_path: Path
) -> tuple[pl.DataFrame, BalanceInfo | None]:
    encoding = detect_encoding(file_path)
    logger.debug(f"{file_path=} has {encoding=}")

    lines = load_lines(file_path, encoding)
    transactions = extract_transactions(case, file_path, lines, encoding)
    balance = extract_balance(case, transactions, file_path)
    transactions = transactions.drop("Saldo nach Buchung", "Waehrung")

    return transactions, balance
