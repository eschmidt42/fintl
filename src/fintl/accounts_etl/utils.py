import logging
import re
import typing as T
from pathlib import Path

import chardet
import polars as pl

from fintl.accounts_etl.schemas import Case, Config

logger = logging.getLogger(__name__)


def detect_encoding(path: Path, encoding_default: str = "utf-8") -> str:
    with open(path, "rb") as f:
        res = chardet.detect(f.read())
    logger.debug(f"detect encoding: {res}")
    enc = res["encoding"]
    if enc is None:
        logger.warning(f"Failed to detect encoding, defaulting to {encoding_default}")
        enc = encoding_default
    return enc


def concatenate_parquets(
    fname: str, config: Config, cases: list[Case], columns: list[str]
) -> pl.DataFrame | None:
    dfs = []
    for case in cases:
        path = config.get_parser_dir(case) / fname
        logger.info(f"Processing {path=}.")

        if not path.exists():
            logger.warning(f"{path=} does not exist, skipping.")
            continue

        tmp = pl.read_parquet(path)

        if len(tmp) == 0:
            logger.warning(f"{len(tmp)=}, skipping.")
            continue
        else:
            logger.info(f"Appending {len(tmp):_d} rows for {case=}")

        tmp = tmp.select(columns)

        dfs.append(tmp)

    if len(dfs) > 0:
        return pl.concat(dfs)
    else:
        return None


def is_match(pattern: str, x: str) -> bool:
    return re.search(pattern, x) is not None


def find_line_with_pattern(lines: list[str], pattern: str) -> tuple[int, str]:
    "Identifies the first relevant line in a csv"

    ix_match = None
    matched_line = ""
    for i, line in enumerate(lines):
        if is_match(pattern, line):
            ix_match = i
            matched_line = line
            break

    if ix_match is None:
        logger.warning(f"Could not find line matching {pattern=}")

    if ix_match is None:
        raise ValueError(
            f"Unexpectedly failed to find the first index with {pattern=} in {lines[:10]=}"
        )

    return ix_match, matched_line


class GermanNumberParsingError(Exception):
    "Raised if a number in a string is not in the expected German format"


def check_if_german_number(s: str) -> bool:
    comma_count = s.count(",")
    dot_count = s.count(".")

    max_one_comma = comma_count <= 1
    if not max_one_comma:
        return False

    comma_pos = s.find(",")
    dot_pos = [i for i, _s in enumerate(s) if _s == "."]

    has_dot = dot_count > 0
    has_comma = comma_count > 0

    ge_punctuation_order = True
    if has_comma and has_dot:
        # case like 1.123,0 fine but 1,234.0 not
        ge_punctuation_order = dot_pos[-1] < comma_pos
    elif has_dot:
        # case like "1.2" or "1.23"
        _s = s.split(".")
        ge_punctuation_order = len(_s[-1]) == 3
    elif has_comma:
        # case like "1,234"
        ge_punctuation_order = True

    return max_one_comma and ge_punctuation_order


def german_string_numbers_to_floats(
    s: T.Union[str, int, float], strip_currency: bool = False
):
    if isinstance(s, (int, float)):
        logger.debug(
            f"Skipping german_string_numbers_to_floats for {s} because it's not a string"
        )
        return s

    if strip_currency:
        s = s.split()[0]

    is_german = check_if_german_number(s)
    if is_german:
        return float(s.replace(".", "").replace(",", ".").strip())
    else:
        raise GermanNumberParsingError(f"Expected German number but found: '{s}'")


def hash_transactions(
    transactions: pl.DataFrame, hash_columns: T.List[str]
) -> pl.DataFrame:
    transactions = transactions.with_columns(
        hash=transactions.select(hash_columns).hash_rows()
    )
    return transactions


def verify_transactions(
    transaction_columns: list[str], transactions: pl.DataFrame, file_path: Path
):
    for col in transaction_columns:
        if col not in transactions.columns:
            raise ValueError(
                f"Expected column '{col}' in transactions parsed from {file_path=}"
            )
