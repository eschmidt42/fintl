import logging
import re
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def is_match(pattern: str, x: str) -> bool:
    """Checks if a string matches a given regex pattern.

    Args:
        pattern: The regex pattern to match against.
        x: The string to test.

    Returns:
        True if the pattern matches the string, False otherwise.
    """
    return re.search(pattern, x) is not None


def find_line_with_pattern(lines: list[str], pattern: str) -> tuple[int, str]:
    """Finds the first line in a list that matches a given pattern.

    Args:
        lines: A list of strings to search through.
        pattern: The regex pattern to match.

    Returns:
        A tuple containing the line index (0-based) and the matched line string.

    Raises:
        ValueError: If no line matches the pattern.
    """

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
    """Raised when a string contains a number not in the expected German format."""


def check_if_german_number(s: str) -> bool:
    """Checks if a string contains a number formatted in the German style.

    German format uses dots for thousands separators and commas for the decimal point
    (e.g., "1.234,56").

    Args:
        s: The string to check for German number formatting.

    Returns:
        True if the string matches German number formatting rules, False otherwise.
    """
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


def german_string_numbers_to_floats(s: str | int | float, strip_currency: bool = False):
    """Converts a German-formatted string number to a Python float.

    Converts German-style number formatting (dots for thousands, comma for decimals)
    to a standard Python float.

    Args:
        s: The value to convert. Can be a string, int, or float.
        strip_currency: If True, removes any currency symbols/words before parsing.

    Returns:
        A float representation of the number.

    Raises:
        GermanNumberParsingError: If the input string is not in German format.
    """
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
    transactions: pl.DataFrame, hash_columns: list[str]
) -> pl.DataFrame:
    """Adds a hash column to a transactions DataFrame based on specific columns.

    Args:
        transactions: DataFrame containing transaction data.
        hash_columns: List of column names to include in the hash calculation.

    Returns:
        The DataFrame with an added 'hash' column.
    """
    transactions = transactions.with_columns(
        hash=transactions.select(hash_columns).hash_rows()
    )
    return transactions


def verify_transactions(
    transaction_columns: list[str], transactions: pl.DataFrame, file_path: Path
):
    """Verifies that all expected columns exist in the transactions DataFrame.

    Args:
        transaction_columns: List of expected column names.
        transactions: The DataFrame to verify.
        file_path: Path to the source file (used for error messages).

    Raises:
        ValueError: If any expected column is missing from the DataFrame.
    """
    for col in transaction_columns:
        if col not in transactions.columns:
            raise ValueError(
                f"Expected column '{col}' in transactions parsed from {file_path=}"
            )
