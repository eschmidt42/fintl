"""Utilities for parsing German-formatted number strings."""

import logging

logger = logging.getLogger(__name__)


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
        logger.debug(f"Skipping german_string_numbers_to_floats for {s} because it's not a string")
        return s

    if strip_currency:
        s = s.split()[0]

    is_german = check_if_german_number(s)
    if is_german:
        return float(s.replace(".", "").replace(",", ".").strip())
    else:
        raise GermanNumberParsingError(f"Expected German number but found: '{s}'")
