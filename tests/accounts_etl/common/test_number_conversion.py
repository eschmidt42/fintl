import pytest

from fintl.accounts_etl.common.number_conversion import (
    GermanNumberParsingError,
    check_if_german_number,
    german_string_numbers_to_floats,
)


def test_check_if_german_number():
    assert check_if_german_number("1.234,56") is True
    assert check_if_german_number("1,234.56") is False
    assert check_if_german_number("1.234") is True
    assert check_if_german_number("1,23") is True
    assert check_if_german_number("1.23") is False
    assert check_if_german_number("1234") is True
    assert check_if_german_number("12") is True
    assert check_if_german_number("1.2") is False


def test_german_string_numbers_to_floats():
    assert german_string_numbers_to_floats("1.234,56") == 1234.56
    assert german_string_numbers_to_floats("1.000.000,00") == 1000000.00
    assert german_string_numbers_to_floats("1,23") == 1.23
    assert german_string_numbers_to_floats(123) == 123
    assert german_string_numbers_to_floats(123.45) == 123.45

    assert german_string_numbers_to_floats("1.234") == 1_234
    assert german_string_numbers_to_floats("1,23") == 1.23
    assert german_string_numbers_to_floats("1234") == 1_234
    assert german_string_numbers_to_floats("12") == 12

    with pytest.raises(GermanNumberParsingError):
        german_string_numbers_to_floats("1,234.56")

    assert (
        german_string_numbers_to_floats("1.234,56 EUR", strip_currency=True) == 1234.56
    )


def test_check_if_german_number_multiple_commas():
    """A string with more than one comma must not be a valid German number."""
    assert check_if_german_number("1,000,00") is False
    assert check_if_german_number(",,") is False
