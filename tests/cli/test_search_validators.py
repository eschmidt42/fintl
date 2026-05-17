"""Tests for the search input validators."""

from fintl.cli.commands.search.validators import AmountValidator, DateValidator


def test_date_validator_empty():
    """Test that DateValidator accepts an empty string as valid."""
    result = DateValidator().validate("")
    assert result.is_valid


def test_date_validator_valid():
    """Test that DateValidator accepts a well-formed ISO date string."""
    result = DateValidator().validate("2024-01-15")
    assert result.is_valid


def test_date_validator_invalid():
    """Test that DateValidator rejects a non-date string with an error message."""
    result = DateValidator().validate("notadate")
    assert not result.is_valid
    assert "Invalid date" in result.failure_descriptions


def test_amount_validator_empty():
    """Test that AmountValidator accepts an empty string as valid."""
    result = AmountValidator().validate("")
    assert result.is_valid


def test_amount_validator_valid():
    """Test that AmountValidator accepts a valid decimal number string."""
    result = AmountValidator().validate("123.45")
    assert result.is_valid


def test_amount_validator_invalid():
    """Test that AmountValidator rejects a non-numeric string with an error message."""
    result = AmountValidator().validate("abc")
    assert not result.is_valid
    assert "Must be a number" in result.failure_descriptions
