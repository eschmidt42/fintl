from fintl.cli.commands.search.validators import AmountValidator, DateValidator


def test_date_validator_empty():
    result = DateValidator().validate("")
    assert result.is_valid


def test_date_validator_valid():
    result = DateValidator().validate("2024-01-15")
    assert result.is_valid


def test_date_validator_invalid():
    result = DateValidator().validate("notadate")
    assert not result.is_valid
    assert "Invalid date" in result.failure_descriptions


def test_amount_validator_empty():
    result = AmountValidator().validate("")
    assert result.is_valid


def test_amount_validator_valid():
    result = AmountValidator().validate("123.45")
    assert result.is_valid


def test_amount_validator_invalid():
    result = AmountValidator().validate("abc")
    assert not result.is_valid
    assert "Must be a number" in result.failure_descriptions
