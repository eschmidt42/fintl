"""Custom exceptions for ETL extraction failures."""


class ExtractTransactionsException(Exception):
    """Exception for any unexpected issues during the transaction extraction."""


class ExtractBalanceException(Exception):
    """Exception for any unexpected issues during the balance extraction."""
