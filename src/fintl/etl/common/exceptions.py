"""Custom exceptions for ETL extraction failures."""


class ExtractTransactionsError(Exception):
    """Exception for any unexpected issues during the transaction extraction."""


class ExtractBalanceError(Exception):
    """Exception for any unexpected issues during the balance extraction."""
