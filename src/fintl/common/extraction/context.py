"""Shared context (prompt and response schema) for Scalable Capital extraction."""

from pydantic import BaseModel, Field

SYSTEM_PROMPT = "You are a Scraper for data contained in a screenshot of a broker web app."


class BalanceInfoExtract(BaseModel):
    """Bank account balance info to be extracted from a document."""

    amount: float = Field(default=..., description="Total amount of the brokerage account.")
    currency: str = Field(
        default="EUR", description="Currency of the total amount of the brokerage account."
    )
