"""Shared context (prompt and response schema) for Scalable Capital extraction."""

from pydantic import BaseModel

_SYSTEM_PROMPT = "You are a Scraper for data contained in a screenshot of a broker web app."


class _BalanceInfoExtract(BaseModel):
    amount: float
    currency: str
