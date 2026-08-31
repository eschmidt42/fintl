"""Types for extraction activities."""

from typing import TypedDict

from openai.types.chat.chat_completion import ChatCompletion
from pydantic import BaseModel

from fintl.common.extraction.context import (
    BalanceInfoExtract,
)


class ExtractionOutput(BaseModel):
    """Container for the result of a single extraction attempt."""

    extraction: BalanceInfoExtract | None
    completion: ChatCompletion | None
    elapsed: float
    ok: bool
    error_message: str


class ResultDetails(TypedDict):
    """Typed dictionary capturing per-sample extraction result details."""

    run: int
    idx: int
    path: str
    image_height: int
    image_width: int
    elapsed: float
    ok: bool
    error_message: str
    y_pred: float | None
    y_true: float
    completion_tokens: int | None
    prompt_tokens: int | None
    total_tokens: int | None
    reasoning_tokens: int | None


ExtractionResponse = tuple[BalanceInfoExtract, ChatCompletion]
