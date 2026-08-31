"""Unit tests for shared extraction utilities."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from instructor.core.exceptions import FailedAttempt, InstructorRetryException
from openai.types import CompletionUsage
from openai.types.chat.chat_completion import ChatCompletion
from openai.types.completion_usage import CompletionTokensDetails

from fintl.common.extraction.context import BalanceInfoExtract
from fintl.common.extraction.core import _get_extraction
from fintl.common.extraction.errors import InferenceError

MODEL = "fake-model"
TIMEOUT = 30
SYSTEM_PROMPT = "You are a Scraper for data contained in a screenshot of a broker web app."


def _make_completion() -> ChatCompletion:
    return ChatCompletion.model_construct(
        id="test-id",
        choices=[],
        created=0,
        model=MODEL,
        object="chat.completion",
        usage=CompletionUsage.model_construct(
            completion_tokens=1,
            prompt_tokens=1,
            total_tokens=2,
            completion_tokens_details=CompletionTokensDetails.model_construct(reasoning_tokens=0),
        ),
    )


def test_get_extraction_returns_client_result(tmp_path: Path):
    """_get_extraction returns the client result on success."""
    file_path = tmp_path / "statement.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    expected = (BalanceInfoExtract(amount=1234.56, currency="EUR"), _make_completion())
    mock_client = MagicMock()
    mock_client.create_with_completion.return_value = expected

    with patch("fintl.common.extraction.core.InstructorImage.from_path", return_value="image"):
        result = _get_extraction(file_path, mock_client, MODEL, TIMEOUT)

    assert result is expected
    mock_client.create_with_completion.assert_called_once_with(
        model=MODEL,
        response_model=BalanceInfoExtract,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": ["Please extract data from the following image", "image"]},
        ],
        timeout=TIMEOUT,
    )


def test_get_extraction_wraps_retry_exceptions(tmp_path: Path):
    """_get_extraction translates retry exhaustion into InferenceError."""
    file_path = tmp_path / "statement.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    cause = RuntimeError("model runner stopped")
    retry_exc = InstructorRetryException(
        str(cause),
        n_attempts=3,
        total_usage=0,
        failed_attempts=[FailedAttempt(1, cause, None)],
    )
    mock_client = MagicMock()
    mock_client.create_with_completion.side_effect = retry_exc

    with patch("fintl.common.extraction.core.InstructorImage.from_path", return_value="image"):
        with pytest.raises(InferenceError, match="statement.png: model runner stopped"):
            _get_extraction(file_path, mock_client, MODEL, TIMEOUT)
