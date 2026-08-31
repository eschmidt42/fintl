"""Unit tests for fintl.common.extraction.llama_swap."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from instructor.core.exceptions import FailedAttempt, InstructorRetryException
from openai.types import CompletionUsage
from openai.types.chat.chat_completion import ChatCompletion
from openai.types.completion_usage import CompletionTokensDetails

from fintl.common.extraction.context import BalanceInfoExtract
from fintl.common.extraction.core import _get_extraction
from fintl.common.extraction.errors import InferenceError
from fintl.common.extraction.llama_swap import (
    LlamaSwapExtractionModel,
    check_health,
    check_inference,
    check_model_available,
    sanity_check,
)

BASE_URL = "http://localhost:8080"
MODEL = "fake-model"
TIMEOUT = 30
SYSTEM_PROMPT = "You are a Scraper for data contained in a screenshot of a broker web app."


def _make_completion() -> ChatCompletion:
    return ChatCompletion.model_construct(
        id="test-id",
        choices=[],
        created=0,
        model="fake-model",
        object="chat.completion",
        usage=CompletionUsage.model_construct(
            completion_tokens=1,
            prompt_tokens=1,
            total_tokens=2,
            completion_tokens_details=CompletionTokensDetails.model_construct(reasoning_tokens=0),
        ),
    )


def _make_extraction() -> BalanceInfoExtract:
    return BalanceInfoExtract(amount=1234.56, currency="EUR")


@pytest.fixture
def llama_swap_model():
    """Provide a model with constructor dependencies mocked out."""
    mock_openai_client = MagicMock(name="openai_client")
    mock_instructor_client = MagicMock(name="instructor_client")

    with (
        patch(
            "fintl.common.extraction.llama_swap.OpenAI", return_value=mock_openai_client
        ) as mock_openai,
        patch(
            "fintl.common.extraction.llama_swap.instructor.from_openai",
            return_value=mock_instructor_client,
        ) as mock_from_openai,
    ):
        model = LlamaSwapExtractionModel(MODEL, base_url=BASE_URL, timeout=TIMEOUT)

    assert mock_openai.call_count == 1
    assert mock_from_openai.call_count == 1
    return model


def test_check_health_returns_true_when_endpoint_is_ok():
    """check_health returns True for an OK health response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "OK"
    mock_response.raise_for_status.return_value = None
    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    assert check_health(mock_client) is True
    mock_client.get.assert_called_once_with("/health", timeout=5.0)


def test_check_model_available_returns_true_when_model_is_listed():
    """check_model_available returns True when the model id is present."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"data": [{"id": "other-model"}, {"id": MODEL}]}
    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    assert check_model_available(mock_client, MODEL) is True
    mock_client.get.assert_called_once_with("/v1/models")


def test_check_model_available_returns_false_when_model_is_missing():
    """check_model_available returns False when the model id is absent."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"data": [{"id": "other-model"}]}
    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    assert check_model_available(mock_client, MODEL) is False


def test_check_inference_returns_true_when_completion_has_content():
    """check_inference returns True when the model produces any content."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "choices": [{"message": {"content": "hello"}}],
    }
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response

    assert check_inference(mock_client, MODEL) is True
    mock_client.post.assert_called_once_with(
        "/v1/chat/completions",
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "Say hello in one word."}],
            "max_tokens": 2000,
            "temperature": 0,
        },
    )


def test_sanity_check_returns_true_when_all_checks_pass():
    """sanity_check returns True when health, model, and inference checks all pass."""
    mock_client = MagicMock()
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_client
    mock_context.__exit__.return_value = None

    with (
        patch(
            "fintl.common.extraction.llama_swap.httpx.Client", return_value=mock_context
        ) as mock_client_cls,
        patch("fintl.common.extraction.llama_swap.check_health", return_value=True) as mock_health,
        patch(
            "fintl.common.extraction.llama_swap.check_model_available", return_value=True
        ) as mock_model,
        patch(
            "fintl.common.extraction.llama_swap.check_inference", return_value=True
        ) as mock_inference,
    ):
        assert sanity_check(MODEL, timeout=TIMEOUT, base_url=BASE_URL) is True

    mock_client_cls.assert_called_once_with(base_url=BASE_URL, timeout=TIMEOUT)
    mock_health.assert_called_once_with(mock_client)
    mock_model.assert_called_once_with(mock_client, MODEL)
    mock_inference.assert_called_once_with(mock_client, MODEL)


def test_sanity_check_returns_false_on_http_error():
    """sanity_check returns False when a health check raises an HTTP error."""
    mock_context = MagicMock()
    mock_context.__enter__.return_value = MagicMock()
    mock_context.__exit__.return_value = None

    with (
        patch("fintl.common.extraction.llama_swap.httpx.Client", return_value=mock_context),
        patch(
            "fintl.common.extraction.llama_swap.check_health",
            side_effect=httpx.HTTPError("boom"),
        ),
    ):
        assert sanity_check(MODEL, timeout=TIMEOUT, base_url=BASE_URL) is False


def test_get_extraction_returns_client_result(tmp_path: Path):
    """_get_extraction returns the client result on success."""
    file_path = tmp_path / "statement.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    expected = (_make_extraction(), _make_completion())
    mock_client = MagicMock()
    mock_client.create_with_completion.return_value = expected

    with patch(
        "fintl.common.extraction.core.InstructorImage.from_path",
        return_value="image",
    ):
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

    with patch(
        "fintl.common.extraction.core.InstructorImage.from_path",
        return_value="image",
    ):
        with pytest.raises(InferenceError, match="statement.png: model runner stopped"):
            _get_extraction(file_path, mock_client, MODEL, TIMEOUT)


def test_init_creates_instructor_client():
    """LlamaSwapExtractionModel wires OpenAI and instructor clients."""
    mock_openai_client = MagicMock(name="openai_client")
    mock_instructor_client = MagicMock(name="instructor_client")

    with (
        patch(
            "fintl.common.extraction.llama_swap.OpenAI", return_value=mock_openai_client
        ) as mock_openai,
        patch(
            "fintl.common.extraction.llama_swap.instructor.from_openai",
            return_value=mock_instructor_client,
        ) as mock_from_openai,
    ):
        model = LlamaSwapExtractionModel(MODEL, base_url=BASE_URL, timeout=TIMEOUT)

    mock_openai.assert_called_once_with(base_url=f"{BASE_URL}/v1", api_key="not-needed")
    mock_from_openai.assert_called_once_with(mock_openai_client)
    assert model.client is mock_instructor_client
    assert model.model == MODEL
    assert model.base_url == BASE_URL
    assert model.timeout == TIMEOUT


def test_predict_returns_successful_output(llama_swap_model: LlamaSwapExtractionModel):
    """Predict returns a successful ExtractionOutput when inference succeeds."""
    extraction = _make_extraction()
    completion = _make_completion()

    with patch(
        "fintl.common.extraction.core._get_extraction",
        return_value=(extraction, completion),
    ) as mock_get:
        result = llama_swap_model.predict(Path("statement.png"))

    assert result.ok is True
    assert result.error_message == ""
    assert result.extraction == extraction
    assert result.completion == completion
    assert result.elapsed >= 0
    mock_get.assert_called_once_with(
        file_path=Path("statement.png"),
        extraction_client=llama_swap_model.client,
        model=llama_swap_model.model,
        timeout=llama_swap_model.timeout,
    )


def test_predict_returns_error_output_on_inference_error(
    llama_swap_model: LlamaSwapExtractionModel,
):
    """Predict captures InferenceError as a failed ExtractionOutput."""
    with patch(
        "fintl.common.extraction.core._get_extraction",
        side_effect=InferenceError("boom"),
    ) as mock_get:
        result = llama_swap_model.predict(Path("statement.png"))

    assert result.ok is False
    assert result.extraction is None
    assert result.completion is None
    assert result.error_message == "boom"
    assert result.elapsed >= 0
    mock_get.assert_called_once()
