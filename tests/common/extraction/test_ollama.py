"""Tests for the Scalable Capital ollama extraction utilities."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from instructor.core.exceptions import FailedAttempt, InstructorRetryException
from openai.types import CompletionUsage
from openai.types.chat.chat_completion import ChatCompletion
from openai.types.completion_usage import CompletionTokensDetails

from fintl.common.extraction import ollama
from fintl.common.extraction.context import (
    _BalanceInfoExtract,
)
from fintl.common.extraction.errors import (
    InferenceError,
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)
from fintl.common.extraction.ollama import (
    OllamaExtractionModel,
    _get_extraction,
    check_model_availability,
    check_provider_availability,
    v1ify,
)


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


def _make_extraction() -> _BalanceInfoExtract:
    return _BalanceInfoExtract(amount=1234.56, currency="EUR")


@pytest.mark.parametrize(
    ("url", "suffix", "expected"),
    [
        ("http://localhost:11434", "/v1", "http://localhost:11434/v1"),
        ("http://localhost:11434/", "/v1", "http://localhost:11434/v1"),
        ("http://localhost:11434/v1", "/v1", "http://localhost:11434/v1"),
        ("http://localhost:11434", "/custom", "http://localhost:11434/custom"),
        ("http://localhost:11434/custom", "/custom", "http://localhost:11434/custom"),
    ],
)
def test_v1ify(url: str, suffix: str, expected: str):
    """Appends a suffix once and removes a trailing slash before appending."""
    assert v1ify(url, suffix=suffix) == expected


def test_get_extraction_calls_client_create(tmp_path: Path, png_fname: str):
    """_get_extraction must call extraction_client.create and return its result."""
    extraction = _BalanceInfoExtract(amount=1234.56, currency="EUR")
    completion = ChatCompletion.model_construct(
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
    mock_client = MagicMock()
    expected = (extraction, completion)
    mock_client.create_with_completion.return_value = expected

    dummy_file = tmp_path / png_fname
    dummy_file.write_bytes(b"\x89PNG")  # minimal non-empty file

    result = _get_extraction(dummy_file, mock_client, 2 * 60)

    assert result is expected
    mock_client.create_with_completion.assert_called_once()


def test_get_extraction_raises_ollama_inference_error_on_retry_exhausted(
    tmp_path: Path, png_fname: str
):
    """_get_extraction wraps InstructorRetryException as OllamaInferenceError."""
    cause = RuntimeError("model runner has unexpectedly stopped")
    retry_exc = InstructorRetryException(
        str(cause),
        n_attempts=3,
        total_usage=0,
        failed_attempts=[FailedAttempt(1, cause, None)],
    )
    mock_client = MagicMock()
    mock_client.create_with_completion.side_effect = retry_exc

    dummy_file = tmp_path / png_fname
    dummy_file.write_bytes(b"\x89PNG")

    with pytest.raises(InferenceError, match="model runner has unexpectedly stopped"):
        _get_extraction(dummy_file, mock_client, 2 * 60)


def test_check_ollama_availability_raises_on_connection_failure():
    """_check_ollama_availability raises OllamaUnavailableError when the server is unreachable."""
    client = MagicMock()
    client.get.side_effect = ConnectionError("connection refused")
    with pytest.raises(OllamaUnavailableError, match="not reachable"):
        check_provider_availability(client)


def test_check_ollama_availability_uses_root_client_endpoint():
    """_check_ollama_availability checks the configured root client."""
    client = MagicMock()
    check_provider_availability(client)

    client.get.assert_called_once_with("/", timeout=5.0)


def test_get_ollama_client_propagates_provider_error():
    """_get_ollama_client lets exceptions from instructor.from_provider bubble up."""
    with patch.object(
        ollama.instructor,
        "from_provider",
        side_effect=ValueError("bad model"),
    ):
        with pytest.raises(ValueError, match="bad model"):
            ollama._get_client(model="bad-model")


def test_ollama_extraction_model_initializes_client():
    """OllamaExtractionModel stores config and creates its instructor client."""
    mock_client = MagicMock()
    base_url = "http://localhost:11434"
    with patch("fintl.common.extraction.ollama._get_client", return_value=mock_client) as mock_get:
        model = OllamaExtractionModel("fake-model", base_url=base_url, timeout=90)

    mock_get.assert_called_once_with(model="fake-model", ollama_base_url=base_url)
    assert model.model == "fake-model"
    assert model.base_url == base_url
    assert model.timeout == 90
    assert model.client is mock_client


def test_ollama_extraction_model_predict_returns_success(tmp_path: Path):
    """Predict returns a successful ExtractionOutput when inference succeeds."""
    mock_client = MagicMock()
    expected = (_make_extraction(), _make_completion())
    base_url = "http://localhost:11434"
    with patch("fintl.common.extraction.ollama._get_client", return_value=mock_client):
        model = OllamaExtractionModel("fake-model", base_url=base_url)

    with patch(
        "fintl.common.extraction.ollama._get_extraction",
        return_value=expected,
    ) as mock_get:
        result = model.predict(tmp_path / "statement.png")

    assert result.ok is True
    assert result.error_message == ""
    assert result.extraction == expected[0]
    assert result.completion == expected[1]
    assert result.elapsed >= 0
    mock_get.assert_called_once_with(
        file_path=tmp_path / "statement.png",
        extraction_client=mock_client,
        timeout=model.timeout,
    )


def test_ollama_extraction_model_predict_returns_error(tmp_path: Path):
    """Predict converts InferenceError into a failed ExtractionOutput."""
    mock_client = MagicMock()
    base_url = "http://localhost:11434"
    with patch("fintl.common.extraction.ollama._get_client", return_value=mock_client):
        model = OllamaExtractionModel("fake-model", base_url=base_url)

    with patch(
        "fintl.common.extraction.ollama._get_extraction",
        side_effect=InferenceError("boom"),
    ) as mock_get:
        result = model.predict(tmp_path / "statement.png")

    assert result.ok is False
    assert result.extraction is None
    assert result.completion is None
    assert result.error_message == "boom"
    assert result.elapsed >= 0
    mock_get.assert_called_once_with(
        file_path=tmp_path / "statement.png",
        extraction_client=mock_client,
        timeout=model.timeout,
    )


def test_check_model_available_raises_when_bare_name_also_missing():
    """_check_model_available raises when model has no tag and no bare-name match."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "llama3.2:latest"}]}
    client = MagicMock()
    client.get.return_value = mock_response
    with pytest.raises(OllamaModelUnavailableError, match="qwen3.5"):
        check_model_availability(client, "qwen3.5")


def test_check_model_available_uses_base_url_as_is_without_v1_suffix():
    """_check_model_available calls /api/tags on the URL when /v1 is absent."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "qwen3.5:27b"}]}
    client = MagicMock()
    client.get.return_value = mock_response
    check_model_availability(client, "qwen3.5:27b")

    client.get.assert_called_once_with("/api/tags", timeout=5.0)


def test_check_model_available_passes_when_model_present():
    """_check_model_available does not raise when the model is in the tags response."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "models": [{"name": "qwen3.5:27b"}, {"name": "llama3.2:latest"}]
    }
    client = MagicMock()
    client.get.return_value = mock_response
    check_model_availability(client, "qwen3.5:27b")  # no raise


def test_check_model_available_passes_on_bare_name_match():
    """_check_model_available accepts a bare model name that matches before the colon."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "qwen3.5:27b"}]}
    client = MagicMock()
    client.get.return_value = mock_response
    check_model_availability(client, "qwen3.5")  # no raise


def test_check_model_available_raises_when_model_missing():
    """_check_model_available raises OllamaModelUnavailableError for an absent model."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "llama3.2:latest"}]}
    client = MagicMock()
    client.get.return_value = mock_response
    with pytest.raises(OllamaModelUnavailableError, match="qwen3.5:27b"):
        check_model_availability(client, "qwen3.5:27b")


def test_check_model_available_raises_on_http_error():
    """_check_model_available raises OllamaModelUnavailableError when the tags call fails."""
    client = MagicMock()
    client.get.side_effect = httpx.ConnectError("connection refused")
    with pytest.raises(OllamaModelUnavailableError, match="Could not retrieve"):
        check_model_availability(client, "qwen3.5:27b")
