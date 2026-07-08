"""Tests for the Scalable Capital ollama extraction utilities."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from instructor.core.exceptions import FailedAttempt, InstructorRetryException

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
    _check_model_available,
    _check_ollama_availability,
    _get_lm_extraction,
)


def test_get_lm_extraction_calls_client_create(tmp_path: Path, png_fname: str):
    """_get_lm_extraction must call extraction_client.create and return its result."""
    expected = _BalanceInfoExtract(amount=1234.56, currency="EUR")
    mock_client = MagicMock()
    mock_client.create.return_value = expected

    dummy_file = tmp_path / png_fname
    dummy_file.write_bytes(b"\x89PNG")  # minimal non-empty file

    result = _get_lm_extraction(dummy_file, mock_client)

    assert result is expected
    mock_client.create.assert_called_once()


def test_get_lm_extraction_raises_ollama_inference_error_on_retry_exhausted(
    tmp_path: Path, png_fname: str
):
    """_get_lm_extraction wraps InstructorRetryException as OllamaInferenceError."""
    cause = RuntimeError("model runner has unexpectedly stopped")
    retry_exc = InstructorRetryException(
        str(cause),
        n_attempts=3,
        total_usage=0,
        failed_attempts=[FailedAttempt(1, cause, None)],
    )
    mock_client = MagicMock()
    mock_client.create.side_effect = retry_exc

    dummy_file = tmp_path / png_fname
    dummy_file.write_bytes(b"\x89PNG")

    with pytest.raises(InferenceError, match="model runner has unexpectedly stopped"):
        _get_lm_extraction(dummy_file, mock_client)


def test_check_ollama_availability_raises_on_connection_failure():
    """_check_ollama_availability raises OllamaUnavailableError when the server is unreachable."""
    with patch.object(httpx, "get", side_effect=httpx.ConnectError("connection refused")):
        with pytest.raises(OllamaUnavailableError, match="not reachable"):
            _check_ollama_availability("http://localhost:11434/v1")


def test_check_ollama_availability_strips_v1_suffix():
    """_check_ollama_availability GET-s the root URL (without /v1)."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch.object(httpx, "get", return_value=mock_response) as mock_get:
        _check_ollama_availability("http://localhost:11434/v1")

    mock_get.assert_called_once_with("http://localhost:11434", timeout=5.0)


def test_get_ollama_client_propagates_provider_error():
    """_get_ollama_client lets exceptions from instructor.from_provider bubble up."""
    with patch.object(
        ollama.instructor,
        "from_provider",
        side_effect=ValueError("bad model"),
    ):
        with pytest.raises(ValueError, match="bad model"):
            ollama._get_ollama_client(model="bad-model")


def test_check_ollama_availability_uses_base_url_as_is_without_v1_suffix():
    """_check_ollama_availability uses base_url unchanged when it has no /v1 suffix."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch.object(httpx, "get", return_value=mock_response) as mock_get:
        _check_ollama_availability("http://localhost:11434")

    mock_get.assert_called_once_with("http://localhost:11434", timeout=5.0)


def test_check_model_available_raises_when_bare_name_also_missing():
    """_check_model_available raises when model has no tag and no bare-name match."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "llama3.2:latest"}]}
    with patch.object(httpx, "get", return_value=mock_response):
        with pytest.raises(OllamaModelUnavailableError, match="qwen3.5"):
            _check_model_available("http://localhost:11434/v1", "qwen3.5")


def test_check_model_available_uses_base_url_as_is_without_v1_suffix():
    """_check_model_available calls /api/tags on the URL when /v1 is absent."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "qwen3.5:27b"}]}
    with patch.object(httpx, "get", return_value=mock_response) as mock_get:
        _check_model_available("http://localhost:11434", "qwen3.5:27b")

    mock_get.assert_called_once_with("http://localhost:11434/api/tags", timeout=5.0)


def test_check_model_available_passes_when_model_present():
    """_check_model_available does not raise when the model is in the tags response."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "models": [{"name": "qwen3.5:27b"}, {"name": "llama3.2:latest"}]
    }
    with patch.object(httpx, "get", return_value=mock_response):
        _check_model_available("http://localhost:11434/v1", "qwen3.5:27b")  # no raise


def test_check_model_available_passes_on_bare_name_match():
    """_check_model_available accepts a bare model name that matches before the colon."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "qwen3.5:27b"}]}
    with patch.object(httpx, "get", return_value=mock_response):
        _check_model_available("http://localhost:11434/v1", "qwen3.5")  # no raise


def test_check_model_available_raises_when_model_missing():
    """_check_model_available raises OllamaModelUnavailableError for an absent model."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "llama3.2:latest"}]}
    with patch.object(httpx, "get", return_value=mock_response):
        with pytest.raises(OllamaModelUnavailableError, match="qwen3.5:27b"):
            _check_model_available("http://localhost:11434/v1", "qwen3.5:27b")


def test_check_model_available_raises_on_http_error():
    """_check_model_available raises OllamaModelUnavailableError when the tags call fails."""
    with patch.object(httpx, "get", side_effect=httpx.ConnectError("connection refused")):
        with pytest.raises(OllamaModelUnavailableError, match="Could not retrieve"):
            _check_model_available("http://localhost:11434/v1", "qwen3.5:27b")
