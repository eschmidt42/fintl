"""Unit tests for fintl.common.extraction.unload."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from fintl.common.config import OllamaConfig
from fintl.common.extraction.errors import OllamaUnavailableError
from fintl.common.extraction.unload import (
    _get_ollama_root_url,
    _list_running_ollama_models,
    unload_ollama,
)

BASE_URL = "http://localhost:11434/v1"
ROOT_URL = "http://localhost:11434"
TIMEOUT = 30


@pytest.fixture
def ollama_config() -> OllamaConfig:
    """Build a reusable ollama config for unload tests."""
    return OllamaConfig(model="some-model", base_url=BASE_URL)


class TestGetOllamaRootUrl:
    """Tests for _get_ollama_root_url helper function."""

    def test_strips_v1_suffix(self):
        """Removes /v1 suffix from URL."""
        assert _get_ollama_root_url("http://localhost:11434/v1") == "http://localhost:11434"

    def test_strips_v1_suffix_with_trailing_slash(self):
        """Removes /v1 suffix even with trailing slash."""
        assert _get_ollama_root_url("http://localhost:11434/v1/") == "http://localhost:11434"

    def test_handles_url_without_v1(self):
        """Returns unchanged URL if it doesn't have /v1 suffix."""
        assert _get_ollama_root_url("http://localhost:11434") == "http://localhost:11434"

    def test_handles_trailing_slash(self):
        """Removes trailing slash before processing."""
        assert _get_ollama_root_url("http://localhost:11434/") == "http://localhost:11434"

    def test_handles_double_v1_suffix(self):
        """Only removes the last /v1 occurrence."""
        assert _get_ollama_root_url("http://localhost:11434/v1/v1") == "http://localhost:11434/v1"


class TestListRunningOllamaModels:
    """Tests for _list_running_ollama_models helper function."""

    def test_successfully_lists_models(self):
        """Returns list of model names from /api/ps response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "models": [
                {"name": "model-a"},
                {"name": "model-b"},
                {"name": "model-c"},
            ]
        }
        with patch("fintl.common.extraction.unload.httpx.get", return_value=mock_response):
            models = _list_running_ollama_models(ROOT_URL, TIMEOUT)

        assert models == ["model-a", "model-b", "model-c"]

    def test_returns_empty_list_when_no_models(self):
        """Returns empty list when /api/ps has no models."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"models": []}
        with patch("fintl.common.extraction.unload.httpx.get", return_value=mock_response):
            models = _list_running_ollama_models(ROOT_URL, TIMEOUT)

        assert models == []

    def test_handles_missing_models_key(self):
        """Returns empty list when 'models' key is missing from response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        with patch("fintl.common.extraction.unload.httpx.get", return_value=mock_response):
            models = _list_running_ollama_models(ROOT_URL, TIMEOUT)

        assert models == []

    def test_http_error_returns_empty_list(self):
        """Returns empty list and logs warning on HTTP error."""
        with patch(
            "fintl.common.extraction.unload.httpx.get",
            side_effect=httpx.HTTPStatusError(
                "500 Internal Server Error",
                request=MagicMock(),
                response=MagicMock(),
            ),
        ):
            models = _list_running_ollama_models(ROOT_URL, TIMEOUT)

        assert models == []

    def test_timeout_error_returns_empty_list(self):
        """Returns empty list and logs warning on timeout."""
        with patch(
            "fintl.common.extraction.unload.httpx.get",
            side_effect=httpx.TimeoutException("Request timed out"),
        ):
            models = _list_running_ollama_models(ROOT_URL, TIMEOUT)

        assert models == []

    def test_connect_error_returns_empty_list(self):
        """Returns empty list and logs warning on connection error."""
        with patch(
            "fintl.common.extraction.unload.httpx.get",
            side_effect=httpx.ConnectError("Connection refused"),
        ):
            models = _list_running_ollama_models(ROOT_URL, TIMEOUT)

        assert models == []


class TestUnloadOllama:
    """Tests for unload_ollama function."""

    def test_ollama_unavailable_skips_unload(self, ollama_config: OllamaConfig):
        """Skips unload when ollama is not available."""
        with (
            patch(
                "fintl.common.extraction.unload.check_ollama_availability",
                side_effect=OllamaUnavailableError("Ollama not reachable"),
            ),
            patch("fintl.common.extraction.unload.httpx.get") as mock_get,
            patch("fintl.common.extraction.unload.httpx.post") as mock_post,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        mock_get.assert_not_called()
        mock_post.assert_not_called()

    def test_no_config_uses_default_url(self):
        """Uses default OLLAMA_BASE_URL when config is None."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"models": []}
        with (
            patch("fintl.common.extraction.unload.check_ollama_availability") as mock_check,
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_response),
        ):
            unload_ollama(None, TIMEOUT)

        # Should check availability with default URL (without /v1)
        mock_check.assert_called_once()
        call_args = mock_check.call_args
        # The base_url should be the default OLLAMA_BASE_URL
        assert "11434" in str(call_args)

    def test_with_config_uses_config_url(self):
        """Uses config.base_url when config is provided."""
        custom_url = "http://remote-host:11434/v1"
        custom_config = OllamaConfig(model="test-model", base_url=custom_url)
        mock_response = MagicMock()
        mock_response.json.return_value = {"models": []}
        with (
            patch("fintl.common.extraction.unload.check_ollama_availability") as mock_check,
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_response),
        ):
            unload_ollama(custom_config, TIMEOUT)

        # Should check availability with custom URL
        mock_check.assert_called_once_with(custom_url)

    def test_unload_single_model_successfully(self, ollama_config: OllamaConfig):
        """Successfully unloads a single model."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {"models": [{"name": "test-model"}]}
        mock_unload_response = MagicMock()

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_ps_response),
            patch(
                "fintl.common.extraction.unload.httpx.post",
                return_value=mock_unload_response,
            ) as mock_post,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        # Verify the POST was called with correct payload
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "api/generate" in str(call_args)
        json_payload = call_args.kwargs.get("json")
        assert json_payload is not None
        assert json_payload["model"] == "test-model"
        assert json_payload["prompt"] == ""
        assert json_payload["keep_alive"] == 0

    def test_unload_multiple_models_successfully(self, ollama_config: OllamaConfig):
        """Successfully unloads multiple models in sequence."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {
            "models": [
                {"name": "model-a"},
                {"name": "model-b"},
                {"name": "model-c"},
            ]
        }
        mock_unload_response = MagicMock()

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_ps_response),
            patch(
                "fintl.common.extraction.unload.httpx.post",
                return_value=mock_unload_response,
            ) as mock_post,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        # Verify POST was called once for each model
        assert mock_post.call_count == 3
        call_args_list = mock_post.call_args_list
        models_unloaded = [call.kwargs["json"]["model"] for call in call_args_list]
        assert models_unloaded == ["model-a", "model-b", "model-c"]

    def test_unload_with_no_running_models(self, ollama_config: OllamaConfig):
        """Handles case when no models are running."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {"models": []}

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_ps_response),
            patch("fintl.common.extraction.unload.httpx.post") as mock_post,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        # No POST calls should be made
        mock_post.assert_not_called()

    def test_http_error_on_model_unload_is_logged_and_continues(self, ollama_config: OllamaConfig):
        """Logs warning and continues with next model when unload POST fails."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {
            "models": [
                {"name": "model-a"},
                {"name": "model-b"},
            ]
        }
        mock_success_response = MagicMock()
        mock_error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=MagicMock(),
            response=MagicMock(),
        )

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_ps_response),
            patch(
                "fintl.common.extraction.unload.httpx.post",
                side_effect=[mock_error, mock_success_response],
            ) as mock_post,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        # Both models should be attempted despite first one failing
        assert mock_post.call_count == 2

    def test_connect_error_on_model_unload_is_logged_and_continues(
        self, ollama_config: OllamaConfig
    ):
        """Logs warning and continues with next model on connection error."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {
            "models": [
                {"name": "model-a"},
                {"name": "model-b"},
            ]
        }
        mock_success_response = MagicMock()
        mock_error = httpx.ConnectError("Connection refused")

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_ps_response),
            patch(
                "fintl.common.extraction.unload.httpx.post",
                side_effect=[mock_error, mock_success_response],
            ) as mock_post,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        # Both models should be attempted despite first one failing
        assert mock_post.call_count == 2

    def test_partial_unload_failure_attempts_all_models(self, ollama_config: OllamaConfig):
        """All models are attempted even if some fail."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {
            "models": [
                {"name": "model-a"},
                {"name": "model-b"},
                {"name": "model-c"},
            ]
        }
        mock_success_response = MagicMock()
        mock_error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=MagicMock(),
            response=MagicMock(),
        )

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_ps_response),
            patch(
                "fintl.common.extraction.unload.httpx.post",
                side_effect=[mock_success_response, mock_error, mock_success_response],
            ) as mock_post,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        # All three models should be attempted
        assert mock_post.call_count == 3
        call_args_list = mock_post.call_args_list
        models_attempted = [call.kwargs["json"]["model"] for call in call_args_list]
        assert models_attempted == ["model-a", "model-b", "model-c"]

    def test_timeout_used_in_post_requests(self, ollama_config: OllamaConfig):
        """Timeout parameter is passed to POST requests."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {"models": [{"name": "test-model"}]}
        mock_unload_response = MagicMock()
        custom_timeout = 60

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload.httpx.get", return_value=mock_ps_response),
            patch(
                "fintl.common.extraction.unload.httpx.post",
                return_value=mock_unload_response,
            ) as mock_post,
        ):
            unload_ollama(ollama_config, custom_timeout)

        # Verify timeout was passed
        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["timeout"] == custom_timeout

    def test_root_url_correctly_constructed(self, ollama_config: OllamaConfig):
        """Root URL is correctly extracted from base_url with /v1 suffix."""
        mock_ps_response = MagicMock()
        mock_ps_response.json.return_value = {"models": []}

        with (
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch(
                "fintl.common.extraction.unload.httpx.get",
                return_value=mock_ps_response,
            ) as mock_get,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        # Verify the /api/ps call used the root URL (without /v1)
        call_args = mock_get.call_args
        url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url", "")
        assert "/api/ps" in url
        assert "/v1/api/ps" not in url
