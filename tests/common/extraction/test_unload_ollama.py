"""Unit tests for fintl.common.extraction.unload."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from fintl.common.config import OllamaConfig
from fintl.common.extraction.errors import OllamaUnavailableError
from fintl.common.extraction.unload import _list_running_ollama_models, unload_ollama

BASE_URL = "http://localhost:11434"
TIMEOUT = 30


@pytest.fixture
def ollama_config() -> OllamaConfig:
    """Build a reusable ollama config for unload tests."""
    return OllamaConfig(model="some-model", base_url=BASE_URL)


class TestListRunningOllamaModels:
    """Tests for _list_running_ollama_models helper function."""

    def test_successfully_lists_models(self):
        """Returns model names from /api/ps."""
        client = MagicMock()
        response = MagicMock()
        response.json.return_value = {"models": [{"name": "model-a"}, {"name": "model-b"}]}
        client.get.return_value = response

        assert _list_running_ollama_models(client, TIMEOUT) == ["model-a", "model-b"]
        client.get.assert_called_once_with("/api/ps", timeout=TIMEOUT)

    def test_returns_empty_list_when_no_models(self):
        """Returns an empty list when /api/ps has no models."""
        client = MagicMock()
        response = MagicMock()
        response.json.return_value = {}
        client.get.return_value = response

        assert _list_running_ollama_models(client, TIMEOUT) == []

    @pytest.mark.parametrize(
        "error",
        [
            httpx.HTTPStatusError("500", request=MagicMock(), response=MagicMock()),
            httpx.TimeoutException("timed out"),
            httpx.ConnectError("refused"),
        ],
    )
    def test_http_error_returns_empty_list(self, error):
        """Returns an empty list when model discovery fails."""
        client = MagicMock()
        client.get.side_effect = error

        assert _list_running_ollama_models(client, TIMEOUT) == []


class TestUnloadOllama:
    """Tests for unload_ollama function."""

    def test_ollama_unavailable_skips_unload(self, ollama_config: OllamaConfig):
        """Skips unload when ollama is not available."""
        with (
            patch(
                "fintl.common.extraction.unload.check_ollama_availability",
                side_effect=OllamaUnavailableError("not reachable"),
            ),
            patch("fintl.common.extraction.unload.httpx.Client") as client_cls,
        ):
            unload_ollama(ollama_config, TIMEOUT)

        client_cls.assert_called_once_with(base_url=BASE_URL, timeout=TIMEOUT)
        client_cls.return_value.__enter__.return_value.post.assert_not_called()

    def test_no_config_uses_default_url(self):
        """Uses the default root URL when no config is provided."""
        with (
            patch("fintl.common.extraction.unload.httpx.Client") as client_cls,
            patch("fintl.common.extraction.unload.check_ollama_availability"),
            patch("fintl.common.extraction.unload._list_running_ollama_models", return_value=[]),
        ):
            unload_ollama(None, TIMEOUT)

        assert client_cls.call_args.kwargs["base_url"] == "http://localhost:11434"

    def test_unloads_all_models_with_shared_client(self, ollama_config: OllamaConfig):
        """Discovers and unloads every running model through one client."""
        client = MagicMock()
        ps_response = MagicMock()
        ps_response.json.return_value = {"models": [{"name": "model-a"}, {"name": "model-b"}]}
        client.get.return_value = ps_response
        client.post.return_value = MagicMock()

        with (
            patch("fintl.common.extraction.unload.httpx.Client") as client_cls,
            patch("fintl.common.extraction.unload.check_ollama_availability"),
        ):
            client_cls.return_value.__enter__.return_value = client
            unload_ollama(ollama_config, TIMEOUT)

        assert client.post.call_count == 2
        assert [call.kwargs["json"]["model"] for call in client.post.call_args_list] == [
            "model-a",
            "model-b",
        ]
        assert all(call.args[0] == "/api/generate" for call in client.post.call_args_list)

    def test_partial_unload_failure_attempts_all_models(self, ollama_config: OllamaConfig):
        """Attempts all models even when one unload request fails."""
        client = MagicMock()
        ps_response = MagicMock()
        ps_response.json.return_value = {
            "models": [{"name": "model-a"}, {"name": "model-b"}, {"name": "model-c"}]
        }
        client.get.return_value = ps_response
        client.post.side_effect = [
            httpx.ConnectError("refused"),
            MagicMock(),
            MagicMock(),
        ]

        with (
            patch("fintl.common.extraction.unload.httpx.Client") as client_cls,
            patch("fintl.common.extraction.unload.check_ollama_availability"),
        ):
            client_cls.return_value.__enter__.return_value = client
            unload_ollama(ollama_config, TIMEOUT)

        assert client.post.call_count == 3
