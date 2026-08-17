"""Unit tests for fintl.common.extraction.unload."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from fintl.common.config import LlamaSwapConfig
from fintl.common.extraction.unload import unload_llama_swap

BASE_URL = "http://0.0.0.0:8080"
TIMEOUT = 30


@pytest.fixture
def llama_swap_config() -> LlamaSwapConfig:
    """Build a reusable llama-swap config for unload tests."""
    return LlamaSwapConfig(model="some-model", base_url=BASE_URL)


def test_not_running_skips_unload(llama_swap_config: LlamaSwapConfig):
    """Skips the unload POST when the health check reports llama-swap is not running."""
    with (
        patch(
            "fintl.common.extraction.unload.check_llama_swap_availability",
            return_value=False,
        ),
        patch("fintl.common.extraction.unload.httpx.Client.post") as mock_post,
    ):
        unload_llama_swap(llama_swap_config, TIMEOUT)

    mock_post.assert_not_called()


def test_no_config_defaults_to_expected_default_calls():
    """Uses the default llama-swap endpoint values when no config is provided."""
    mock_health_response = MagicMock()
    mock_health_response.status_code = 200
    mock_health_response.text = "OK"

    mock_unload_response = MagicMock()

    with (
        patch(
            "fintl.common.extraction.unload.httpx.Client.get", return_value=mock_health_response
        ) as mock_get,
        patch(
            "fintl.common.extraction.unload.httpx.Client.post", return_value=mock_unload_response
        ) as mock_post,
    ):
        unload_llama_swap(None, TIMEOUT)

    mock_get.assert_called_once_with("/health", timeout=5.0)
    mock_post.assert_called_once_with("/api/models/unload", timeout=TIMEOUT)


def test_success_posts_to_correct_url(llama_swap_config: LlamaSwapConfig):
    """POSTs to <base_url>/api/models/unload with the configured timeout."""
    mock_response = MagicMock()
    with (
        patch(
            "fintl.common.extraction.unload.check_llama_swap_availability",
            return_value=True,
        ),
        patch(
            "fintl.common.extraction.unload.httpx.Client.post",
            return_value=mock_response,
        ) as mock_post,
    ):
        unload_llama_swap(llama_swap_config, TIMEOUT)

    mock_post.assert_called_once_with("/api/models/unload", timeout=TIMEOUT)
    mock_response.raise_for_status.assert_called_once()


def test_http_error_on_unload_is_swallowed(llama_swap_config: LlamaSwapConfig):
    """Logs a warning and returns normally (does not raise) when the unload POST fails."""
    with (
        patch(
            "fintl.common.extraction.unload.check_llama_swap_availability",
            return_value=True,
        ),
        patch(
            "fintl.common.extraction.unload.httpx.Client.post",
            side_effect=httpx.HTTPStatusError(
                "500 Internal Server Error",
                request=MagicMock(),
                response=MagicMock(),
            ),
        ),
    ):
        unload_llama_swap(llama_swap_config, TIMEOUT)  # must not raise


def test_connect_error_on_unload_is_swallowed(llama_swap_config: LlamaSwapConfig):
    """Logs a warning and returns normally when the unload POST cannot connect."""
    with (
        patch(
            "fintl.common.extraction.unload.check_llama_swap_availability",
            return_value=True,
        ),
        patch(
            "fintl.common.extraction.unload.httpx.Client.post",
            side_effect=httpx.ConnectError("Connection refused"),
        ),
    ):
        unload_llama_swap(llama_swap_config, TIMEOUT)  # must not raise
