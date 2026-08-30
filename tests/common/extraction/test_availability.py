"""Unit tests for fintl.common.extraction.availability module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fintl.common import Config, OllamaConfig, Provider, Sources
from fintl.common.extraction.availability import (
    check_llama_swap_ok,
    check_ollama_ok,
)
from fintl.common.extraction.errors import (
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)


@pytest.fixture
def ollama_config() -> OllamaConfig:
    """Build a reusable ollama config for tests."""
    return OllamaConfig(model="test-model", base_url="http://localhost:11434")


@pytest.fixture
def llama_swap_config(tmp_path: Path) -> Config:
    """Build a reusable config with llama-swap settings."""
    from fintl.common.config import LlamaSwapConfig

    llama_swap = LlamaSwapConfig(model="test-model", base_url="http://localhost:8000")
    return Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider()),
        llama_swap=llama_swap,
    )


class TestCheckOllamaOk:
    """Tests for check_ollama_ok function."""

    def test_config_is_none_returns_false_and_logs_warning(self) -> None:
        """Returns False and logs warning when config is None."""
        with patch("fintl.common.extraction.availability.logger") as mock_logger:
            result = check_ollama_ok(None)

        assert result is False
        mock_logger.warning.assert_called_once()
        assert "configuration missing" in mock_logger.warning.call_args[0][0].lower()

    def test_ollama_unavailable_returns_false_and_logs_warning(
        self, ollama_config: OllamaConfig
    ) -> None:
        """Returns False and logs warning when ollama is unreachable."""
        error = OllamaUnavailableError("Connection refused")

        with (
            patch(
                "fintl.common.extraction.availability.check_ollama_availability",
                side_effect=error,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_ollama_ok(ollama_config)

        assert result is False
        mock_logger.warning.assert_called_once()
        assert "not available" in mock_logger.warning.call_args[0][0].lower()

    def test_ollama_model_unavailable_returns_false_and_logs_warning(
        self, ollama_config: OllamaConfig
    ) -> None:
        """Returns False and logs warning when model is not available."""
        error = OllamaModelUnavailableError("Model not found")

        with (
            patch("fintl.common.extraction.availability.check_ollama_availability"),
            patch(
                "fintl.common.extraction.availability.check_ollama_model_availability",
                side_effect=error,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_ollama_ok(ollama_config)

        assert result is False
        mock_logger.warning.assert_called_once()
        assert "model" in mock_logger.warning.call_args[0][0].lower()
        assert ollama_config.model in mock_logger.warning.call_args[0]

    def test_all_checks_pass_returns_true(self, ollama_config: OllamaConfig) -> None:
        """Returns True when all checks pass."""
        with (
            patch("fintl.common.extraction.availability.check_ollama_availability"),
            patch("fintl.common.extraction.availability.check_ollama_model_availability"),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_ollama_ok(ollama_config)

        assert result is True
        mock_logger.warning.assert_not_called()

    def test_passes_correct_base_url_to_availability_check(
        self, ollama_config: OllamaConfig
    ) -> None:
        """Passes the config's base_url to check_ollama_availability."""
        with (
            patch(
                "fintl.common.extraction.availability.check_ollama_availability"
            ) as mock_check_avail,
            patch("fintl.common.extraction.availability.check_ollama_model_availability"),
        ):
            check_ollama_ok(ollama_config)

        client = mock_check_avail.call_args.args[0]
        assert client.base_url == ollama_config.base_url

    def test_passes_correct_base_url_and_model_to_model_check(
        self, ollama_config: OllamaConfig
    ) -> None:
        """Passes both base_url and model name to check_ollama_model_availability."""
        with (
            patch("fintl.common.extraction.availability.check_ollama_availability"),
            patch(
                "fintl.common.extraction.availability.check_ollama_model_availability"
            ) as mock_check_model,
        ):
            check_ollama_ok(ollama_config)

        client, model = mock_check_model.call_args.args
        assert client.base_url == ollama_config.base_url
        assert model == ollama_config.model

    def test_model_name_included_in_unavailable_warning(self, ollama_config: OllamaConfig) -> None:
        """Model name is included in the warning log when model is unavailable."""
        error = OllamaModelUnavailableError("Not found")

        with (
            patch("fintl.common.extraction.availability.check_ollama_availability"),
            patch(
                "fintl.common.extraction.availability.check_ollama_model_availability",
                side_effect=error,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            check_ollama_ok(ollama_config)

        # Check that the model name appears in the logged arguments
        logged_args = mock_logger.warning.call_args[0]
        assert ollama_config.model in logged_args

    def test_ollama_availability_error_message_logged(self, ollama_config: OllamaConfig) -> None:
        """The exception message from OllamaUnavailableError is logged."""
        error_msg = "Ollama server connection failed"
        error = OllamaUnavailableError(error_msg)

        with (
            patch(
                "fintl.common.extraction.availability.check_ollama_availability",
                side_effect=error,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            check_ollama_ok(ollama_config)

        # Check that the error message is in the logged output
        logged_args = mock_logger.warning.call_args[0]
        assert error_msg in str(logged_args)


class TestCheckLlamaSwapOk:
    """Tests for check_llama_swap_ok function."""

    def test_llama_swap_config_is_none_returns_false_and_logs_warning(self, tmp_path: Path) -> None:
        """Returns False and logs warning when config.llama_swap is None."""
        config = Config(
            target_dir=tmp_path,
            sources=Sources(dkb=Provider()),
            llama_swap=None,
        )

        with patch("fintl.common.extraction.availability.logger") as mock_logger:
            result = check_llama_swap_ok(config, do_inference_check=False)

        assert result is False
        mock_logger.warning.assert_called_once()
        assert "configuration missing" in mock_logger.warning.call_args[0][0].lower()

    def test_health_check_fails_returns_false_and_logs_warning(
        self, llama_swap_config: Config
    ) -> None:
        """Returns False and logs warning when llama-swap health check fails."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=False,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        assert result is False
        mock_logger.warning.assert_called_once()
        assert "not available" in mock_logger.warning.call_args[0][0].lower()

    def test_model_availability_check_fails_returns_false_and_logs_warning(
        self, llama_swap_config: Config
    ) -> None:
        """Returns False and logs warning when model is not available."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=False,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        assert result is False
        mock_logger.warning.assert_called_once()
        assert "model" in mock_logger.warning.call_args[0][0].lower()

    def test_model_name_included_in_availability_warning(self, llama_swap_config: Config) -> None:
        """Model name is included in the warning when model is unavailable."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=False,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        logged_args = mock_logger.warning.call_args[0]
        llama_swap = llama_swap_config.llama_swap
        assert llama_swap is not None
        assert llama_swap.model in logged_args

    def test_inference_check_disabled_returns_true_when_health_and_model_pass(
        self, llama_swap_config: Config
    ) -> None:
        """Returns True when do_inference_check=False and health + model checks pass."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        assert result is True
        mock_logger.warning.assert_not_called()

    def test_inference_check_enabled_but_fails_returns_false_and_logs_warning(
        self, llama_swap_config: Config
    ) -> None:
        """Returns False and logs warning when do_inference_check=True and inference fails."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference", return_value=False
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_llama_swap_ok(llama_swap_config, do_inference_check=True)

        assert result is False
        mock_logger.warning.assert_called_once()
        assert "inference" in mock_logger.warning.call_args[0][0].lower()

    def test_inference_check_model_name_in_warning(self, llama_swap_config: Config) -> None:
        """Model name is included in the warning when inference fails."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference", return_value=False
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=True)

        logged_args = mock_logger.warning.call_args[0]
        llama_swap = llama_swap_config.llama_swap
        assert llama_swap is not None
        assert llama_swap.model in logged_args

    def test_all_checks_pass_without_inference_returns_true(
        self, llama_swap_config: Config
    ) -> None:
        """Returns True when all checks pass with do_inference_check=False."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        assert result is True
        mock_logger.warning.assert_not_called()

    def test_all_checks_pass_with_inference_returns_true(self, llama_swap_config: Config) -> None:
        """Returns True when all checks pass with do_inference_check=True."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference", return_value=True
            ),
            patch("fintl.common.extraction.availability.logger") as mock_logger,
        ):
            result = check_llama_swap_ok(llama_swap_config, do_inference_check=True)

        assert result is True
        mock_logger.warning.assert_not_called()

    def test_creates_httpx_client_with_correct_base_url(self, llama_swap_config: Config) -> None:
        """httpx.Client is created with config's base_url."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as mock_client,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        mock_client.assert_called_once()
        call_kwargs = mock_client.call_args.kwargs
        llama_swap = llama_swap_config.llama_swap
        assert llama_swap is not None
        assert call_kwargs["base_url"] == llama_swap.base_url

    def test_creates_httpx_client_with_correct_timeout(self, llama_swap_config: Config) -> None:
        """httpx.Client is created with config's model_timeout."""
        with (
            patch("fintl.common.extraction.availability.httpx.Client") as mock_client,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        mock_client.assert_called_once()
        call_kwargs = mock_client.call_args.kwargs
        assert call_kwargs["timeout"] == llama_swap_config.model_timeout

    def test_uses_httpx_client_as_context_manager(self, llama_swap_config: Config) -> None:
        """httpx.Client is used as a context manager."""
        mock_client = MagicMock()

        with (
            patch(
                "fintl.common.extraction.availability.httpx.Client", return_value=mock_client
            ) as _,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        # Verify context manager was used
        mock_client.__enter__.assert_called_once()
        mock_client.__exit__.assert_called_once()

    def test_passes_client_to_health_check(self, llama_swap_config: Config) -> None:
        """The httpx.Client is passed to check_llamaswap_availability."""
        mock_client = MagicMock()

        with (
            patch("fintl.common.extraction.availability.httpx.Client", return_value=mock_client),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability"
            ) as mock_health_check,
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        # Verify client from __enter__ was passed to health check
        mock_health_check.assert_called_once()
        passed_client = mock_health_check.call_args[0][0]
        # Verify it's the one returned by __enter__
        assert passed_client is mock_client.__enter__.return_value

    def test_passes_client_and_model_to_model_check(self, llama_swap_config: Config) -> None:
        """The httpx.Client and model name are passed to check_llamaswap_model_availability."""
        mock_client = MagicMock()

        with (
            patch("fintl.common.extraction.availability.httpx.Client", return_value=mock_client),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability"
            ) as mock_model_check,
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        # Verify client and model were passed
        mock_model_check.assert_called_once()
        passed_client = mock_model_check.call_args[0][0]
        passed_model = mock_model_check.call_args[0][1]
        assert passed_client is mock_client.__enter__.return_value
        llama_swap = llama_swap_config.llama_swap
        assert llama_swap is not None
        assert passed_model == llama_swap.model

    def test_passes_client_and_model_to_inference_check(self, llama_swap_config: Config) -> None:
        """The httpx.Client and model name are passed to check_llamaswap_inference."""
        mock_client = MagicMock()

        with (
            patch("fintl.common.extraction.availability.httpx.Client", return_value=mock_client),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference"
            ) as mock_inference_check,
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=True)

        # Verify client and model were passed
        mock_inference_check.assert_called_once()
        passed_client = mock_inference_check.call_args[0][0]
        passed_model = mock_inference_check.call_args[0][1]
        assert passed_client is mock_client.__enter__.return_value
        llama_swap = llama_swap_config.llama_swap
        assert llama_swap is not None
        assert passed_model == llama_swap.model

    def test_inference_check_not_called_when_disabled(self, llama_swap_config: Config) -> None:
        """check_llamaswap_inference is not called when do_inference_check=False."""
        mock_client = MagicMock()

        with (
            patch("fintl.common.extraction.availability.httpx.Client", return_value=mock_client),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference"
            ) as mock_inference_check,
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=False)

        # Verify inference check was not called
        mock_inference_check.assert_not_called()

    def test_inference_check_called_when_enabled(self, llama_swap_config: Config) -> None:
        """check_llamaswap_inference is called when do_inference_check=True."""
        mock_client = MagicMock()

        with (
            patch("fintl.common.extraction.availability.httpx.Client", return_value=mock_client),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference"
            ) as mock_inference_check,
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=True)

        # Verify inference check was called
        mock_inference_check.assert_called_once()

    def test_inference_skipped_if_model_check_fails(self, llama_swap_config: Config) -> None:
        """check_llamaswap_inference is not called if model availability check fails."""
        mock_client = MagicMock()

        with (
            patch("fintl.common.extraction.availability.httpx.Client", return_value=mock_client),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=True,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_model_availability",
                return_value=False,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference"
            ) as mock_inference_check,
            patch("fintl.common.extraction.availability.logger"),
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=True)

        # Inference check should not be called
        mock_inference_check.assert_not_called()

    def test_inference_skipped_if_health_check_fails(self, llama_swap_config: Config) -> None:
        """check_llamaswap_inference is not called if health check fails."""
        mock_client: MagicMock = MagicMock()

        with (
            patch("fintl.common.extraction.availability.httpx.Client", return_value=mock_client),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_availability",
                return_value=False,
            ),
            patch(
                "fintl.common.extraction.availability.check_llamaswap_inference"
            ) as mock_inference_check,
            patch("fintl.common.extraction.availability.logger"),
        ):
            check_llama_swap_ok(llama_swap_config, do_inference_check=True)

        # Inference check should not be called
        mock_inference_check.assert_not_called()
