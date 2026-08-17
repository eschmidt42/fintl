"""Collecting of logic to check model provider and model availability."""

import logging

import httpx

from fintl.common import Config, OllamaConfig
from fintl.common.extraction.errors import (
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)
from fintl.common.extraction.llama_swap import check_health as check_llamaswap_availability
from fintl.common.extraction.llama_swap import check_inference as check_llamaswap_inference
from fintl.common.extraction.llama_swap import (
    check_model_available as check_llamaswap_model_availability,
)
from fintl.common.extraction.ollama import (
    check_model_availability as check_ollama_model_availability,
)
from fintl.common.extraction.ollama import (
    check_provider_availability as check_ollama_availability,
)

logger = logging.getLogger(__name__)


def check_ollama_ok(
    config: OllamaConfig | None,
) -> bool:
    """Testing aspects for ollama availability.

    Aspects:
    - ollama config available
    - ollama running
    - model available on ollama
    """
    if config is None:
        logger.warning("Ollama configuration missing, aborting PNG parsing.")
        return False

    try:
        check_ollama_availability(config.base_url)
    except OllamaUnavailableError as exc:
        logger.warning("Ollama is not available, aborting PNG parsing: %s", exc)
        return False

    try:
        check_ollama_model_availability(config.base_url, config.model)
    except OllamaModelUnavailableError as exc:
        logger.warning(
            "Ollama model (%s) not available, aborting PNG parsing: %s",
            config.model,
            exc,
        )
        return False

    return True


def check_llama_swap_ok(config: Config, do_inference_check: bool) -> bool:
    """Testing aspects for llama-swap availability.

    Aspects:
    - llama-swap config available
    - llama-swap is available
    - model available on llama-swap
    - (optional) inference functional
    """
    if config.llama_swap is None:
        logger.warning("llama-swap configuration missing, aborting PNG parsing.")
        return False

    with httpx.Client(base_url=config.llama_swap.base_url, timeout=config.model_timeout) as client:
        if not check_llamaswap_availability(client):
            logger.warning("llama-swap is not available, aborting PNG parsing.")
            return False

        if not check_llamaswap_model_availability(client, config.llama_swap.model):
            logger.warning(
                "llama-swap model %s is not available in /v1/models, aborting PNG parsing.",
                config.llama_swap.model,
            )
            return False

        if do_inference_check and not check_llamaswap_inference(client, config.llama_swap.model):
            logger.warning(
                "llama-swap model %s inference produced not output, aborting PNG parsing.",
                config.llama_swap.model,
            )
            return False

    return True
