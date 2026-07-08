"""Collecting of logic to check model provider and model availability."""

import logging

from fintl.common import OllamaConfig
from fintl.common.extraction.errors import (
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)
from fintl.common.extraction.ollama import (
    check_model_availability as check_ollama_model_availability,
)
from fintl.common.extraction.ollama import (
    check_provider_availability as check_ollama_availability,
)

logger = logging.getLogger(__name__)


def check_ollama_ok(
    ollama_config: OllamaConfig | None,
) -> bool:
    """Testing aspects for ollama availability."""
    if ollama_config is None:
        logger.warning("Ollama configuration missing, aborting PNG parsing.")
        return False

    try:
        check_ollama_availability(ollama_config.base_url)
    except OllamaUnavailableError as exc:
        logger.warning("Ollama is not available, aborting PNG parsing: %s", exc)
        return False

    try:
        check_ollama_model_availability(ollama_config.base_url, ollama_config.model)
    except OllamaModelUnavailableError as exc:
        logger.warning(
            "Ollama model (%s) not available, aborting PNG parsing: %s",
            ollama_config.model,
            exc,
        )
        return False

    return True
