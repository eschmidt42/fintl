"""Functionality for unloading models from ollama / llama-swap.

Algorithm pseudo code:

```text
if service not running:
    return

unload all models
```

"""

import logging

import httpx

from fintl.common.config import LlamaSwapConfig, OllamaConfig
from fintl.common.extraction.constants import LLAMA_SWAP_BASE_URL, OLLAMA_BASE_URL
from fintl.common.extraction.errors import OllamaUnavailableError
from fintl.common.extraction.llama_swap import check_health as check_llama_swap_availability
from fintl.common.extraction.ollama import check_provider_availability as check_ollama_availability

logger = logging.getLogger(__name__)


def unload_llama_swap(
    config: LlamaSwapConfig | None, timeout: int, unload_url: str = "/api/models/unload"
):
    """Unload all models from the local llama-swap server to free up resources.

    If llama-swap is not running then there is nothing to do.
    If llama-swap is running then unloading call is made.
    """
    base_url = config.base_url if config is not None else LLAMA_SWAP_BASE_URL
    logger.debug("Checking whether llama-swap is running at '%s'.", base_url)

    with httpx.Client(base_url=base_url, timeout=timeout) as client:
        if not check_llama_swap_availability(client):
            logger.debug("llama-swap is not running no need for further steps.")
            return

        logger.debug("Attempting unloading all llama-swap-served models via '%s'.", unload_url)

        try:
            r = client.post(unload_url, timeout=timeout)
            r.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("Failed to unload llama-swap models: %s", e)
            return

    logger.debug("Completed unloading of all llama-swap-served models via '%s'.", unload_url)


def _list_running_ollama_models(client: httpx.Client, timeout: int) -> list[str]:
    """Return model names currently loaded in ollama via GET /api/ps."""
    try:
        r = client.get("/api/ps", timeout=timeout)
        r.raise_for_status()
        return [m["name"] for m in r.json().get("models", [])]
    except httpx.HTTPError as e:
        logger.warning("Failed to list running ollama models: %s", e)
        return []


def unload_ollama(config: OllamaConfig | None, timeout: int):
    """Unload all ollama models from the local ollama server to free up resources.

    If ollama is not available, nothing to be done.

    If ollama is running:
    When *config* is ``None`` (e.g. only llama-swap is configured) the default
    ollama URL is tried and every currently-loaded model is discovered via
    ``GET /api/ps`` and unloaded, so GPU memory is freed.
    """
    base_url = config.base_url if config is not None else OLLAMA_BASE_URL
    logger.debug("Checking whether ollama is running at '%s'.", base_url)

    with httpx.Client(base_url=base_url, timeout=timeout) as client:
        try:
            check_ollama_availability(client)
        except OllamaUnavailableError:
            logger.debug("Ollama is not running, no need for further steps.")
            return

        logger.debug("Confirmed ollama is running.")
        logger.debug("Discovering running models via /api/ps.")
        models_to_unload = _list_running_ollama_models(client, timeout)

        for model in models_to_unload:
            logger.debug("Attempting unloading of ollama model '%s' via '/api/generate'.", model)
            try:
                r = client.post(
                    "/api/generate",
                    json={
                        "model": model,
                        "prompt": "",  # empty prompt
                        "keep_alive": 0,  # 0 = unload immediately
                    },
                    timeout=timeout,
                )
                r.raise_for_status()
                logger.debug("Completed unloading of ollama-served model '%s'.", model)
            except httpx.HTTPError as e:
                logger.warning("Failed to unload ollama model %s: %s", model, e)
