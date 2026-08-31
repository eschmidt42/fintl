"""llama-swap-backed extraction utilities for Scalable Capital broker screenshots."""

import logging

import httpx
import instructor
from openai import OpenAI

from fintl.common.extraction.constants import LLAMA_SWAP_BASE_URL, TIMEOUT
from fintl.common.extraction.core import ExtractionModel

logger = logging.getLogger(__name__)


def check_health(client: httpx.Client) -> bool:
    """GET /health — llama-swap-native, returns 'OK' on 200."""
    r = client.get("/health", timeout=5.0)
    r.raise_for_status()
    body = r.text.strip()
    logger.debug(f"  /health -> {r.status_code} '{body}'")
    return r.status_code == 200 and body == "OK"


def check_model_available(client: httpx.Client, model: str) -> bool:
    """GET /v1/models — OpenAI-compatible list; look for our model id in data[].id."""
    r = client.get("/v1/models")
    r.raise_for_status()
    payload = r.json()
    ids = [m["id"] for m in payload.get("data", [])]
    logger.debug(f"  /v1/models -> {len(ids)} model(s): {ids}")
    return model in ids


def check_inference(client: httpx.Client, model: str) -> bool:
    """POST /v1/chat/completions with a tiny hello-world prompt.

    Using chat/completions (not /v1/completions) because:
      - it's the endpoint instructor + most clients use,
      - the 'model' field is what triggers llama-swap's hot-swap,
      - omitting 'model' makes the proxy refuse to forward.
    """
    r = client.post(
        "/v1/chat/completions",
        json={
            "model": model,  # required — drives the swap
            "messages": [{"role": "user", "content": "Say hello in one word."}],
            "max_tokens": 2000,
            "temperature": 0,
        },
    )
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"]
    logger.debug(f"  /v1/chat/completions -> '{content}'")
    return bool(content)


def sanity_check(
    model: str, *, timeout: int = TIMEOUT, base_url: str = LLAMA_SWAP_BASE_URL
) -> bool:
    """Run health, model-availability, and inference checks against the llama-swap server."""
    logger.debug(f"llama-swap sanity check @ {base_url} (model='{model}')")
    with httpx.Client(base_url=base_url, timeout=timeout) as client:
        try:
            assert check_health(client), "health check failed"
            assert check_model_available(client, model), f"model '{model}' not in /v1/models"
            assert check_inference(client, model), "inference produced no content"
        except (httpx.HTTPError, AssertionError, KeyError) as e:
            logger.debug(f"  ✗ FAILED: {e}")
            return False
    logger.debug("  ✓ All checks passed")
    return True


class LlamaSwapExtractionModel(ExtractionModel):
    """Extraction model that delegates inference to a llama-swap server."""

    def __init__(self, model: str, *, base_url: str = LLAMA_SWAP_BASE_URL, timeout: int = 2 * 60):
        """Initialise the llama-swap extraction model and create the instructor client."""
        super().__init__(model, base_url=base_url, timeout=timeout)

    def _create_client(self, *, model: str, base_url: str) -> instructor.Instructor:
        """Create an Instructor client configured for llama-swap."""
        return instructor.from_openai(
            OpenAI(base_url=f"{base_url}/v1", api_key="not-needed"),
        )
