"""Ollama-backed extraction utilities for Scalable Capital broker screenshots."""

import logging

import httpx
import instructor

from fintl.common.extraction.constants import OLLAMA_BASE_URL
from fintl.common.extraction.core import ExtractionModel
from fintl.common.extraction.errors import (
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)

logger = logging.getLogger(__name__)


def check_provider_availability(client: httpx.Client) -> None:
    """Check that the ollama server is reachable.

    Performs a GET against the root endpoint with a short timeout.

    Raises:
        OllamaUnavailableError: when the server cannot be reached.
    """
    try:
        client.get("/", timeout=5.0).raise_for_status()
    except Exception as exc:
        raise OllamaUnavailableError(f"Ollama is not reachable: {exc}") from exc


def check_model_availability(client: httpx.Client, model: str) -> None:
    """Check that *model* has been pulled into the local ollama instance.

    Calls ``GET {root}/api/tags`` and inspects the returned model list.
    Model names returned by ollama may include a tag suffix (e.g. ``":latest"``);
    if *model* contains no ``:``, a bare-name match against the part before
    ``:`` is also accepted.

    Raises:
        OllamaModelUnavailableError: when the model is not found.
    """
    try:
        response = client.get("/api/tags", timeout=5.0)
        response.raise_for_status()
        available = [m["name"] for m in response.json().get("models", [])]
    except Exception as exc:
        raise OllamaModelUnavailableError(
            f"Could not retrieve model list from ollama: {exc}"
        ) from exc

    # exact match first; then fall back to bare-name match when model has no tag
    if model in available:
        return
    if ":" not in model:
        bare_names = {m.split(":")[0] for m in available}
        if model in bare_names:
            return

    raise OllamaModelUnavailableError(
        f"Model '{model}' is not available in ollama. Pull it first with: ollama pull {model}"
    )


def v1ify(url: str, *, suffix: str = "/v1") -> str:
    """Append *suffix* to *url* unless it is already present."""
    if url.endswith(suffix):
        return url
    v1_url = f"{url.rstrip('/')}{suffix}"
    return v1_url


def _get_client(*, model: str, ollama_base_url: str = OLLAMA_BASE_URL) -> instructor.Instructor:
    """Create and return an Instructor client configured for the given ollama model."""
    v1_url = v1ify(ollama_base_url)
    return instructor.from_provider(
        f"ollama/{model}",
        base_url=v1_url,
        mode=instructor.Mode.TOOLS,
        async_client=False,
    )


class OllamaExtractionModel(ExtractionModel):
    """Extraction model that delegates inference to a local ollama instance."""

    def __init__(self, model: str, *, base_url: str = OLLAMA_BASE_URL, timeout: int = 2 * 60):
        """Initialise the ollama extraction model and create the instructor client."""
        super().__init__(model, base_url=base_url, timeout=timeout)

    def _create_client(self, *, model: str, base_url: str) -> instructor.Instructor:
        """Create an Instructor client configured for the given ollama model."""
        return _get_client(model=model, ollama_base_url=base_url)
