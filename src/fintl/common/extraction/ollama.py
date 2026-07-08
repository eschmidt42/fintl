"""Ollama-backed extraction utilities for Scalable Capital broker screenshots."""

from pathlib import Path

import httpx
import instructor
from instructor.processing.multimodal import Image

from fintl.common.extraction.context import _SYSTEM_PROMPT, _BalanceInfoExtract
from fintl.common.extraction.errors import (
    OllamaInferenceError,
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)


def _check_ollama_availability(base_url: str) -> None:
    """Check that the ollama server is reachable.

    Strips the ``/v1`` suffix (if present) to reach the ollama root endpoint
    and performs a GET with a short timeout.

    Raises:
        OllamaUnavailableError: when the server cannot be reached.
    """
    root_url = base_url.rstrip("/")
    if root_url.endswith("/v1"):
        root_url = root_url[:-3]
    try:
        httpx.get(root_url, timeout=5.0).raise_for_status()
    except Exception as exc:
        raise OllamaUnavailableError(f"Ollama is not reachable at {base_url}: {exc}") from exc


def _check_model_available(base_url: str, model: str) -> None:
    """Check that *model* has been pulled into the local ollama instance.

    Calls ``GET {root}/api/tags`` and inspects the returned model list.
    Model names returned by ollama may include a tag suffix (e.g. ``":latest"``);
    if *model* contains no ``:``, a bare-name match against the part before
    ``:`` is also accepted.

    Raises:
        OllamaModelUnavailableError: when the model is not found.
    """
    root_url = base_url.rstrip("/")
    if root_url.endswith("/v1"):
        root_url = root_url[:-3]
    try:
        response = httpx.get(f"{root_url}/api/tags", timeout=5.0)
        response.raise_for_status()
        available = [m["name"] for m in response.json().get("models", [])]
    except Exception as exc:
        raise OllamaModelUnavailableError(
            f"Could not retrieve model list from ollama at {base_url}: {exc}"
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


def _get_ollama_client(
    *, model: str, ollama_base_url: str = "http://localhost:11434/v1"
) -> instructor.Instructor:
    """Create and return an Instructor client configured for the given ollama model."""
    return instructor.from_provider(
        f"ollama/{model}",
        base_url=ollama_base_url,
        mode=instructor.Mode.JSON,
        async_client=False,
    )


def _get_lm_extraction(
    file_path: Path, extraction_client: instructor.Instructor
) -> _BalanceInfoExtract:
    """Run LM inference to extract balance information from an image file."""
    from instructor.core.exceptions import InstructorRetryException

    try:
        return extraction_client.create(  # type: ignore
            response_model=_BalanceInfoExtract,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        "Please extract data from the following image",
                        Image.from_path(file_path),
                    ],
                },  # type: ignore[arg-type]
            ],
        )
    except InstructorRetryException as exc:
        last = exc.failed_attempts[-1].exception if exc.failed_attempts else exc
        # explicitly cutting of the traceback here for readability.
        # remove `from None` if you need to debug.
        raise OllamaInferenceError(
            f"Ollama inference failed for {file_path.name}: {last}"
        ) from None
