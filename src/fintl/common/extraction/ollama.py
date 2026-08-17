"""Ollama-backed extraction utilities for Scalable Capital broker screenshots."""

import logging
import time
from pathlib import Path
from typing import cast

import httpx
import instructor
from instructor.processing.multimodal import Image as InstructorImage

from fintl.common.extraction.constants import OLLAMA_BASE_URL
from fintl.common.extraction.context import _SYSTEM_PROMPT, _BalanceInfoExtract
from fintl.common.extraction.errors import (
    InferenceError,
    OllamaModelUnavailableError,
    OllamaUnavailableError,
)
from fintl.common.extraction.types import ExtractionOutput, ExtractionResponse

logger = logging.getLogger(__name__)


def check_provider_availability(base_url: str) -> None:
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


def check_model_availability(base_url: str, model: str) -> None:
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


def _get_client(
    *, model: str, ollama_base_url: str = "http://localhost:11434/v1"
) -> instructor.Instructor:
    """Create and return an Instructor client configured for the given ollama model."""
    return instructor.from_provider(
        f"ollama/{model}",
        base_url=ollama_base_url,
        mode=instructor.Mode.JSON,
        async_client=False,
    )


def _get_extraction(
    file_path: Path, extraction_client: instructor.Instructor, timeout: int
) -> ExtractionResponse:
    """Run LM inference using ollama to extract balance information from an image file."""
    from instructor.core.exceptions import InstructorRetryException

    try:
        res = extraction_client.create_with_completion(  # type: ignore
            response_model=_BalanceInfoExtract,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        "Please extract data from the following image",
                        InstructorImage.from_path(file_path),
                    ],
                },  # type: ignore[arg-type]
            ],
            timeout=timeout,
        )

        return cast(ExtractionResponse, res)
    except InstructorRetryException as exc:
        last = exc.failed_attempts[-1].exception if exc.failed_attempts else exc
        # explicitly cutting of the traceback here for readability.
        # remove `from None` if you need to debug.
        raise InferenceError(f"Ollama inference failed for {file_path.name}: {last}") from None

    else:
        msg = "No idea how we got here, but the _get_lm_extraction failed."
        raise RuntimeError(msg)


class OllamaExtractionModel:
    """Extraction model that delegates inference to a local ollama instance."""

    model: str
    base_url: str
    client: instructor.Instructor
    timeout: int

    def __init__(self, model: str, *, base_url: str = OLLAMA_BASE_URL, timeout: int = 2 * 60):
        """Initialise the ollama extraction model and create the instructor client."""
        self.model = model
        self.base_url = base_url
        self.timeout = timeout

        self.client = _get_client(model=model, ollama_base_url=base_url)

    def predict(self, path: Path) -> ExtractionOutput:
        """Run inference on *path* and return an ExtractionOutput with results or error info."""
        start = time.perf_counter()
        try:
            extraction, completion = _get_extraction(
                file_path=path, extraction_client=self.client, timeout=self.timeout
            )
            ok = True
            error_message = ""
        except InferenceError as ex:
            extraction, completion = None, None
            ok = False
            error_message = str(ex)

        elapsed = time.perf_counter() - start
        return ExtractionOutput(
            extraction=extraction,
            completion=completion,
            elapsed=elapsed,
            ok=ok,
            error_message=error_message,
        )
