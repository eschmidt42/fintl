"""llama-swap-backed extraction utilities for Scalable Capital broker screenshots."""

import time
from pathlib import Path
from typing import cast

import httpx
import instructor
from instructor.processing.multimodal import Image as InstructorImage
from openai import OpenAI

from fintl.common.extraction.constants import LLAMA_SWAP_URL, TIMEOUT
from fintl.common.extraction.context import _SYSTEM_PROMPT, _BalanceInfoExtract
from fintl.common.extraction.errors import InferenceError
from fintl.common.extraction.types import ExtractionOutput, ExtractionResponse


def check_health(client: httpx.Client) -> bool:
    """GET /health — llama-swap-native, returns 'OK' on 200."""
    r = client.get("/health", timeout=5.0)
    r.raise_for_status()
    body = r.text.strip()
    print(f"  /health -> {r.status_code} '{body}'")
    return r.status_code == 200 and body == "OK"


def check_model_available(client: httpx.Client, model: str) -> bool:
    """GET /v1/models — OpenAI-compatible list; look for our model id in data[].id."""
    r = client.get("/v1/models")
    r.raise_for_status()
    payload = r.json()
    ids = [m["id"] for m in payload.get("data", [])]
    print(f"  /v1/models -> {len(ids)} model(s): {ids}")
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
    print(f"  /v1/chat/completions -> '{content}'")
    return bool(content)


def sanity_check(model: str, *, timeout: int = TIMEOUT, base_url: str = LLAMA_SWAP_URL) -> bool:
    """Run health, model-availability, and inference checks against the llama-swap server."""
    print(f"llama-swap sanity check @ {base_url} (model='{model}')")
    with httpx.Client(base_url=base_url, timeout=timeout) as client:
        try:
            assert check_health(client), "health check failed"
            assert check_model_available(client, model), f"model '{model}' not in /v1/models"
            assert check_inference(client, model), "inference produced no content"
        except (httpx.HTTPError, AssertionError, KeyError) as e:
            print(f"  ✗ FAILED: {e}")
            return False
    print("  ✓ All checks passed")
    return True


def _get_llama_swap_extraction(
    file_path: Path, extraction_client: instructor.Instructor, model: str, timeout: int
) -> ExtractionResponse:
    """Run LM inference to extract balance information from an image file."""
    from instructor.core.exceptions import InstructorRetryException

    try:
        res = extraction_client.create_with_completion(  # type: ignore
            model=model,
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
        raise InferenceError(f"llama-swap inference failed for {file_path.name}: {last}") from None

    else:
        msg = "No idea how we got here, but the _get_lm_extraction failed."
        raise RuntimeError(msg)


class LLamaSwapExtractionModel:
    """Extraction model that delegates inference to a llama-swap server."""

    model: str
    base_url: str
    client: instructor.Instructor
    timeout: int

    def __init__(self, model: str, *, base_url: str = LLAMA_SWAP_URL, timeout: int = 2 * 60):
        """Initialise the llama-swap extraction model and create the instructor client."""
        self.model = model
        self.base_url = base_url
        self.timeout = timeout

        self.client = instructor.from_openai(
            OpenAI(base_url=f"{base_url}/v1", api_key="not-needed"),
        )

    def predict(self, path: Path) -> ExtractionOutput:
        """Run inference on *path* and return an ExtractionOutput with results or error info."""
        start = time.perf_counter()
        try:
            extraction, completion = _get_llama_swap_extraction(
                file_path=path,
                extraction_client=self.client,
                model=self.model,
                timeout=self.timeout,
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
