"""Shared extraction model and Instructor inference utilities."""

import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import cast

import instructor
from instructor.processing.multimodal import Image as InstructorImage

from fintl.common.extraction.context import SYSTEM_PROMPT, BalanceInfoExtract
from fintl.common.extraction.errors import InferenceError
from fintl.common.extraction.types import ExtractionOutput, ExtractionResponse


def _get_extraction(
    file_path: Path, extraction_client: instructor.Instructor, model: str, timeout: int
) -> ExtractionResponse:
    """Run LM inference to extract balance information from an image file."""
    from instructor.core.exceptions import InstructorRetryException

    try:
        res = extraction_client.create_with_completion(  # type: ignore
            model=model,
            response_model=BalanceInfoExtract,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
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
        raise InferenceError(f"Inference failed for {file_path.name}: {last}") from None


class ExtractionModel(ABC):
    """Base class for extraction models with shared prediction behavior."""

    model: str
    base_url: str
    client: instructor.Instructor
    timeout: int

    def __init__(self, model: str, *, base_url: str, timeout: int):
        """Initialise the model and create its provider-specific Instructor client."""
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self.client = self._create_client(model=model, base_url=base_url)

    @abstractmethod
    def _create_client(self, *, model: str, base_url: str) -> instructor.Instructor:
        """Create the Instructor client for the concrete model provider."""

    def predict(self, path: Path) -> ExtractionOutput:
        """Run inference on *path* and return an ExtractionOutput with results or error info."""
        start = time.perf_counter()
        try:
            extraction, completion = _get_extraction(
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
