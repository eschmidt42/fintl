import datetime
import os
from pathlib import Path

import pytest

from fintl.common import OllamaConfig
from fintl.etl.providers.scalable import broker20260309 as broker


@pytest.fixture
def png_fname() -> str:
    return "Screenshot 2026-04-27 at 08.20.00.png"


@pytest.fixture
def png_file(files_root_path: Path, png_fname: str) -> Path:
    return files_root_path / "artefacts" / "Scalable-Capital" / png_fname


def test_files_exist(files_root_path: Path, png_file: Path):
    assert files_root_path.exists()
    assert png_file.exists()


@pytest.fixture
def real_ollama_config() -> OllamaConfig:
    model = os.environ.get("FINTL_OLLAMA_MODEL")
    if not model:
        pytest.skip("FINTL_OLLAMA_MODEL env var not set")
    base_url = os.environ.get("FINTL_OLLAMA_BASE_URL", "http://localhost:11434/v1")
    return OllamaConfig(model=model, base_url=base_url)


@pytest.mark.ollama
def test_extract_balance_with_real_ollama(
    real_ollama_config: OllamaConfig, png_file
) -> None:
    """Verify that extract_balance returns a valid BalanceInfo from a real Ollama call."""

    result = broker.extract_balance(
        broker.CASE, png_file, ollama_config=real_ollama_config
    )

    assert result.date == datetime.date(2026, 4, 27)
    assert isinstance(result.amount, float)
    assert result.currency  # non-empty string
    assert result.provider == broker.CASE.provider
    assert result.service == broker.CASE.service
    assert result.parser == broker.CASE.parser
