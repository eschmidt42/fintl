import datetime
import os
from pathlib import Path

import pytest

from fintl.accounts_etl.scalable import broker20260309 as broker
from fintl.accounts_etl.schemas import OllamaConfig

_PNG_PATH = (
    Path(__file__).parent.parent
    / "files"
    / "png_files"
    / "Scalable-Capital"
    / "Screenshot 2026-04-27 at 08.20.00.png"
)


@pytest.fixture
def real_ollama_config() -> OllamaConfig:
    model = os.environ.get("FINTL_OLLAMA_MODEL")
    if not model:
        pytest.skip("FINTL_OLLAMA_MODEL env var not set")
    base_url = os.environ.get("FINTL_OLLAMA_BASE_URL", "http://localhost:11434/v1")
    return OllamaConfig(model=model, base_url=base_url)


@pytest.mark.ollama
def test_extract_balance_with_real_ollama(real_ollama_config: OllamaConfig) -> None:
    """Verify that extract_balance returns a valid BalanceInfo from a real Ollama call."""
    assert _PNG_PATH.exists(), f"fixture PNG missing: {_PNG_PATH}"

    result = broker.extract_balance(
        broker.CASE, _PNG_PATH, ollama_config=real_ollama_config
    )

    assert result.date == datetime.date(2026, 4, 27)
    assert isinstance(result.amount, float)
    assert result.currency  # non-empty string
    assert result.provider == broker.CASE.provider
    assert result.service == broker.CASE.service
    assert result.parser == broker.CASE.parser
