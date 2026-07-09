"""Tests for scalable.broker20260309 requiring a real Ollama instance."""

import datetime
import os
from pathlib import Path

import pytest

from fintl.common import Config, OllamaConfig, Provider, Sources
from fintl.common.logging import Logging
from fintl.etl.providers.scalable import broker20260309 as broker


@pytest.fixture
def png_fname() -> str:
    """Return the PNG fixture filename for Ollama broker tests."""
    return "Screenshot 2026-04-27 at 08.20.00.png"


@pytest.fixture
def png_file(files_root_path: Path, png_fname: str) -> Path:
    """Return the full path to the PNG fixture file."""
    return files_root_path / "artefacts" / "Scalable-Capital" / png_fname


def test_files_exist(files_root_path: Path, png_file: Path):
    """Test that required fixture files exist."""
    assert files_root_path.exists()
    assert png_file.exists()


@pytest.fixture
def ollama_config() -> OllamaConfig:  # pragma: no cover
    """Return an OllamaConfig built from environment variables, skipping if unset."""
    model = os.environ.get("FINTL_OLLAMA_MODEL")
    if not model:
        pytest.skip("FINTL_OLLAMA_MODEL env var not set")
    base_url = os.environ.get("FINTL_OLLAMA_BASE_URL", "http://localhost:11434/v1")
    return OllamaConfig(model=model, base_url=base_url)


@pytest.fixture
def config(
    tmp_path: Path, png_file: Path, logger_config_path: Path, ollama_config: OllamaConfig
) -> Config:
    """Return a valid Config."""
    scalable_src = png_file.parent
    target_dir = tmp_path / "out"
    target_dir.mkdir()

    return Config(
        target_dir=target_dir,
        sources=Sources(scalable=Provider(broker=scalable_src)),
        logging=Logging(config_file=logger_config_path),
        ollama=ollama_config,
    )


@pytest.mark.ollama
def test_extract_balance_with_real_ollama(  # pragma: no cover
    config: Config, png_file: Path
) -> None:
    """Verify that extract_balance returns a valid BalanceInfo from a real Ollama call."""
    result = broker.extract_balance(broker.CASE, png_file, config=config)

    assert result.date == datetime.date(2026, 4, 27)
    assert isinstance(result.amount, float)
    assert result.currency  # non-empty string
    assert result.provider == broker.CASE.provider
    assert result.service == broker.CASE.service
    assert result.parser == broker.CASE.parser
