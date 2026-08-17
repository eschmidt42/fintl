"""Tests for scalable.broker20260309 requiring a real Ollama instance."""

import datetime
import os
from pathlib import Path

import pytest

from fintl.common import Config, LlamaSwapConfig, Provider, Sources
from fintl.common.extraction.availability import check_llama_swap_ok
from fintl.common.extraction.constants import LLAMA_SWAP_BASE_URL, ModelProvider
from fintl.common.extraction.unload import unload_llama_swap, unload_ollama
from fintl.common.logging import Logging
from fintl.etl.common.schemas import BalanceInfo
from fintl.etl.io.files.filenames import balance_htm_name_to_json
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
def llama_swap_config() -> LlamaSwapConfig:  # pragma: no cover
    """Return an OllamaConfig built from environment variables, skipping if unset."""
    model = os.environ.get("FINTL_LLAMA_SWAP_MODEL")
    if not model:
        pytest.skip("FINTL_LLAMA_SWAP_MODEL env var not set")
    base_url = os.environ.get("FINTL_LLAMA_SWAP_BASE_URL", LLAMA_SWAP_BASE_URL)
    return LlamaSwapConfig(model=model, base_url=base_url)


@pytest.fixture
def config(
    tmp_path: Path, png_file: Path, logger_config_path: Path, llama_swap_config: LlamaSwapConfig
) -> Config:
    """Return a valid Config."""
    scalable_src = png_file.parent
    target_dir = tmp_path / "out"
    target_dir.mkdir()

    return Config(
        target_dir=target_dir,
        sources=Sources(scalable=Provider(broker=scalable_src)),
        logging=Logging(config_file=logger_config_path),
        llama_swap=llama_swap_config,
        model_provider=ModelProvider.llama_swap,
    )


@pytest.mark.llama_swap
def test_parse_new_files(  # pragma: no cover
    tmp_path: Path, config: Config, png_file: Path
) -> None:
    """Verify that extract_balance returns a valid BalanceInfo from a real Ollama call."""
    unload_ollama(config.ollama, config.model_timeout)

    assert check_llama_swap_ok(config, do_inference_check=True)

    target_dir = tmp_path / "target"
    balance_html_source_paths = broker.parse_new_files(
        broker.CASE, [png_file], parsed_dir=target_dir, config=config
    )

    assert len(balance_html_source_paths) == 1
    html_path = balance_html_source_paths[0]
    assert html_path.is_file()

    json_fname = balance_htm_name_to_json(html_path)
    json_path = target_dir / json_fname
    assert json_path.exists()
    assert json_path.is_file()

    with json_path.open() as f:
        result = BalanceInfo.model_validate_json(f.read())

    assert result.date == datetime.date(2026, 4, 27)
    assert isinstance(result.amount, float)
    assert result.currency  # non-empty string
    assert result.provider == broker.CASE.provider
    assert result.service == broker.CASE.service
    assert result.parser == broker.CASE.parser

    unload_llama_swap(config.llama_swap, config.model_timeout)
