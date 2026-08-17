"""Shared pytest fixtures and hooks for the test suite."""

import os
from pathlib import Path

import pytest

os.environ["FINTL_CONFIG"] = str(Path(__file__).parent / ".pytest-fintl-config.toml")


@pytest.fixture
def logger_config_path() -> Path:
    """Return the path to the logger configuration JSON file."""
    return Path(__file__).parent / "logger-config.json"


@pytest.fixture
def files_root_path() -> Path:
    """Return the path to the test fixture files root directory."""
    return Path(__file__).parent / "files"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    """Skip tests marked with ollama unless the ollama mark expression is active."""
    markexpr = getattr(config.option, "markexpr", "") or ""
    if "ollama" in markexpr:  # pragma: no cover
        return
    skip = pytest.mark.skip(reason="requires Ollama; run with: pytest -m ollama")
    for item in items:
        if item.get_closest_marker("ollama"):
            item.add_marker(skip)


@pytest.fixture
def png_fname() -> str:
    """Return the PNG fixture filename for broker20260309 tests."""
    return "Screenshot 2026-04-27 at 08.20.00.png"


@pytest.fixture
def png_file(files_root_path: Path, png_fname: str) -> Path:
    """Return the full path to the broker20260309 PNG fixture file."""
    return files_root_path / "artefacts" / "Scalable-Capital" / png_fname
