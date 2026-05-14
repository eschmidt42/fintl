from pathlib import Path

import pytest


@pytest.fixture
def logger_config_path() -> Path:
    return Path(__file__).parent / "logger-config.json"


@pytest.fixture
def files_root_path() -> Path:
    return Path(__file__).parent / "files"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    markexpr = getattr(config.option, "markexpr", "") or ""
    if "ollama" in markexpr:  # pragma: no cover
        return
    skip = pytest.mark.skip(reason="requires Ollama; run with: pytest -m ollama")
    for item in items:
        if item.get_closest_marker("ollama"):
            item.add_marker(skip)
