import pytest


def pytest_collection_modifyitems(config, items):
    markexpr = getattr(config.option, "markexpr", "") or ""
    if "ollama" in markexpr:
        return
    skip = pytest.mark.skip(reason="requires Ollama; run with: pytest -m ollama")
    for item in items:
        if item.get_closest_marker("ollama"):
            item.add_marker(skip)
