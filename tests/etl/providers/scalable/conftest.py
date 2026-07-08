"""Shared pytest fixtures for Scalable Capital provider tests."""

from pathlib import Path

import pytest


@pytest.fixture
def png_fname() -> str:
    """Return the PNG fixture filename for broker20260309 tests."""
    return "Screenshot 2026-04-27 at 08.20.00.png"


@pytest.fixture
def png_file(files_root_path: Path, png_fname: str) -> Path:
    """Return the full path to the broker20260309 PNG fixture file."""
    return files_root_path / "artefacts" / "Scalable-Capital" / png_fname
