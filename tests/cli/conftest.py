"""Shared fixtures and helpers for CLI command tests."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from fintl.common import Config, Sources
from fintl.common.logging import Logging


@pytest.fixture
def cli_runner() -> CliRunner:
    """Return a Typer CLI test runner."""
    return CliRunner()


def make_config(tmp_path: Path, sources: Sources, logger_config_path: Path) -> Config:
    """Build a Config instance pointing at a temporary target directory."""
    target = tmp_path / "target"
    target.mkdir(parents=True, exist_ok=True)
    return Config(
        target_dir=target,
        sources=sources,
        logging=Logging(config_file=logger_config_path),
    )
