from pathlib import Path

import pytest
from typer.testing import CliRunner

from fintl.accounts_etl.schemas import Config, Logging, Sources

_LOGGER_PATH = Path(__file__).parent.parent / "logger-config.json"


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def make_config(tmp_path: Path, sources: Sources) -> Config:
    target = tmp_path / "target"
    target.mkdir(parents=True, exist_ok=True)
    return Config(
        target_dir=target,
        sources=sources,
        logging=Logging(config_file=_LOGGER_PATH),
    )
