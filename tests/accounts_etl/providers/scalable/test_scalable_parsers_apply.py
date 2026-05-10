from pathlib import Path

import pytest

from fintl.accounts_etl.common.schemas import Config, Logging, Provider, Sources
from fintl.accounts_etl.engine import runner
from fintl.accounts_etl.providers.scalable import broker0, broker20231028
from fintl.accounts_etl.providers.scalable.files import get_parser_source_files


@pytest.fixture
def png_fname() -> str:
    return "Screenshot 2026-04-27 at 08.20.00.png"


@pytest.fixture
def png_file(files_root_path: Path, png_fname: str) -> Path:
    return files_root_path / "artefacts" / "Scalable-Capital" / png_fname


def test_files_exist(files_root_path: Path, png_file: Path):
    assert files_root_path.exists()
    assert png_file.exists()


def test_broker_parsers_apply(tmp_path: Path, png_file: Path, logger_config_path: Path):

    scalable_broker_source_dir = png_file.parent

    logger_path = logger_config_path
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(scalable=Provider(broker=scalable_broker_source_dir)),
        logging=Logging(config_file=logger_path),
    )

    source_files_broker0 = get_parser_source_files(
        broker0.CASE, config, broker0.check_if_parser_applies
    )
    assert len(source_files_broker0) == 1

    source_files_broker20231028 = get_parser_source_files(
        broker20231028.CASE, config, broker20231028.check_if_parser_applies
    )
    assert len(source_files_broker20231028) == 1

    runner.check_service_overlap(config, "scalable", "broker")
