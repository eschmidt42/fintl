"""Tests verifying that Postbank parsers claim the correct source files."""

from pathlib import Path

import pytest

from fintl.common import Config, Provider, Sources
from fintl.common.logging import Logging
from fintl.etl.engine import runner
from fintl.etl.io.files.orchestrator import get_parser_source_files
from fintl.etl.providers.postbank import (
    giro0,
    giro202305,
)


@pytest.fixture
def csv_file(files_root_path: Path) -> Path:
    """Return the path to the Postbank giro fixture CSV file."""
    return (
        files_root_path
        / "csv_files"
        / "Postbank"
        / "Kontoumsaetze_123_1234567_12_20231028_083011.csv"
    )


def test_files_exist(files_root_path: Path, csv_file: Path):
    """Test that required fixture files exist."""
    assert files_root_path.exists()
    assert csv_file.exists()


def test_giro_parsers_apply(tmp_path: Path, csv_file: Path, logger_config_path: Path):
    """Test that each Postbank giro parser claims exactly the expected source files."""
    postbank_giro_source_dir = csv_file.parent

    assert postbank_giro_source_dir.exists()

    logger_path = logger_config_path
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(
            postbank=Provider(giro=postbank_giro_source_dir),
        ),
        logging=Logging(config_file=logger_path),
    )

    source_files_giro0 = get_parser_source_files(giro0.CASE, config, giro0.check_if_parser_applies)
    assert len(source_files_giro0) == 1

    source_files_giro202305 = get_parser_source_files(
        giro202305.CASE, config, giro202305.check_if_parser_applies
    )
    assert len(source_files_giro202305) == 1

    runner.check_service_overlap(config, "postbank", "giro")
