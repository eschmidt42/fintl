from pathlib import Path

from fintl.accounts_etl import runner
from fintl.accounts_etl.file_helper import get_parser_source_files
from fintl.accounts_etl.postbank import (
    giro0,
    giro202305,
)
from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources


def test_giro_parsers_apply(tmp_path: Path):
    data_root_dir = Path(__file__).parent.parent / "files"
    assert data_root_dir.exists()
    csv_root_dir = data_root_dir / "csv_files"
    assert csv_root_dir.exists()

    postbank_giro_source_dir = csv_root_dir / "Postbank"

    assert postbank_giro_source_dir.exists()

    logger_path = Path(__file__).parent.parent.parent / "logger-config.json"
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(
            postbank=Provider(giro=postbank_giro_source_dir),
        ),
        logging=Logging(config_file=logger_path),
    )

    source_files_giro0 = get_parser_source_files(
        giro0.CASE, config, giro0.check_if_parser_applies
    )
    assert len(source_files_giro0) == 1

    source_files_giro202305 = get_parser_source_files(
        giro202305.CASE, config, giro202305.check_if_parser_applies
    )
    assert len(source_files_giro202305) == 1

    runner.check_service_overlap(config, "postbank", "giro")
