from pathlib import Path

from fintl.accounts_etl import runner
from fintl.accounts_etl.scalable import broker0, broker20231028
from fintl.accounts_etl.scalable.files import get_parser_source_files
from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources


def test_broker_parsers_apply(tmp_path: Path):
    data_root_dir = Path(__file__).parent.parent / "files"
    assert data_root_dir.exists()
    html_root_dir = data_root_dir / "html_files"
    assert html_root_dir.exists()

    scalable_broker_source_dir = html_root_dir / "Scalable-Capital"

    assert scalable_broker_source_dir.exists()

    logger_path = (
        Path(__file__).parent.parent.parent / "fine_logging" / "logger-config.json"
    )
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
