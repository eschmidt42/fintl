from pathlib import Path

from fintl.accounts_etl import runner
from fintl.accounts_etl.dkb import (
    credit0,
    giro0,
    giro202307,
    giro202312,
    tagesgeld0,
    tagesgeld202307,
    tagesgeld202312,
)
from fintl.accounts_etl.file_helper import get_parser_source_files
from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources


def test_giro_parsers_apply(tmp_path: Path):
    data_root_dir = Path(__file__).parent.parent / "files"
    assert data_root_dir.exists()
    csv_root_dir = data_root_dir / "csv_files"
    assert csv_root_dir.exists()

    dkb_giro_source_dir = csv_root_dir / "DKB" / "kontoauszug"

    assert dkb_giro_source_dir.exists()

    logger_path = (
        Path(__file__).parent.parent.parent / "fine_logging" / "logger-config.json"
    )
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(
            dkb=Provider(
                giro=dkb_giro_source_dir,
            ),
        ),
        logging=Logging(config_file=logger_path),
    )

    source_files_giro0 = get_parser_source_files(
        giro0.CASE, config, giro0.check_if_parser_applies
    )
    assert len(source_files_giro0) == 1

    source_files_giro202307 = get_parser_source_files(
        giro202307.CASE, config, giro202307.check_if_parser_applies
    )
    assert len(source_files_giro202307) == 1

    source_files_giro202312 = get_parser_source_files(
        giro202312.CASE, config, giro202312.check_if_parser_applies
    )
    assert len(source_files_giro202312) == 2

    runner.check_service_overlap(config, "dkb", "giro")


def test_tagesgeld_parsers_apply(tmp_path: Path):
    data_root_dir = Path(__file__).parent.parent / "files"
    assert data_root_dir.exists()
    csv_root_dir = data_root_dir / "csv_files"
    assert csv_root_dir.exists()

    dkb_tagesgeld_source_dir = csv_root_dir / "DKB" / "tagesgeld"

    assert dkb_tagesgeld_source_dir.exists()

    logger_path = (
        Path(__file__).parent.parent.parent / "fine_logging" / "logger-config.json"
    )
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(
            dkb=Provider(tagesgeld=dkb_tagesgeld_source_dir),
        ),
        logging=Logging(config_file=logger_path),
    )

    source_files_tagesgeld0 = get_parser_source_files(
        tagesgeld0.CASE, config, tagesgeld0.check_if_parser_applies
    )
    assert len(source_files_tagesgeld0) == 1

    source_files_tagesgeld202307 = get_parser_source_files(
        tagesgeld202307.CASE, config, tagesgeld202307.check_if_parser_applies
    )
    assert len(source_files_tagesgeld202307) == 1

    source_files_tagesgeld202312 = get_parser_source_files(
        tagesgeld202312.CASE, config, tagesgeld202312.check_if_parser_applies
    )
    assert len(source_files_tagesgeld202312) == 2

    runner.check_service_overlap(config, "dkb", "tagesgeld")


def test_credit_parsers_apply(tmp_path: Path):
    data_root_dir = Path(__file__).parent.parent / "files"
    assert data_root_dir.exists()
    csv_root_dir = data_root_dir / "csv_files"
    assert csv_root_dir.exists()

    dkb_credit_source_dir = csv_root_dir / "DKB" / "credit"

    assert dkb_credit_source_dir.exists()

    logger_path = (
        Path(__file__).parent.parent.parent / "fine_logging" / "logger-config.json"
    )
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(
            dkb=Provider(credit=dkb_credit_source_dir),
        ),
        logging=Logging(config_file=logger_path),
    )

    source_files_credit0 = get_parser_source_files(
        credit0.CASE, config, credit0.check_if_parser_applies
    )
    assert len(source_files_credit0) == 1

    runner.check_service_overlap(config, "dkb", "credit")
