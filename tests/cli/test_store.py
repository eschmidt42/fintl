from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fintl.accounts_etl.schemas import Case, ParserSpec, Provider, Sources
from fintl.cli.main import app

from .conftest import make_config


def _spec(
    provider: str,
    service: str,
    parser: str,
    *,
    applies: bool = True,
    precedence: int = 0,
) -> ParserSpec:
    return ParserSpec(
        case=Case(provider=provider, service=service, parser=parser),
        applies=MagicMock(return_value=applies),
        run=MagicMock(),
        precedence=precedence,
    )


def _store_config(tmp_path: Path):
    giro_src = tmp_path / "sources" / "dkb" / "giro"
    giro_src.mkdir(parents=True)
    return make_config(tmp_path, Sources(dkb=Provider(giro=giro_src)))


def test_run_copies_matched_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner
):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    stub = downloads / "export.csv"
    stub.write_text("date,amount\n2024-01-01,100\n")

    config = _store_config(tmp_path)
    spec = _spec("dkb", "giro", "giro0", applies=True)
    monkeypatch.setattr("fintl.cli.store.Config", lambda: config)
    monkeypatch.setattr("fintl.cli.store.ALL_PARSERS", [spec])

    result = cli_runner.invoke(app, ["store", "--from-dir", str(downloads), "--yes"])

    assert result.exit_code == 0, result.output
    dest_dir = config.get_source_dir_from_case(spec.case)
    assert (dest_dir / "export.csv").exists()


def test_run_skips_unmatched_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner
):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    (downloads / "unknown.csv").write_text("data\n")

    config = _store_config(tmp_path)
    spec = _spec("dkb", "giro", "giro0", applies=False)
    monkeypatch.setattr("fintl.cli.store.Config", lambda: config)
    monkeypatch.setattr("fintl.cli.store.ALL_PARSERS", [spec])

    result = cli_runner.invoke(app, ["store", "--from-dir", str(downloads), "--yes"])

    assert result.exit_code == 0, result.output
    assert "Unmatched: 1" in result.output


def test_run_already_copied_file_is_not_duplicated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cli_runner
):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    stub = downloads / "export.csv"
    stub.write_text("date,amount\n2024-01-01,100\n")

    config = _store_config(tmp_path)
    spec = _spec("dkb", "giro", "giro0", applies=True)
    monkeypatch.setattr("fintl.cli.store.Config", lambda: config)
    monkeypatch.setattr("fintl.cli.store.ALL_PARSERS", [spec])

    # first run copies the file (using --copy so source dir is preserved for the second run)
    cli_runner.invoke(app, ["store", "--from-dir", str(downloads), "--yes", "--copy"])
    # second run: file already exists in raw dir → skipped, not duplicated
    result = cli_runner.invoke(
        app, ["store", "--from-dir", str(downloads), "--yes", "--copy"]
    )

    assert result.exit_code == 0, result.output
    assert "Copied: 0" in result.output
    assert "Skipped: 1" in result.output
