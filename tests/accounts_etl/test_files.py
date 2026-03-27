"""Unit tests for fintl.accounts_etl.files."""

from pathlib import Path

from fintl.accounts_etl.files import copy_new_files


def test_copy_new_files_creates_raw_dir_when_missing(tmp_path: Path):
    """copy_new_files must create raw_dir if it does not yet exist."""
    src = tmp_path / "source" / "export.csv"
    src.parent.mkdir()
    src.write_text("data")

    raw_dir = tmp_path / "raw" / "dkb" / "giro"
    assert not raw_dir.exists()

    copy_new_files(raw_dir, [src])

    assert raw_dir.exists()
    assert (raw_dir / "export.csv").read_text() == "data"


def test_copy_new_files_raw_dir_already_exists(tmp_path: Path):
    """copy_new_files must skip mkdir when raw_dir already exists."""
    src = tmp_path / "export.csv"
    src.write_text("data")

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()  # already exists

    copy_new_files(raw_dir, [src])

    assert (raw_dir / "export.csv").read_text() == "data"
