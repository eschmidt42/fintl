# Conftest.py
from pathlib import Path

import pytest

from fintl.path_utils import normalize_path, sanity_check_path


def test_normalize_path(tmp_path: Path):
    # Test with a relative path
    result = normalize_path(Path("test.txt"))
    assert result == Path.cwd() / "test.txt"

    # Test with an absolute path
    result = normalize_path(Path.cwd() / "test.txt")
    assert result == Path.cwd() / "test.txt"

    # Test with a home directory link (~)
    result = normalize_path(Path("~/test.txt"))
    assert result == Path.home() / "test.txt"


def test_sanity_check_path_not_exists():
    path = Path("/path/to/exists")
    with pytest.raises(ValueError, match=f"Path {path} does not exist"):
        sanity_check_path(path)


def test_sanity_check_path_exists():
    path = Path.home()
    sanity_check_path(path)


def test_sanity_check_path_invalid_input_type():
    path = "not a path"
    with pytest.raises(ValueError):
        sanity_check_path(path)  # type: ignore
