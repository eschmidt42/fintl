"""Tests for the fintl common path utilities."""

from pathlib import Path

import pytest

from fintl.common.paths import normalize_path, sanity_check_path


def test_normalize_path(tmp_path: Path):
    """Test that normalize_path resolves relative, absolute, and home-relative paths."""
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
    """Test that sanity_check_path raises ValueError for a non-existent path."""
    path = Path("/path/to/exists")
    with pytest.raises(ValueError, match=f"Path {path} does not exist"):
        sanity_check_path(path)


def test_sanity_check_path_exists():
    """Test that sanity_check_path does not raise for an existing path."""
    path = Path.home()
    sanity_check_path(path)


def test_sanity_check_path_invalid_input_type():
    """Test that sanity_check_path raises ValueError when passed a non-Path argument."""
    path = "not a path"
    with pytest.raises(ValueError):
        sanity_check_path(path)  # type: ignore
