"""Tests for file utility functions."""

import logging

import polars as pl
import pytest

from fintl.etl.io.files.utils import find_common_columns


def test_find_common_columns_logs_kept_columns(caplog: pytest.LogCaptureFixture):
    """Test that find_common_columns logs the kept column names at INFO level."""
    caplog.set_level(logging.INFO)
    find_common_columns(
        [
            pl.DataFrame({"a": [1], "b": [2]}),
            pl.DataFrame({"a": [3], "c": [4]}),
        ]
    )
    assert "Kept the columns ['a']" in caplog.text


def test_find_common_columns_logs_discarded_columns(caplog: pytest.LogCaptureFixture):
    """Test that find_common_columns logs discarded column names."""
    find_common_columns(
        [
            pl.DataFrame({"a": [1], "b": [2]}),
            pl.DataFrame({"a": [3]}),
        ]
    )
    assert "Discarded the columns ['b']" in caplog.text
