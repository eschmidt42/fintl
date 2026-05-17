"""Tests for the search core run function."""

from unittest.mock import MagicMock, patch


def test_run_creates_and_runs_app():
    """Test that run() instantiates TableApp and calls run() on it."""
    mock_app = MagicMock()

    with patch("fintl.cli.commands.search.core.TableApp", return_value=mock_app):
        from fintl.cli.commands.search.core import run

        run()

    mock_app.run.assert_called_once()
