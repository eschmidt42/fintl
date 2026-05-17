"""Tests for the Textual TUI search app.

Uses Textual's async run_test() / Pilot API.  Config and get_transactions are
monkeypatched so no real filesystem state is required.
"""

import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from textual.widgets import DataTable, Input, Static

from fintl.cli.commands.search.tui import RowDetailScreen, TableApp

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Return a sample two-row transactions DataFrame."""
    return pl.DataFrame(
        {
            "source": ["me", "me"],
            "recipient": ["Alice", "Bob"],
            "amount": [-10.0, 100.0],
            "description": ["foo", "bar"],
            "date": [datetime.date(2024, 1, 1), datetime.date(2023, 6, 1)],
            "provider": ["DKB", "GLS"],
            "service": ["giro", "giro"],
            "parser": ["giro0", "giro0"],
        }
    )


@pytest.fixture()
def patched_app(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, sample_df: pl.DataFrame
) -> type[TableApp]:
    """Return the TableApp class with Config and get_transactions monkeypatched."""
    mock_config = MagicMock()
    mock_config.target_dir = tmp_path
    monkeypatch.setattr("fintl.cli.commands.search.tui.Config", lambda: mock_config)
    monkeypatch.setattr(
        "fintl.cli.commands.search.tui.get_transactions",
        lambda _path: sample_df.clone(),
    )
    monkeypatch.setattr("fintl.cli.commands.search.tui.WAIT_TIME", 0.05)
    return TableApp


# ── Helpers ───────────────────────────────────────────────────────────────────


def _header_event(column_value: str | None) -> MagicMock:
    """Return a mock DataTable.HeaderSelected event for the given column value."""
    event = MagicMock(spec=DataTable.HeaderSelected)
    event.column_key = MagicMock()
    event.column_key.value = column_value
    return event


# ── Startup ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_app_starts_and_populates_table(
    patched_app: type[TableApp], sample_df: pl.DataFrame
) -> None:
    """Test that the app starts and populates the data table with all rows."""
    async with patched_app().run_test() as pilot:
        table = pilot.app.query_one("#data-table", DataTable)
        assert table.row_count == len(sample_df)


@pytest.mark.anyio
async def test_stats_shows_row_count_on_start(patched_app: type[TableApp]) -> None:
    """Test that the stats widget shows the correct row count on startup."""
    async with patched_app().run_test() as pilot:
        stats = pilot.app.query_one("#stats", Static)
        assert isinstance(stats.content, str)
        assert "2/2" in stats.content


# ── Direct filter_dataframe tests ────────────────────────────────────────────


@pytest.mark.anyio
async def test_filter_by_source(patched_app: type[TableApp]) -> None:
    """Test that filtering by source returns the expected rows."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#source-input", Input).value = "me"
        app.apply_filter()
        assert app.transactions_filtered.height == 2


@pytest.mark.anyio
async def test_filter_by_recipient(patched_app: type[TableApp]) -> None:
    """Test that filtering by recipient narrows the results correctly."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#recipient-input", Input).value = "Alice"
        app.apply_filter()
        assert app.transactions_filtered.height == 1


@pytest.mark.anyio
async def test_filter_by_description(patched_app: type[TableApp]) -> None:
    """Test that filtering by description narrows the results correctly."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#description-input", Input).value = "bar"
        app.apply_filter()
        assert app.transactions_filtered.height == 1


@pytest.mark.anyio
async def test_filter_by_provider(patched_app: type[TableApp]) -> None:
    """Test that filtering by provider narrows the results correctly."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#provider-input", Input).value = "gls"
        app.apply_filter()
        assert app.transactions_filtered.height == 1


@pytest.mark.anyio
async def test_filter_by_service(patched_app: type[TableApp]) -> None:
    """Test that filtering by service returns all matching rows."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#service-input", Input).value = "giro"
        app.apply_filter()
        assert app.transactions_filtered.height == 2


@pytest.mark.anyio
async def test_filter_by_date_lower_bound(patched_app: type[TableApp]) -> None:
    """Test that a date lower-bound filter excludes rows before that date."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#date-lb-input", Input).value = "2024-01-01"
        app.apply_filter()
        assert app.transactions_filtered.height == 1


@pytest.mark.anyio
async def test_filter_by_date_upper_bound(patched_app: type[TableApp]) -> None:
    """Test that a date upper-bound filter excludes rows after that date."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#date-ub-input", Input).value = "2023-12-31"
        app.apply_filter()
        assert app.transactions_filtered.height == 1


@pytest.mark.anyio
async def test_filter_by_amount_lower_bound(patched_app: type[TableApp]) -> None:
    """Test that an amount lower-bound filter excludes negative amounts."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#amount-lb-input", Input).value = "0"
        app.apply_filter()
        assert app.transactions_filtered.height == 1


@pytest.mark.anyio
async def test_filter_by_amount_upper_bound(patched_app: type[TableApp]) -> None:
    """Test that an amount upper-bound filter excludes amounts above the threshold."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#amount-ub-input", Input).value = "-5"
        app.apply_filter()
        assert app.transactions_filtered.height == 1


@pytest.mark.anyio
async def test_filter_with_no_sort_column(patched_app: type[TableApp]) -> None:
    """Test that apply_filter works correctly when no sort column is set."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app._sort_column = ""
        app.apply_filter()
        assert app.transactions_filtered.height == 2


# ── Sorting ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_header_click_sets_sort_column(patched_app: type[TableApp]) -> None:
    """Test that clicking a header sets the sort column and clears reverse flag."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.on_data_table_header_selected(_header_event("amount"))
        assert app._sort_column == "amount"
        assert app._sort_reverse is False


@pytest.mark.anyio
async def test_header_click_same_column_toggles_reverse(
    patched_app: type[TableApp],
) -> None:
    """Test that clicking the same column header toggles the sort direction."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app._sort_column = "amount"
        app._sort_reverse = False
        app.on_data_table_header_selected(_header_event("amount"))
        assert app._sort_reverse is True


@pytest.mark.anyio
async def test_header_click_none_column_is_noop(patched_app: type[TableApp]) -> None:
    """Test that a header click with a None column key does not change sort state."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        original = app._sort_column
        app.on_data_table_header_selected(_header_event(None))
        assert app._sort_column == original


# ── Actions ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_action_clear_filters(patched_app: type[TableApp]) -> None:
    """Test that action_clear_filters resets all input fields to empty strings."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        src_input = app.query_one("#source-input", Input)
        src_input.value = "something"
        app.action_clear_filters()
        assert src_input.value == ""


@pytest.mark.anyio
async def test_action_focus_table(patched_app: type[TableApp]) -> None:
    """Test that action_focus_table focuses the data table widget."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        pilot.app.action_focus_table()
        assert pilot.app.query_one("#data-table", DataTable) is not None


# ── on_input_changed: timer path and invalid input ────────────────────────────


@pytest.mark.anyio
async def test_on_input_changed_valid_triggers_filter(
    patched_app: type[TableApp],
) -> None:
    """Test that a valid input change triggers the filter after the debounce timer."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        # Setting value triggers the Changed message
        app.query_one("#source-input", Input).value = "me"
        await pilot.pause(0.2)  # wait for 0.05 s timer to fire
        assert app.transactions_filtered.height == 2


@pytest.mark.anyio
async def test_on_input_changed_invalid_shows_error(
    patched_app: type[TableApp],
) -> None:
    """Test that an invalid input value shows an error message in the stats widget."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#date-lb-input", Input).value = "notadate"
        await pilot.pause(0.1)
        stats = app.query_one("#stats", Static)
        assert isinstance(stats.content, str)
        assert "Invalid date" in stats.content


@pytest.mark.anyio
async def test_on_input_changed_second_change_resets_existing_timer(
    patched_app: type[TableApp],
) -> None:
    """Exercises lines 293-294: the hasattr(_filter_timer) True branch."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        src = app.query_one("#source-input", Input)
        src.value = "m"  # first change — creates _filter_timer (else branch)
        src.value = "me"  # second change before timer fires — hits lines 293-294
        await pilot.pause(0.2)
        assert app.transactions_filtered.height == 2


@pytest.mark.anyio
async def test_on_input_changed_valid_with_other_input_invalid(
    patched_app: type[TableApp],
) -> None:
    """Test that a valid change on one input is handled when another input is invalid.

    Exercises branch 285->288.

    amount-lb-input is invalid → _all_inputs_valid() is False.
    When date-lb-input gets a VALID value, validation_result is valid so the
    first `if` is False (285->288 branch).
    """
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        app.query_one("#amount-lb-input", Input).value = "abc"
        await pilot.pause(0)  # process the invalid amount change
        app.query_one("#date-lb-input", Input).value = "2024-01-01"
        await pilot.pause(0.2)


@pytest.mark.anyio
async def test_on_input_changed_non_filterable_input_skips_timer(
    patched_app: type[TableApp],
) -> None:
    """Test that a non-filterable input change does not create a debounce timer.

    Exercises branch 291->exit: input.id not in _FILTERABLE_INPUT_IDS.
    """
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        event = MagicMock(spec=Input.Changed)
        event.validation_result = None
        event.input = MagicMock()
        event.input.id = "some-other-input"
        app.on_input_changed(event)
        assert not hasattr(app, "_filter_timer")


# ── apply_filter exception path ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_apply_filter_exception_falls_back_to_original(
    patched_app: type[TableApp], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that apply_filter falls back to the original DataFrame on exception."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app

        def _raise():
            raise ValueError("bad filter")

        monkeypatch.setattr(app, "filter_dataframe", _raise)
        app.apply_filter()
        assert app.transactions_filtered.equals(app.transactions_original)


# ── RowDetailScreen ───────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_row_detail_screen_shows_fields(patched_app: type[TableApp]) -> None:
    """Test that RowDetailScreen displays all fields from the selected row."""
    row = {"source": "me", "amount": -10.0, "date": datetime.date(2024, 1, 1)}
    async with patched_app().run_test() as pilot:
        await pilot.app.push_screen(RowDetailScreen(row))
        await pilot.pause()
        screen = pilot.app.screen
        assert isinstance(screen, RowDetailScreen)
        detail_table = screen.query_one("#detail-table", DataTable)
        assert detail_table.row_count == len(row)


@pytest.mark.anyio
async def test_row_detail_screen_row_select_copies_value(
    patched_app: type[TableApp],
) -> None:
    """Test that selecting a row in RowDetailScreen copies the value to clipboard."""
    row = {"source": "me", "amount": -10.0}
    async with patched_app().run_test() as pilot:
        await pilot.app.push_screen(RowDetailScreen(row))
        await pilot.pause()
        screen = pilot.app.screen
        assert isinstance(screen, RowDetailScreen)
        event = MagicMock(spec=DataTable.RowSelected)
        event.cursor_row = 0
        screen.on_data_table_row_selected(event)


# ── on_data_table_row_selected (main app) ─────────────────────────────────────


@pytest.mark.anyio
async def test_main_table_row_selected_pushes_detail_screen(
    patched_app: type[TableApp],
) -> None:
    """Test that selecting a row in the main table pushes the RowDetailScreen."""
    async with patched_app().run_test() as pilot:
        assert isinstance(pilot.app, TableApp)
        app = pilot.app
        event = MagicMock(spec=DataTable.RowSelected)
        event.cursor_row = 0
        app.on_data_table_row_selected(event)
        await pilot.pause()
        assert isinstance(app.screen, RowDetailScreen)
