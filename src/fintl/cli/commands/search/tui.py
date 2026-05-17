"""Textual TUI widgets for the interactive transaction search command."""

import polars as pl
from dateutil.parser import parse
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Collapsible, DataTable, Footer, Header, Input, Static

from fintl.cli.commands.search.constants import (
    _FILTERABLE_INPUT_IDS,
    CSS_PATH,
    MAX_COLUMN_WIDTH,
    WAIT_TIME,
)
from fintl.cli.commands.search.helper import get_transactions
from fintl.cli.commands.search.validators import AmountValidator, DateValidator
from fintl.common import Config


class RowDetailScreen(ModalScreen):
    """Modal screen showing full details for a selected transaction row."""

    BINDINGS = [Binding("escape", "dismiss", "Close")]

    def __init__(self, row: dict) -> None:
        """Initialise RowDetailScreen."""
        super().__init__()
        self.row = row

    def compose(self) -> ComposeResult:
        """Compose the detail dialog layout."""
        with Vertical(id="detail-dialog"):
            yield DataTable(id="detail-table", cursor_type="row")
            yield Static(
                "↑↓ navigate  enter: copy value  esc: close",
                id="detail-hint",
            )

    def on_mount(self) -> None:
        """Populate the detail table with row field/value pairs on mount."""
        table = self.query_one("#detail-table", DataTable)
        table.add_column("Field", width=14)
        table.add_column("Value", width=80)
        for field, value in self.row.items():
            table.add_row(field, str(value))
        table.focus()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Copy the selected row's value to the clipboard."""
        keys = list(self.row.keys())
        values = list(self.row.values())
        field = keys[event.cursor_row]
        value = str(values[event.cursor_row])
        self.app.copy_to_clipboard(value)
        preview = value[:60] + "…" if len(value) > 60 else value
        self.app.notify(f"Copied [{field}]: {preview}")


class TableApp(App):
    """Main Textual app for browsing and filtering bank transactions."""

    CSS_PATH = CSS_PATH
    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", show=True),
        Binding("ctrl+x", "clear_filters", "Clear filters", show=True),
        Binding("escape", "focus_table", "Focus table", show=True),
    ]

    def compose(self) -> ComposeResult:
        """Compose the main app layout with filter inputs and a data table."""
        yield Header()
        with Vertical():
            with Collapsible(title="Filters", id="filter-container"):
                with Vertical():
                    with Horizontal(classes="filter-row"):
                        yield Input(
                            placeholder="Filter by source...",
                            id="source-input",
                            classes="filter-input",
                        )
                        yield Input(
                            placeholder="Filter by recipient...",
                            id="recipient-input",
                            classes="filter-input",
                        )
                        yield Input(
                            placeholder="Filter by description...",
                            id="description-input",
                            classes="filter-input",
                        )
                    with Horizontal(classes="filter-row"):
                        yield Input(
                            placeholder="After date...",
                            id="date-lb-input",
                            classes="filter-input",
                            validators=[DateValidator()],
                        )
                        yield Input(
                            placeholder="Before date...",
                            id="date-ub-input",
                            classes="filter-input",
                            validators=[DateValidator()],
                        )
                    with Horizontal(classes="filter-row"):
                        yield Input(
                            placeholder="Above amount...",
                            id="amount-lb-input",
                            classes="filter-input",
                            validators=[AmountValidator()],
                        )
                        yield Input(
                            placeholder="Below amount...",
                            id="amount-ub-input",
                            classes="filter-input",
                            validators=[AmountValidator()],
                        )
                    with Horizontal(classes="filter-row"):
                        yield Input(
                            placeholder="Filter by provider...",
                            id="provider-input",
                            classes="filter-input",
                        )
                        yield Input(
                            placeholder="Filter by service...",
                            id="service-input",
                            classes="filter-input",
                        )

            yield DataTable(id="data-table", cursor_type="row")
        yield Static("", id="stats")
        yield Footer()

    def on_mount(self) -> None:
        """Load transactions and populate the table on app mount."""
        self._sort_column: str = "date"
        self._sort_reverse: bool = True
        config = Config()
        path_root = config.target_dir
        self.transactions_original = get_transactions(path_root)
        self.transactions_filtered = self.transactions_original.clone()

        self.set_table(self.transactions_original)
        self.update_stats()

    def set_table(self, transactions: pl.DataFrame):
        """Clear and repopulate the data table with the given transactions."""
        table = self.query_one("#data-table", DataTable)
        table = table.clear(columns=True)
        self.set_columns(table, transactions)
        self.set_rows(table, transactions)

    def set_rows(self, table: DataTable, transactions: pl.DataFrame):
        """Add all transaction rows to the data table."""
        for row in transactions.rows():
            table.add_row(*row)

    def set_columns(self, table: DataTable, transactions: pl.DataFrame):
        """Add columns to the data table with auto-sized widths."""
        columns = list(transactions.columns)

        for c in columns:
            max_chars = transactions[c].cast(pl.String).str.len_chars().max()
            max_chars = int(max_chars) + 2  # type: ignore
            column_width = min(max_chars, MAX_COLUMN_WIDTH)
            if c == self._sort_column:
                label = f"{c} {'▼' if self._sort_reverse else '▲'}"
            else:
                label = c
            table.add_column(label, key=c, width=column_width)

    def update_stats(self) -> None:
        """Update the stats bar with the current filtered/total row counts."""
        stats = self.query_one("#stats", Static)
        total_rows = len(self.transactions_original)
        filtered_rows = len(self.transactions_filtered)
        stats.update(f"Showing {filtered_rows}/{total_rows}")

    def filter_dataframe(self) -> pl.DataFrame:
        """Apply all active filter inputs and return the filtered DataFrame."""
        date_lb_text = self.query_one("#date-lb-input", Input).value.strip()
        date_ub_text = self.query_one("#date-ub-input", Input).value.strip()
        amount_lb_text = self.query_one("#amount-lb-input", Input).value.strip()
        amount_ub_text = self.query_one("#amount-ub-input", Input).value.strip()

        df = self.transactions_original

        text_columns = ["source", "recipient", "description", "provider", "service"]

        for col in text_columns:
            text = self.query_one(f"#{col}-input", Input).value.strip().lower()
            if text:
                df = df.filter(
                    pl.col(col).cast(pl.String).str.to_lowercase().str.contains(text, literal=True)
                )

        if date_lb_text:
            date_lb = parse(date_lb_text)
            df = df.filter(pl.col("date").ge(date_lb))

        if date_ub_text:
            date_ub = parse(date_ub_text)
            df = df.filter(pl.col("date").le(date_ub))

        if amount_lb_text:
            amount_lb_number = float(amount_lb_text)
            df = df.filter(pl.col("amount").ge(amount_lb_number))

        if amount_ub_text:
            amount_ub_number = float(amount_ub_text)
            df = df.filter(pl.col("amount").le(amount_ub_number))

        if self._sort_column:
            df = df.sort(self._sort_column, descending=self._sort_reverse)
        else:
            df = df.sort("date", descending=True)

        return df

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Push the row detail screen when a transaction row is selected."""
        row_dict = self.transactions_filtered.row(event.cursor_row, named=True)
        self.push_screen(RowDetailScreen(row_dict))

    def apply_filter(self):
        """Re-filter the transactions and refresh the table display."""
        try:
            self.transactions_filtered = self.filter_dataframe()
            self.set_table(self.transactions_filtered)
            self.update_stats()
        except Exception:
            # If filter fails, show original data
            self.transactions_filtered = self.transactions_original
            self.set_table(self.transactions_original)
            self.update_stats()

    def action_focus_table(self) -> None:
        """Move keyboard focus to the data table."""
        self.query_one("#data-table", DataTable).focus()

    def action_clear_filters(self) -> None:
        """Clear all filter inputs and expand the filter panel."""
        for _input in self.query(".filter-input").results(Input):
            _input.clear()
        collapsible = self.query_one("#filter-container", Collapsible)
        collapsible.collapsed = False

    def _all_inputs_valid(self) -> bool:
        """Return True if every validated filter input is currently valid."""
        return all(
            _input.is_valid
            for _input in self.query(".filter-input").results(Input)
            if _input.validators  # only check inputs that have validators attached
        )

    def on_input_changed(self, event: Input.Changed) -> None:
        """Debounce filter application and show validation errors in the stats bar."""
        if event.validation_result and not event.validation_result.is_valid:
            msg = " · ".join(event.validation_result.failure_descriptions)
            self.query_one("#stats", Static).update(f"[red]{msg}[/red]")
        elif self._all_inputs_valid():
            self.update_stats()  # restore normal row count display

        if (
            event.validation_result and self._all_inputs_valid()
        ) or event.validation_result is None:
            if event.input.id in _FILTERABLE_INPUT_IDS:
                if hasattr(self, "_filter_timer"):
                    self._filter_timer.stop()
                    self._filter_timer = self.set_timer(WAIT_TIME, self.apply_filter)
                else:
                    self._filter_timer = self.set_timer(WAIT_TIME, self.apply_filter)

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        """Toggle sort order or change sort column when a header is clicked."""
        column = event.column_key.value
        if column is None:
            return

        if self._sort_column == column:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = column
            self._sort_reverse = False

        self.apply_filter()
