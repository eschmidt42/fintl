"""
CLI tool to interactively search through bank transactions.

Example data:

┌────────┬──────────┬────────┬──────────┬─────────┬─────────┬─────────┬────────┬─────────┬─────────┐
│ source ┆ recipien ┆ amount ┆ descript ┆ date    ┆ provide ┆ service ┆ parser ┆ file    ┆ hash    │
│ ---    ┆ t        ┆ ---    ┆ ion      ┆ ---     ┆ r       ┆ ---     ┆ ---    ┆ ---     ┆ ---     │
│ str    ┆ ---      ┆ f64    ┆ ---      ┆ date    ┆ ---     ┆ str     ┆ str    ┆ str     ┆ u64     │
│        ┆ str      ┆        ┆ str      ┆         ┆ str     ┆         ┆        ┆         ┆         │
╞════════╪══════════╪════════╪══════════╪═════════╪═════════╪═════════╪════════╪═════════╪═════════╡
│ myself ┆ EXAMPLE  ┆ -100.0 ┆ 2022-10- ┆ 2022-10 ┆ DKB     ┆ giro    ┆ giro0  ┆ /home/a ┆ 1234567 │
│        ┆ BANK     ┆        ┆ 12       ┆ -14     ┆         ┆         ┆        ┆ lice/Do ┆ 8901234 │
│        ┆ MAIN ST  ┆        ┆ Debitk.0 ┆         ┆         ┆         ┆        ┆ cuments ┆ 56789   │
│        ┆          ┆        ┆ 0 VISA…  ┆         ┆         ┆         ┆        ┆ /Paperw ┆         │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ ork…    ┆         │
│ myself ┆ Santa    ┆ -131.0 ┆ Foobar   ┆ 2022-05 ┆ DKB     ┆ giro    ┆ giro0  ┆ /home/a ┆ 9876543 │
│        ┆ Clause   ┆        ┆          ┆ -09     ┆         ┆         ┆        ┆ lice/Do ┆ 2109876 │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ cuments ┆ 54321   │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ /Paperw ┆         │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ ork…    ┆         │
└────────┴──────────┴────────┴──────────┴─────────┴─────────┴─────────┴────────┴─────────┴─────────┘
"""

from functools import cache
from pathlib import Path

import polars as pl
from dateutil.parser import parse
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.validation import ValidationResult, Validator
from textual.widgets import Collapsible, DataTable, Footer, Header, Input, Static

from fintl.accounts_etl.schemas import Config

WAIT_TIME = 1  # seconds

MAX_COLUMN_WIDTH = 24

_FILTERABLE_INPUT_IDS = [
    "source-input",
    "recipient-input",
    "description-input",
    "date-lb-input",
    "date-ub-input",
    "amount-lb-input",
    "amount-ub-input",
    "provider-input",
    "service-input",
]


@cache
def get_transactions(path_root: Path) -> pl.DataFrame:
    path_transactions = path_root / "all-transactions.parquet"
    assert path_transactions.exists()

    df = pl.read_parquet(path_transactions)

    df = df.sort("date", descending=True)
    df = df.drop(["file", "hash"])
    return df


class RowDetailScreen(ModalScreen):
    BINDINGS = [Binding("escape", "dismiss", "Close")]

    def __init__(self, row: dict) -> None:
        super().__init__()
        self.row = row

    def compose(self) -> ComposeResult:
        with Vertical(id="detail-dialog"):
            yield DataTable(id="detail-table", cursor_type="row")
            yield Static(
                "↑↓ navigate  enter: copy value  esc: close",
                id="detail-hint",
            )

    def on_mount(self) -> None:
        table = self.query_one("#detail-table", DataTable)
        table.add_column("Field", width=14)
        table.add_column("Value", width=80)
        for field, value in self.row.items():
            table.add_row(field, str(value))
        table.focus()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        keys = list(self.row.keys())
        values = list(self.row.values())
        field = keys[event.cursor_row]
        value = str(values[event.cursor_row])
        self.app.copy_to_clipboard(value)
        preview = value[:60] + "…" if len(value) > 60 else value
        self.app.notify(f"Copied [{field}]: {preview}")


class DateValidator(Validator):
    def validate(self, value: str) -> ValidationResult:
        if not value:
            return self.success()
        try:
            parse(value)
            return self.success()
        except ValueError:
            return self.failure("Invalid date")


class AmountValidator(Validator):
    def validate(self, value: str) -> ValidationResult:
        if not value:
            return self.success()
        try:
            float(value)
            return self.success()
        except ValueError:
            return self.failure("Must be a number")


class TableApp(App):
    CSS_PATH = "search.tcss"
    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", show=True),
        Binding("ctrl+x", "clear_filters", "Clear filters", show=True),
        Binding("escape", "focus_table", "Focus table", show=True),
    ]

    def compose(self) -> ComposeResult:
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
        self._sort_column: str = "date"
        self._sort_reverse: bool = True
        config = Config()
        path_root = config.target_dir
        self.transactions_original = get_transactions(path_root)
        self.transactions_filtered = self.transactions_original.clone()

        self.set_table(self.transactions_original)
        self.update_stats()

    def set_table(self, transactions: pl.DataFrame):
        table = self.query_one("#data-table", DataTable)
        table = table.clear(columns=True)
        self.set_columns(table, transactions)
        self.set_rows(table, transactions)

    def set_rows(self, table: DataTable, transactions: pl.DataFrame):
        for row in transactions.rows():
            table.add_row(*row)

    def set_columns(self, table: DataTable, transactions: pl.DataFrame):
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
        stats = self.query_one("#stats", Static)
        total_rows = len(self.transactions_original)
        filtered_rows = len(self.transactions_filtered)
        stats.update(f"Showing {filtered_rows}/{total_rows}")

    def filter_dataframe(self) -> pl.DataFrame:
        source_input = self.query_one("#source-input", Input)
        recipient_input = self.query_one("#recipient-input", Input)
        description_input = self.query_one("#description-input", Input)
        date_lb_input = self.query_one("#date-lb-input", Input)
        date_ub_input = self.query_one("#date-ub-input", Input)
        amount_lb_input = self.query_one("#amount-lb-input", Input)
        amount_ub_input = self.query_one("#amount-ub-input", Input)
        provider_input = self.query_one("#provider-input", Input)
        service_input = self.query_one("#service-input", Input)

        source_text = source_input.value.strip()
        recipient_text = recipient_input.value.strip()
        description_text = description_input.value.strip()
        date_lb_text = date_lb_input.value.strip()
        amount_lb_text = amount_lb_input.value.strip()
        date_ub_text = date_ub_input.value.strip()
        amount_ub_text = amount_ub_input.value.strip()
        provider_text = provider_input.value.strip()
        service_text = service_input.value.strip()

        df = self.transactions_original

        if source_text:
            df = df.filter(
                pl.col("source")
                .cast(pl.String)
                .str.to_lowercase()
                .str.contains(source_text.lower(), literal=True)
            )

        if recipient_text:
            df = df.filter(
                pl.col("recipient")
                .cast(pl.String)
                .str.to_lowercase()
                .str.contains(recipient_text.lower(), literal=True)
            )

        if description_text:
            df = df.filter(
                pl.col("description")
                .cast(pl.String)
                .str.to_lowercase()
                .str.contains(description_text.lower(), literal=True)
            )

        if provider_text:
            df = df.filter(
                pl.col("provider")
                .cast(pl.String)
                .str.to_lowercase()
                .str.contains(provider_text.lower(), literal=True)
            )

        if service_text:
            df = df.filter(
                pl.col("service")
                .cast(pl.String)
                .str.to_lowercase()
                .str.contains(service_text.lower(), literal=True)
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
        row_dict = self.transactions_filtered.row(event.cursor_row, named=True)
        self.push_screen(RowDetailScreen(row_dict))

    def apply_filter(self):
        try:
            self.transactions_filtered = self.filter_dataframe()
            self.set_table(self.transactions_filtered)
            self.update_stats()
        except Exception as e:
            # If filter fails, show original data
            self.transactions_filtered = self.transactions_original
            self.set_table(self.transactions_original)
            self.update_stats()

    def action_focus_table(self) -> None:
        self.query_one("#data-table", DataTable).focus()

    def action_clear_filters(self) -> None:
        for input in self.query(".filter-input").results(Input):
            input.clear()
        collapsible = self.query_one("#filter-container", Collapsible)
        collapsible.collapsed = False

    def _all_inputs_valid(self) -> bool:
        return all(
            input.is_valid
            for input in self.query(".filter-input").results(Input)
            if input.validators  # only check inputs that have validators attached
        )

    def on_input_changed(self, event: Input.Changed) -> None:
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
        column = event.column_key.value
        if column is None:
            return

        if self._sort_column == column:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = column
            self._sort_reverse = False

        self.apply_filter()


def main():
    app = TableApp()
    app.run()


if __name__ == "__main__":
    main()
