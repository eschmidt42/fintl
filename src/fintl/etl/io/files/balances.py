"""I/O helpers for reading, writing, and concatenating balance records."""

import logging
from pathlib import Path

import polars as pl

from fintl.etl.common.schemas import (
    BALANCE_SCHEMA,
    BalanceInfo,
    TransactionColumnsEnum,
)
from fintl.etl.io.files.filenames import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    balance_name_to_parquet,
)

logger = logging.getLogger(__name__)


def concatenate_new_balances(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
) -> tuple[pl.DataFrame, int]:
    """Loads and concatenates new balance records with existing history.

    Args:
        parser_dir: Directory containing the history of balances.
        parsed_dir: Directory containing newly parsed balance files.
        new_files_to_parse: List of paths to newly parsed balance files.

    Returns:
        A tuple containing:
            - The combined DataFrame of old and new balance records.
            - The count of newly added balance records.
    """
    all_balances_file = parser_dir / "balances.parquet"
    newly_parsed_balances = [
        pl.read_parquet(parsed_dir / balance_name_to_parquet(file_path))
        for file_path in new_files_to_parse
    ]
    newly_parsed_balances = pl.concat(newly_parsed_balances)

    n_old = 0
    if all_balances_file.exists():
        old_balances = pl.read_parquet(all_balances_file)
        n_old = len(old_balances)
        balances = pl.concat([old_balances, newly_parsed_balances])
    else:
        balances = newly_parsed_balances

    balances = balances.sort("date")
    balances = balances.unique(subset=["date", "provider", "service", "parser"])
    n_new = len(balances)
    return balances, n_new - n_old


def concatenate_balances_history(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
) -> None:
    """Processes new balance files and updates the balance history.

    Reads new balances, appends them to the existing history, sorts them,
    and writes the updated history to both Parquet and Excel formats.

    Args:
        parser_dir: Directory to read/write balance history files.
        parsed_dir: Directory containing newly parsed balance files.
        new_files_to_parse: List of paths to new balance files to process.
    """
    balances, n_new_lines = concatenate_new_balances(parser_dir, parsed_dir, new_files_to_parse)
    balances = balances.sort(TransactionColumnsEnum.date.value, descending=False)

    balances_parquet_path = parser_dir / "balances.parquet"
    logger.info(f"Writing {n_new_lines=:_d} to {balances_parquet_path=}")
    balances.write_parquet(balances_parquet_path)

    excel_path = parser_dir / "balances.xlsx"
    logger.info(f"Writing {n_new_lines=:_d} to {excel_path=}")
    balances.write_excel(excel_path)


def store_balance(parsed_dir: Path, file_path: Path, balance: BalanceInfo | None) -> None:
    """Writes a balance record to JSON and Parquet formats.

    Args:
        parsed_dir: Directory to write the balance JSON and Parquet files.
        file_path: Path to the source balance CSV file (used to generate output filenames).
        balance: The BalanceInfo object to store, or None if no balance is available.

    Returns:
        None
    """
    json_file = parsed_dir / balance_csv_name_to_json(file_path)

    logger.debug(f"Writing {json_file=}")
    if balance is None:
        d = "{}"
    else:
        d = balance.model_dump_json(indent=4)

    with json_file.open("w") as f:
        f.write(d)

    parquet_file = parsed_dir / balance_csv_name_to_parquet(file_path)
    logger.debug(f"Writing {parquet_file=}")
    if balance is None:
        balance_df = pl.DataFrame([], schema=BALANCE_SCHEMA)
    else:
        balance_df = pl.DataFrame([balance.model_dump()])
    balance_df.write_parquet(parquet_file)
