"""I/O helpers for loading, concatenating, and storing transaction records."""

import logging
from pathlib import Path

import polars as pl

from fintl.etl.common.schemas import (
    TRANSACTION_COLUMNS,
    TransactionColumnsEnum,
)
from fintl.etl.io.files.filenames import (
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
    transaction_name_to_parquet,
)
from fintl.etl.io.files.utils import (
    find_common_columns,
)

logger = logging.getLogger(__name__)


def postprocess_old_and_new_transactions(transactions: pl.DataFrame) -> pl.DataFrame:
    """Sorts transactions by date and removes duplicates based on hash.

    Args:
        transactions: A DataFrame of combined transactions.

    Returns:
        The postprocessed DataFrame, sorted by date with unique hashes.
    """
    transactions = transactions.unique(subset=["hash"], maintain_order=True)
    transactions = transactions.sort("date")

    return transactions


def stack_old_and_new_transactions(
    all_transactions_file: Path, newly_parsed_transactions: pl.DataFrame
) -> tuple[pl.DataFrame, int]:
    """Merges old transactions with newly parsed ones.

    Args:
        all_transactions_file: Path to the existing transactions Parquet file.
        newly_parsed_transactions: A DataFrame containing the newly parsed transactions.

    Returns:
        A tuple containing:
            - The combined DataFrame of old and new transactions.
            - The number of old transactions loaded.
    """
    if all_transactions_file.exists():
        old_transactions = pl.read_parquet(all_transactions_file)

        old_transactions = old_transactions.select(TRANSACTION_COLUMNS)

        n_old = len(old_transactions)
        transactions = pl.concat(
            [old_transactions, newly_parsed_transactions],
        )
    else:
        n_old = 0
        transactions = newly_parsed_transactions

    return transactions, n_old


def process_new_transactions(
    newly_parsed_transactions: list[pl.DataFrame],
) -> pl.DataFrame:
    """Projects newly parsed transactions against the standard transaction columns.

    Args:
        newly_parsed_transactions: A list of DataFrames containing parsed transactions.

    Returns:
        A single concatenated DataFrame containing only the standard TRANSACTION_COLUMNS.
    """
    newly_parsed_transactions = [df.select(TRANSACTION_COLUMNS) for df in newly_parsed_transactions]

    single_transactions_list = pl.concat(newly_parsed_transactions)
    return single_transactions_list


def load_transactions(parsed_dir: Path, new_files_to_parse: list[Path]) -> list[pl.DataFrame]:
    """Loads newly parsed transaction DataFrames from Parquet files.

    Args:
        parsed_dir: The directory containing the parsed Parquet files.
        new_files_to_parse: A list of paths to the new source CSV files.

    Returns:
        A list of Polars DataFrames containing the newly loaded transactions.
    """
    newly_parsed_transactions: list[pl.DataFrame] = []

    for file_path in new_files_to_parse:
        parquet_file_path = parsed_dir / transaction_name_to_parquet(file_path)

        if not parquet_file_path.exists():
            logger.warning(f"{parquet_file_path=} does not exist, skipping.")
            continue

        try:
            transaction_df = pl.read_parquet(parquet_file_path)
        except (OSError, pl.exceptions.PolarsError) as error:
            logger.warning(f"Failed to read {parquet_file_path=}: {error}, skipping.")
            continue

        newly_parsed_transactions.append(transaction_df)
        logger.debug(f"Processing {parquet_file_path}: Shape = {transaction_df.shape}")

    return newly_parsed_transactions


def merge_transactions(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
) -> tuple[pl.DataFrame | None, int]:
    """Loads, processes, and concatenates new transaction data with existing history.

    Orchestates the full transaction pipeline: loading new Parquet splits,
    projecting them to standard columns, merging with old history, deduplicating,
    and sorting by date.

    Args:
        parser_dir: Directory where the existing `transactions.parquet` is stored.
        parsed_dir: Directory containing newly parsed transaction Parquet files.
        new_files_to_parse: List of paths to new transaction source files.

    Returns:
        A tuple containing:
            - The combined DataFrame of old and new transactions, or None if no new data.
            - The count of newly added transactions.
    """
    all_transactions_file = parser_dir / "transactions.parquet"

    newly_parsed_transactions = load_transactions(parsed_dir, new_files_to_parse)

    if len(newly_parsed_transactions) == 0:
        logger.warning(
            f"There were no new transaction items loaded, i.e. {len(newly_parsed_transactions)=:_}."
        )
        return None, 0

    find_common_columns(newly_parsed_transactions)

    newly_parsed_transactions = process_new_transactions(newly_parsed_transactions)

    transactions, n_old = stack_old_and_new_transactions(
        all_transactions_file, newly_parsed_transactions
    )

    transactions = postprocess_old_and_new_transactions(transactions)

    n_new = len(transactions)
    n_added = max(0, n_new - n_old)

    return transactions, n_added


def update_transactions_history(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
) -> None:
    """Processes new transaction files and updates the transaction history.

    If new transactions are available, they are merged with existing history,
    deduplicated, sorted, and written to both Parquet and Excel formats.

    Args:
        parser_dir: Directory to read/write transaction history files.
        parsed_dir: Directory containing newly parsed transaction files.
        new_files_to_parse: List of paths to new transaction files to process.
    """
    transactions, n_new_lines = merge_transactions(parser_dir, parsed_dir, new_files_to_parse)
    if transactions is None:
        logger.warning(f"{transactions=}, skipping writing to disk.")
        return

    transactions = transactions.unique(subset=["hash"], maintain_order=True, keep="first")
    transactions = transactions.sort(TransactionColumnsEnum.date.value, descending=False)

    transactions_parquet_path = parser_dir / "transactions.parquet"
    logger.info(f"Writing {n_new_lines=:_d} to {transactions_parquet_path=}")
    transactions.write_parquet(transactions_parquet_path)

    excel_path = parser_dir / "transactions.xlsx"
    logger.info(f"Writing {n_new_lines=:_d} to {excel_path=}")
    transactions.write_excel(excel_path)


def store_transactions(parsed_dir: Path, file_path: Path, transactions: pl.DataFrame) -> None:
    """Stores a transaction DataFrame to the parsed directory as Parquet and Excel.

    Args:
        parsed_dir: Directory to write the transaction output files.
        file_path: Path to the original transaction CSV file.
        transactions: A Polars DataFrame containing the transaction data.

    Returns:
        None
    """
    excel_file = parsed_dir / transaction_csv_name_to_xlsx(file_path)
    logger.debug(f"Writing {excel_file=}")
    transactions.write_excel(excel_file)

    parquet_file = parsed_dir / transaction_csv_name_to_parquet(file_path)
    logger.debug(f"Writing {parquet_file=}")
    transactions.write_parquet(parquet_file)
