import logging
from pathlib import Path
from typing import Callable

import polars as pl

from fintl.accounts_etl.dkb.files import (
    logger,
)
from fintl.accounts_etl.files import detect_present_parsed_files
from fintl.accounts_etl.scalable.files import (
    balance_htm_name_to_parquet,
    transaction_htm_name_to_parquet,
)
from fintl.accounts_etl.schemas import (
    BALANCE_SCHEMA,
    TRANSACTION_COLUMNS,
    BalanceInfo,
    Case,
    Config,
    TransactionColumnsEnum,
)

logger = logging.getLogger(__name__)


def balance_csv_name_to_parquet(file: Path) -> str:
    """Converts a balance CSV file name to the corresponding Parquet name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding Parquet file.
    """
    return file.name.replace(".csv", "-balance.parquet")


def balance_name_to_parquet(file: Path) -> str:
    """Determines the correct Parquet output name for a balance file.

    Dispatches to the appropriate converter based on the file's suffix.

    Args:
        file: The input balance file path.

    Returns:
        The name of the corresponding Parquet balance file.

    Raises:
        ValueError: If the file suffix is unsupported.
    """
    if file.name.endswith("csv"):
        return balance_csv_name_to_parquet(file)
    elif (
        file.name.endswith("htm")
        or file.name.endswith("html")
        or file.name.endswith("png")
    ):
        return balance_htm_name_to_parquet(file)
    else:
        raise ValueError(f"Unexpected suffix of {file=}")


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
    balances, n_new_lines = concatenate_new_balances(
        parser_dir, parsed_dir, new_files_to_parse
    )
    balances = balances.sort(TransactionColumnsEnum.date.value, descending=False)

    balances_parquet_path = parser_dir / "balances.parquet"
    logger.info(f"Writing {n_new_lines=:_d} to {balances_parquet_path=}")
    balances.write_parquet(balances_parquet_path)

    excel_path = parser_dir / "balances.xlsx"
    logger.info(f"Writing {n_new_lines=:_d} to {excel_path=}")
    balances.write_excel(excel_path)


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
    newly_parsed_transactions = [
        df.select(TRANSACTION_COLUMNS) for df in newly_parsed_transactions
    ]

    single_transactions_list = pl.concat(newly_parsed_transactions)
    return single_transactions_list


def transaction_csv_name_to_parquet(file: Path) -> str:
    """Converts a transaction CSV file name to the corresponding Parquet name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding Parquet file (e.g., 'export.csv' -> 'export-transactions.parquet').
    """
    return file.name.replace(".csv", "-transactions.parquet")


def transaction_name_to_parquet(file: Path) -> str:
    """Determines the correct Parquet output name for a transaction file.

    Dispatches to the appropriate converter based on the file's suffix.

    Args:
        file: The input transaction file path.

    Returns:
        The name of the corresponding Parquet transaction file.

    Raises:
        ValueError: If the file suffix is unsupported.
    """
    if file.name.endswith("csv"):
        return transaction_csv_name_to_parquet(file)
    elif (
        file.name.endswith("htm")
        or file.name.endswith("html")
        or file.name.endswith("png")
    ):
        return transaction_htm_name_to_parquet(file)
    else:
        raise ValueError(f"Unexpected suffix of {file=}")


def load_transactions(
    parsed_dir: Path, new_files_to_parse: list[Path]
) -> list[pl.DataFrame]:
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

        transaction_df = pl.read_parquet(parquet_file_path)
        newly_parsed_transactions.append(transaction_df)
        logger.debug(f"Processing {parquet_file_path}: Shape = {transaction_df.shape}")

    return newly_parsed_transactions


def find_common_columns(dfs: list[pl.DataFrame]) -> None:
    """Identifies columns common to all provided DataFrames and logs the results.

    Iterates through the list of DataFrames to determine which columns are
    present in all of them, while tracking columns that differ or are missing.

    Args:
        dfs: A list of Polars DataFrames to compare.
    """

    common_columns, discarded_columns = None, []

    for transaction_df in dfs:
        if common_columns is None:
            common_columns = transaction_df.columns
        else:
            new_common_columns = [
                c for c in common_columns if c in transaction_df.columns
            ]
            if len(new_common_columns) < len(common_columns):
                discarded_columns.extend(
                    list(set(common_columns).difference(new_common_columns))
                )
            discarded_columns.extend(
                list(set(transaction_df.columns).difference(new_common_columns))
            )
            common_columns = new_common_columns

    logger.info(f"Kept the columns {common_columns}")
    if len(discarded_columns) > 0:
        logger.warning(f"Discarded the columns {list(set(discarded_columns))}")


def concatenate_new_transactions(
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
        logger.warning(f"{len(newly_parsed_transactions)=:_}, returning empty.")
        return None, 0

    find_common_columns(newly_parsed_transactions)

    newly_parsed_transactions = process_new_transactions(newly_parsed_transactions)

    transactions, n_old = stack_old_and_new_transactions(
        all_transactions_file, newly_parsed_transactions
    )

    transactions = postprocess_old_and_new_transactions(transactions)

    n_new = len(transactions)

    return transactions, n_new - n_old


def concatenate_transactions_history(
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
    transactions, n_new_lines = concatenate_new_transactions(
        parser_dir, parsed_dir, new_files_to_parse
    )
    if transactions is None:
        logger.warning(f"{transactions=}, skipping writing to disk.")
        return

    transactions = transactions.unique(
        subset=["hash"], maintain_order=True, keep="first"
    )
    transactions = transactions.sort(
        TransactionColumnsEnum.date.value, descending=False
    )

    transactions_parquet_path = parser_dir / "transactions.parquet"
    logger.info(f"Writing {n_new_lines=:_d} to {transactions_parquet_path=}")
    transactions.write_parquet(transactions_parquet_path)

    excel_path = parser_dir / "transactions.xlsx"
    logger.info(f"Writing {n_new_lines=:_d} to {excel_path=}")
    transactions.write_excel(excel_path)


def concatenate_new_information_to_history(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
):
    "Concatenates new files to history / old files in data/{provider}/{service}/{parser}/{transactions,balances}.{xlsx,parquet}"
    logger.info("Concatenating new information to history")

    if len(new_files_to_parse) == 0:
        logger.info("There were no new files parsed, returning.")
        return

    concatenate_transactions_history(parser_dir, parsed_dir, new_files_to_parse)

    concatenate_balances_history(parser_dir, parsed_dir, new_files_to_parse)

    logger.info("Done concatenating information to history")


def detect_new_parsed_files(
    raw_dir: Path,
    parser_dir: Path,
    parsed_dir: Path,
) -> list[Path]:
    logger.info(f"Detecting newly parsed files")

    available_parsed_balance_files = list(parsed_dir.glob("*-balance.parquet"))

    all_balances_parquet_path = parser_dir / "balances.parquet"

    if all_balances_parquet_path.exists():
        all_balances = pl.read_parquet(all_balances_parquet_path)

        already_stored_files = (
            all_balances["file"].unique().to_list()
        )  # original name inlcuding .csv ending

        already_stored_files = set([Path(f).stem for f in already_stored_files])
    else:
        already_stored_files = set()

    n = len("-balance.parquet")
    newly_parsed_parquets = [
        f
        for f in available_parsed_balance_files
        if not f.name[:-n] in already_stored_files
    ]

    newly_parsed_csv_files = [
        raw_dir / f"{f.name[:-n]}.csv" for f in newly_parsed_parquets
    ]
    return newly_parsed_csv_files


def select_files_to_parse(
    present_parsed_files: list[Path], raw_files: list[Path]
) -> list[Path]:
    """Selects raw files that have not yet been parsed.

    Compares raw files against already present parsed files and returns only
    those that do not have a corresponding parsed counterpart yet.

    Args:
        present_parsed_files: List of already parsed file paths.
        raw_files: List of raw file paths to check against.

    Returns:
        A list of raw file paths that need to be parsed.
    """
    parsed_files = [file_path.name for file_path in present_parsed_files]
    files_to_parse = [
        file_path
        for file_path in raw_files
        if (file_path.name.replace(".csv", "-transactions.xlsx") not in parsed_files)
    ]
    logger.info(
        f"Selecting {len(files_to_parse):_} files to parse after comparing {len(present_parsed_files):_} present parsed files and {len(raw_files):_} raw files."
    )
    return files_to_parse


def detect_raw_files(raw_dir: Path, check_if_parser_applies: Callable) -> list[Path]:
    """Detects relevant raw CSV files in the given directory.

    Args:
        raw_dir: The directory to search for raw files.
        check_if_parser_applies: A callable that takes a file path and returns True
            if the file should be processed.

    Returns:
        A list of matched file paths.
    """
    raw_files = [
        file_path
        for file_path in raw_dir.glob("**/*.csv")
        if check_if_parser_applies(file_path)
    ]
    logger.info(f"Detected {len(raw_files):_} raw files @ {raw_dir=}.")
    return raw_files


def detect_new_raw_files(
    raw_dir: Path,
    check_if_parser_applies: Callable,
    parsed_dir: Path,
    provider: str,
    service: str,
) -> list[Path]:
    logger.info(f"Detecting new raw files for {provider=} -> {service=}")

    raw_files = detect_raw_files(raw_dir, check_if_parser_applies)
    logger.info(f"Found {len(raw_files):_d} raw files in {raw_dir=}")

    present_parsed_files = detect_present_parsed_files(parsed_dir)
    logger.info(f"Found {len(present_parsed_files):_d} matching files in {parsed_dir=}")

    new_files_to_parse = select_files_to_parse(present_parsed_files, raw_files)
    logger.info(f"Hence found {len(new_files_to_parse):_d} new files to parse")

    logger.info(f"Finished detecting files to be parsed for {provider=} -> {service=}")
    return new_files_to_parse


def detect_relevant_target_files(raw_dir: Path) -> list[Path]:
    """Detects relevant raw files in the given target directory."""
    relevant_target_files = [file_path for file_path in raw_dir.glob("**/*.csv")]
    logger.info(
        f"Detected {len(relevant_target_files):_} relevant source files @ {raw_dir=}."
    )
    return relevant_target_files


def detect_relevant_source_files(
    source_dir: Path, check_if_parser_applies: Callable
) -> list[Path]:
    """Detects relevant CSV files in the given source directory.

    Args:
        source_dir: The directory to search for source files.
        check_if_parser_applies: A callable that takes a file path and returns True
            if the file is relevant.

    Returns:
        A list of matched source file paths.
    """
    relevant_source_files = [
        file_path
        for file_path in source_dir.glob("**/*.csv")
        if check_if_parser_applies(file_path)
    ]
    logger.info(
        f"Detected {len(relevant_source_files):_} relevant source files @ {source_dir=}."
    )
    return relevant_source_files


def get_parser_source_files(
    case: Case, config: Config, check_if_parser_applies: Callable
) -> list[Path]:
    source_dir = config.get_source_dir(case.provider, case.service)
    relevant_source_files = detect_relevant_source_files(
        source_dir, check_if_parser_applies
    )
    return relevant_source_files


def balance_csv_name_to_json(file: Path) -> str:
    """Converts a balance CSV file name to the corresponding JSON name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding JSON file.
    """
    return file.name.replace(".csv", "-balance.json")


def store_balance(parsed_dir: Path, file_path: Path, balance: BalanceInfo | None):
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


def transaction_csv_name_to_xlsx(file: Path) -> str:
    """Converts a transaction CSV file name to the corresponding XLSX name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding XLSX file (e.g., 'export.csv' -> 'export-transactions.xlsx').
    """
    return file.name.replace(".csv", "-transactions.xlsx")


def store_transactions(parsed_dir: Path, file_path: Path, transactions: pl.DataFrame):
    excel_file = parsed_dir / transaction_csv_name_to_xlsx(file_path)
    logger.debug(f"Writing {excel_file=}")
    transactions.write_excel(excel_file)

    parquet_file = parsed_dir / transaction_csv_name_to_parquet(file_path)
    logger.debug(f"Writing {parquet_file=}")
    transactions.write_parquet(parquet_file)
