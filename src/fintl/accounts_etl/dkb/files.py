import logging
from pathlib import Path
from typing import Callable

import polars as pl

from fintl.accounts_etl.scalable.files import (
    balance_htm_name_to_parquet,
    transaction_htm_name_to_parquet,
)
from fintl.accounts_etl.schemas import (
    TRANSACTION_COLUMNS,
    TransactionColumnsEnum,
)

logger = logging.getLogger(__name__)


def detect_relevant_source_files(
    source_dir: Path, check_if_parser_applies: Callable
) -> list[Path]:
    """Detects relevant raw files in the given source directory."""
    relevant_source_files = [
        file_path
        for file_path in source_dir.glob("**/*.csv")
        if check_if_parser_applies(file_path)
    ]
    logger.info(
        f"Detected {len(relevant_source_files):_} relevant source files @ {source_dir=}."
    )
    return relevant_source_files


def detect_raw_files(raw_dir: Path, check_if_parser_applies: Callable) -> list[Path]:
    """Detects relevant raw files."""
    raw_files = [
        file_path
        for file_path in raw_dir.glob("**/*.csv")
        if check_if_parser_applies(file_path)
    ]
    logger.info(f"Detected {len(raw_files):_} raw files @ {raw_dir=}.")
    return raw_files


def select_files_to_parse(present_parsed_files: list[Path], raw_files: list[Path]):
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


def transaction_csv_name_to_xlsx(file: Path) -> str:
    return file.name.replace(".csv", "-transactions.xlsx")


def transaction_csv_name_to_parquet(file: Path) -> str:
    return file.name.replace(".csv", "-transactions.parquet")


def balance_csv_name_to_json(file: Path) -> str:
    return file.name.replace(".csv", "-balance.json")


def balance_csv_name_to_parquet(file: Path) -> str:
    return file.name.replace(".csv", "-balance.parquet")


def balance_name_to_parquet(file: Path) -> str:
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


def transaction_name_to_parquet(file: Path) -> str:
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


# TODO: refactor this function, barely readable
def concatenate_new_transactions(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
) -> tuple[pl.DataFrame | None, int]:
    all_transactions_file = parser_dir / "transactions.parquet"

    newly_parsed_transactions = []
    common_columns, discarded_columns = None, []
    for file_path in new_files_to_parse:
        parquet_file_path = parsed_dir / transaction_name_to_parquet(file_path)
        if not parquet_file_path.exists():
            logger.warning(f"{parquet_file_path=} does not exist, skipping.")
            continue
        transaction_df = pl.read_parquet(parquet_file_path)
        newly_parsed_transactions.append(transaction_df)
        logger.debug(f"Processing {parquet_file_path}: Shape = {transaction_df.shape}")

        # finding common columns
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

    if len(newly_parsed_transactions) == 0:
        logger.warning(f"{len(newly_parsed_transactions)=:_}, returning empty.")
        return None, 0

    newly_parsed_transactions = [
        df.select(TRANSACTION_COLUMNS) for df in newly_parsed_transactions
    ]

    newly_parsed_transactions = pl.concat(newly_parsed_transactions)

    n_old = 0
    if all_transactions_file.exists():
        old_transactions = pl.read_parquet(all_transactions_file)

        old_transactions = old_transactions.select(TRANSACTION_COLUMNS)

        n_old = len(old_transactions)
        transactions = pl.concat(
            [old_transactions, newly_parsed_transactions],
        )
    else:
        transactions = newly_parsed_transactions

    transactions = transactions.sort("date")
    transactions = transactions.unique(subset=["hash"])
    n_new = len(transactions)
    return transactions, n_new - n_old


def concatenate_new_balances(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
) -> tuple[pl.DataFrame, int]:
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


def concatenate_transactions_history(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
):
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


def concatenate_balances_history(
    parser_dir: Path, parsed_dir: Path, new_files_to_parse: list[Path]
):
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
