import logging
from pathlib import Path

import polars as pl
from rich.console import Console

from fintl.accounts_etl.common.labels import assign_labels
from fintl.accounts_etl.common.schemas import (
    BALANCE_COLUMNS,
    TRANSACTION_COLUMNS,
    Config,
)
from fintl.accounts_etl.engine import runner
from fintl.accounts_etl.parquets import concatenate_parquets

logger = logging.getLogger(__name__)


def concatenate_all_providers(config: Config) -> None:
    """Concatenates transaction and balance data from all providers.

    Iterates through all defined cases, loads their transaction and balance
    parquet files, and merges them into consolidated 'all-transactions' and
    'all-balances' output files in the target directory.

    Args:
        config: Application configuration containing directory paths and cases.

    Returns:
        None
    """

    logger.info("Concatenating all providers")

    cases = runner.all_cases()
    logger.info(f"Concatenating transactions")
    transactions = concatenate_parquets(
        "transactions.parquet", config, cases, TRANSACTION_COLUMNS
    )

    if transactions is not None:
        parquet_path = config.target_dir / "all-transactions.parquet"
        logger.info(f"Writing to {parquet_path=}")
        transactions.write_parquet(parquet_path)

        excel_path = config.target_dir / "all-transactions.xlsx"
        logger.info(f"Writing to {excel_path=}")
        transactions.write_excel(excel_path)
    else:
        logger.warning(
            f"All transaction dataframes were empty or paths did not exist, skipping writing."
        )

    logger.info(f"Concatenating balances")
    balances = concatenate_parquets("balances.parquet", config, cases, BALANCE_COLUMNS)

    if balances is not None:
        parquet_path = config.target_dir / "all-balances.parquet"
        logger.info(f"Writing to {parquet_path=}")
        balances.write_parquet(parquet_path)

        excel_path = config.target_dir / "all-balances.xlsx"
        logger.info(f"Writing to {excel_path=}")
        balances.write_excel(excel_path)
    else:
        logger.warning(
            f"All balances dataframes were empty or paths did not exist, skipping writing."
        )

    logger.info("Finished concatenating all providers")


def make_labels(config: Config) -> None:
    """Reads unlabelled transactions and applies label rules to them.

    Loads the consolidated 'all-transactions.parquet', assigns labels
    based on the configuration rules, and saves the result as both
    Parquet and Excel files with '-labelled' in the filename.

    Args:
        config: Application configuration containing target directory and label rules.

    Returns:
        None
    """
    logger.info("Preparing to assigning labels")

    # load all-transactions.parquet
    path_in: Path = config.target_dir / "all-transactions.parquet"
    logger.info(f"Reading from {path_in=}")

    if not path_in.exists():
        logger.warning(
            f"Could not assign labels, {path_in=} does not seem to exist, returning."
        )
        return

    df = pl.read_parquet(path_in)

    # assign labels
    df = assign_labels(df, config.label_rules)

    # store all-transactions-labelled.xlsx/parquet
    path_out: Path = config.target_dir / "all-transactions-labelled.parquet"
    logger.info(f"Writing to {path_out=}")
    df.write_parquet(path_out)

    path_out = path_out.with_suffix(".xlsx")
    logger.info(f"Writing to {path_out=}")
    df.write_excel(path_out)

    logger.info("Labelling done")


def main(config: Config) -> None:
    """Executes the full ETL pipeline for account data.

    Runs the enabled services, concatenates all provider data, and
    applies transaction labelling according to the configuration.

    Args:
        config: Application configuration driving the ETL process.

    Returns:
        None
    """
    console = Console()
    logger.info(f"Starting ETL pipeline")

    runner.print_etl_overview(config, console)
    runner.run_enabled_services(config, console)

    concatenate_all_providers(config)

    make_labels(config)

    logger.info("ETL pipeline done")
