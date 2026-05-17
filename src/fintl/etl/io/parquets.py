"""Utilities for reading and concatenating per-case Parquet files."""

import logging

import polars as pl

from fintl.common import Case, Config
from fintl.etl.common.schemas import ProviderEnum, ServiceEnum

logger = logging.getLogger(__name__)


def concatenate_parquets(
    fname: str, config: Config, cases: list[Case], columns: list[str]
) -> pl.DataFrame | None:
    """Concatenates data from multiple parquet files for a list of cases.

    Iterates through the provided cases, reads their parquet files, selects
    the specified columns, and concatenates the resulting DataFrames.

    Args:
        fname: Filename of the parquet file to read (e.g., 'transactions.parquet').
        config: Application configuration containing directory paths.
        cases: List of Case objects representing providers/services/parsers.
        columns: List of column names to select from each DataFrame.

    Returns:
        A concatenated DataFrame if data is found, otherwise None.
    """
    dfs = []
    for case in cases:
        path = config.get_parser_dir(case) / fname
        logger.info(f"Processing {path=}.")

        if not path.exists():
            logger.warning(
                f"{path=} does not exist for {case.provider} / {case.service} / {case.parser}, skipping."  # noqa: E501
            )
            continue

        tmp = pl.read_parquet(path)
        n_rows = len(tmp)
        is_transactions = fname == "transactions.parquet"
        is_scalable_broker = (
            case.provider == ProviderEnum.scalable and case.service == ServiceEnum.broker
        )
        if n_rows == 0:
            if not (is_transactions and is_scalable_broker):
                logger.warning(
                    f"{n_rows=} for {case.provider} / {case.service} / {case.parser}, skipping {fname}."  # noqa: E501
                )
            continue
        else:
            logger.info(f"Appending {len(tmp):_d} rows for {case=}")

        tmp = tmp.select(columns)

        dfs.append(tmp)

    if len(dfs) > 0:
        return pl.concat(dfs)
    else:
        return None
