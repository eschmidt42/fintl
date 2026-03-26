from pathlib import Path
from typing import Callable

import polars as pl

from fintl.accounts_etl.dkb.files import (
    balance_csv_name_to_json,
    balance_csv_name_to_parquet,
    concatenate_balances_history,
    concatenate_transactions_history,
    detect_raw_files,
    detect_relevant_source_files,
    logger,
    select_files_to_parse,
    transaction_csv_name_to_parquet,
    transaction_csv_name_to_xlsx,
)
from fintl.accounts_etl.files import detect_present_parsed_files
from fintl.accounts_etl.schemas import BALANCE_SCHEMA, BalanceInfo, Case, Config


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


def get_parser_source_files(
    case: Case, config: Config, check_if_parser_applies: Callable
) -> list[Path]:
    source_dir = config.get_source_dir(case.provider, case.service)
    relevant_source_files = detect_relevant_source_files(
        source_dir, check_if_parser_applies
    )
    return relevant_source_files


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


def store_transactions(parsed_dir: Path, file_path: Path, transactions: pl.DataFrame):
    excel_file = parsed_dir / transaction_csv_name_to_xlsx(file_path)
    logger.debug(f"Writing {excel_file=}")
    transactions.write_excel(excel_file)

    parquet_file = parsed_dir / transaction_csv_name_to_parquet(file_path)
    logger.debug(f"Writing {parquet_file=}")
    transactions.write_parquet(parquet_file)
