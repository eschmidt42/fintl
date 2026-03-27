from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from fintl.accounts_etl.scalable import broker20260309 as broker
from fintl.accounts_etl.scalable.files import (
    balance_htm_name_to_json,
    balance_htm_name_to_parquet,
    transaction_htm_name_to_parquet,
    transaction_htm_name_to_xlsx,
)
from fintl.accounts_etl.schemas import Config, Logging, OllamaConfig, Provider, Sources

PNG_FILENAME = "Screenshot 2026-03-09 at 14.30.53.png"
MOCK_AMOUNT = 1234.56
MOCK_CURRENCY = "EUR"


def get_time(path: Path) -> float:
    return path.stat().st_mtime


@pytest.fixture
def mock_lm_extraction():
    mock_result = broker._BalanceInfoExtract(amount=MOCK_AMOUNT, currency=MOCK_CURRENCY)
    mock_client = object()  # dummy; _get_lm_extraction is also patched
    with (
        patch.object(broker, "_get_ollama_client", return_value=mock_client),
        patch.object(broker, "_get_lm_extraction", return_value=mock_result),
    ):
        yield


def test_main(tmp_path: Path, mock_lm_extraction):
    broker_source_dir = (
        Path(__file__).parent.parent / "files" / "png_files" / "Scalable-Capital"
    )
    assert broker_source_dir.exists()

    logger_path = (
        Path(__file__).parent.parent.parent / "fine_logging" / "logger-config.json"
    )
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(scalable=Provider(broker=broker_source_dir)),
        logging=Logging(config_file=logger_path),
        ollama=OllamaConfig(model="fake-model"),
    )

    # paths
    raw_dir = config.get_raw_dir(broker.CASE)
    file = Path(PNG_FILENAME)
    copied_file_path = raw_dir / file

    parsed_dir = config.get_parsed_dir(broker.CASE)
    path_balance_json_single = parsed_dir / balance_htm_name_to_json(file)
    path_balance_parquet_single = parsed_dir / balance_htm_name_to_parquet(file)
    path_transactions_parquet_single = parsed_dir / transaction_htm_name_to_parquet(
        file
    )
    path_transactions_xlsx_single = parsed_dir / transaction_htm_name_to_xlsx(file)

    parser_dir = config.get_parser_dir(broker.CASE)
    path_balances_xlsx_parser = parser_dir / "balances.xlsx"
    path_balances_parquet_parser = parser_dir / "balances.parquet"
    path_transactions_parquet_parser = parser_dir / "transactions.parquet"
    path_transactions_xlsx_parser = parser_dir / "transactions.xlsx"

    # nothing should exist yet
    assert not path_balance_json_single.exists()
    assert not path_balance_parquet_single.exists()
    assert not path_transactions_parquet_single.exists()
    assert not path_transactions_xlsx_single.exists()

    assert not path_balances_xlsx_parser.exists()
    assert not path_balances_parquet_parser.exists()
    assert not path_transactions_parquet_parser.exists()
    assert not path_transactions_xlsx_parser.exists()

    # running the processing
    broker.main(config)

    # make sure the new raw file was copied as expected
    assert raw_dir.exists()
    assert copied_file_path.exists()

    # make sure the new raw file was parsed as expected
    assert parsed_dir.exists()
    assert path_balance_json_single.exists()
    assert path_balance_parquet_single.exists()
    assert path_transactions_parquet_single.exists()
    assert path_transactions_xlsx_single.exists()

    assert path_balances_xlsx_parser.exists()
    assert path_balances_parquet_parser.exists()
    assert path_transactions_parquet_parser.exists()
    assert path_transactions_xlsx_parser.exists()

    # verify extracted balance values
    balance_df = pl.read_parquet(path_balance_parquet_single)
    assert balance_df["amount"][0] == pytest.approx(MOCK_AMOUNT)
    assert balance_df["currency"][0] == MOCK_CURRENCY

    t_raw = get_time(copied_file_path)
    t_balance_json_single = get_time(path_balance_json_single)
    t_balance_parquet_single = get_time(path_balance_parquet_single)
    t_transactions_parquet_single = get_time(path_transactions_parquet_single)
    t_transactions_xlsx_single = get_time(path_transactions_xlsx_single)

    n_balances = len(pl.read_parquet(path_balances_parquet_parser))
    n_transactions = len(pl.read_parquet(path_transactions_parquet_parser))

    # running the process again ensuring nothing happens because all files are already present
    broker.main(config)

    assert t_raw == get_time(copied_file_path)
    assert t_balance_json_single == get_time(path_balance_json_single)
    assert t_balance_parquet_single == get_time(path_balance_parquet_single)
    assert t_transactions_parquet_single == get_time(path_transactions_parquet_single)
    assert t_transactions_xlsx_single == get_time(path_transactions_xlsx_single)

    n_balances_new = len(pl.read_parquet(path_balances_parquet_parser))
    n_transactions_new = len(pl.read_parquet(path_transactions_parquet_parser))

    assert n_balances == n_balances_new
    assert n_transactions == n_transactions_new

    # running the process again ensuring only parsed files are created that are missing
    path_balance_json_single.unlink()
    path_balance_parquet_single.unlink()
    path_transactions_parquet_single.unlink()
    path_transactions_xlsx_single.unlink()

    broker.main(config)

    assert t_raw == get_time(copied_file_path)
    assert t_balance_json_single < get_time(path_balance_json_single)
    assert t_balance_parquet_single < get_time(path_balance_parquet_single)
    assert t_transactions_parquet_single < get_time(path_transactions_parquet_single)
    assert t_transactions_xlsx_single < get_time(path_transactions_xlsx_single)


# ── Edge case / error path tests ──────────────────────────────────────────────


def test_get_date_from_string_raises_when_name_does_not_match(tmp_path: Path):
    """get_date_from_string must raise ValueError for a filename that does not
    match the expected 'Screenshot YYYY-MM-DD*.png' pattern."""
    import pytest

    from fintl.accounts_etl.scalable.broker20260309 import get_date_from_string

    with pytest.raises(ValueError, match="Could not extract date"):
        get_date_from_string("not_a_screenshot.txt")


def test_get_lm_extraction_calls_client_create(tmp_path: Path):
    """_get_lm_extraction must call extraction_client.create and return its result."""
    from unittest.mock import MagicMock

    from fintl.accounts_etl.scalable.broker20260309 import (
        _BalanceInfoExtract,
        _get_lm_extraction,
    )

    expected = _BalanceInfoExtract(amount=1234.56, currency="EUR")
    mock_client = MagicMock()
    mock_client.create.return_value = expected

    dummy_file = tmp_path / "Screenshot 2026-03-09 at 14.30.53.png"
    dummy_file.write_bytes(b"\x89PNG")  # minimal non-empty file

    result = _get_lm_extraction(dummy_file, mock_client)

    assert result is expected
    mock_client.create.assert_called_once()
