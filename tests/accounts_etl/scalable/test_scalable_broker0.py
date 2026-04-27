from pathlib import Path

import polars as pl
import pytest

from fintl.accounts_etl.scalable import broker0 as broker
from fintl.accounts_etl.scalable.files import (
    balance_htm_name_to_json,
    balance_htm_name_to_parquet,
    transaction_htm_name_to_parquet,
    transaction_htm_name_to_xlsx,
)
from fintl.accounts_etl.schemas import Config, Logging, Provider, Sources


def get_time(path: Path) -> float:
    return path.stat().st_mtime


def test_main(tmp_path: Path):
    broker_source_dir = (
        Path(__file__).parent.parent / "files" / "artefacts" / "Scalable-Capital"
    )
    assert broker_source_dir.exists()

    logger_path = Path(__file__).parent.parent.parent / "logger-config.json"
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(scalable=Provider(broker=broker_source_dir)),
        logging=Logging(config_file=logger_path),
    )

    # paths
    raw_dir = config.get_raw_dir(broker.CASE)
    file = Path("2022-08-12.html")
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

    # make sure the new raw fille was parsed as expected
    assert parsed_dir.exists()
    assert path_balance_json_single.exists()
    assert path_balance_parquet_single.exists()
    assert path_transactions_parquet_single.exists()
    assert path_transactions_xlsx_single.exists()

    assert path_balances_xlsx_parser.exists()
    assert path_balances_parquet_parser.exists()
    assert path_transactions_parquet_parser.exists()
    assert path_transactions_xlsx_parser.exists()

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


def test_check_if_parser_applies_date_none_raises(tmp_path: Path):
    """check_if_parser_applies must raise ValueError when the inner date regex
    returns None (defensive check that cannot normally be triggered without
    mocking, since the outer pattern already requires the date format)."""
    from unittest.mock import MagicMock, patch

    file_path = tmp_path / "2022-08-12.html"
    file_path.write_text("€")

    outer_match = MagicMock()  # truthy – outer pattern 'matches'
    # First call returns the outer match; second call returns None (inner date pattern)
    with patch("fintl.accounts_etl.scalable.broker0.re") as mock_re:
        mock_re.search.side_effect = [outer_match, None]
        with pytest.raises(ValueError, match="is None"):
            broker.check_if_parser_applies(file_path)


def test_extract_balance_raises_when_large_price_missing(tmp_path: Path):
    """extract_balance must raise ValueError when large-price div is absent."""
    html = "<html><body><div>no price here</div></body></html>"
    file_path = tmp_path / "2022-08-12.html"
    file_path.write_text(html)
    with pytest.raises(ValueError):
        broker.extract_balance(broker.CASE, file_path, [])


def test_extract_balance_raises_when_decimal_missing(tmp_path: Path):
    """extract_balance must raise ValueError when decimal div is absent."""
    html = (
        '<html><body><div data-testid="large-price"><div>1234</div></div></body></html>'
    )
    file_path = tmp_path / "2022-08-12.html"
    file_path.write_text(html)
    with pytest.raises(ValueError):
        broker.extract_balance(broker.CASE, file_path, [])


def test_extract_balance_raises_when_suffix_missing(tmp_path: Path):
    """extract_balance must raise ValueError when suffix div is absent."""
    html = (
        "<html><body>"
        '<div data-testid="large-price"><div>1234</div></div>'
        '<div data-testid="decimal">69</div>'
        "</body></html>"
    )
    file_path = tmp_path / "2022-08-12.html"
    file_path.write_text(html)
    with pytest.raises(ValueError):
        broker.extract_balance(broker.CASE, file_path, [])


def test_extract_balance_currency_none_falls_back_to_empty_string(tmp_path: Path):
    """When suffix > div has multiple string children .string returns None;
    currency must fall back to ''."""
    # Two spans inside the inner div → BeautifulSoup .string returns None
    html = (
        "<html><body>"
        '<div data-testid="large-price"><div>1234</div></div>'
        '<div data-testid="decimal">69</div>'
        '<div data-testid="suffix"><div><span>€</span><span> </span></div></div>'
        "</body></html>"
    )
    file_path = tmp_path / "2022-08-12.html"
    file_path.write_text(html)
    balance = broker.extract_balance(broker.CASE, file_path, [])
    assert balance.currency == ""
