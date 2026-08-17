"""Tests for scalable.broker20260309 parser."""

import logging
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from openai.types import CompletionUsage
from openai.types.chat.chat_completion import ChatCompletion
from openai.types.completion_usage import CompletionTokensDetails

import fintl.common.extraction.ollama
from fintl.common import Config, OllamaConfig, Provider, Sources
from fintl.common.extraction import ModelProvider, availability, ollama
from fintl.common.extraction.errors import OllamaModelUnavailableError, OllamaUnavailableError
from fintl.common.logging import Logging
from fintl.etl.io.files.filenames import (
    balance_htm_name_to_json,
    balance_htm_name_to_parquet,
    transaction_htm_name_to_parquet,
    transaction_htm_name_to_xlsx,
)
from fintl.etl.providers.scalable import broker20260309 as broker
from fintl.etl.providers.scalable.broker20260309 import (
    get_date_from_string,
)

PNG_FILENAME = "Screenshot 2026-03-09 at 14.30.53.png"
MOCK_AMOUNT = 1234.56
MOCK_CURRENCY = "EUR"


def _config(tmp_path: Path, logger_config_path: Path) -> Config:
    scalable_src = tmp_path / "scalable_src"
    scalable_src.mkdir()
    target_dir = tmp_path / "out"
    target_dir.mkdir()

    return Config(
        target_dir=target_dir,
        sources=Sources(scalable=Provider(broker=scalable_src)),
        logging=Logging(config_file=logger_config_path),
    )


def test_files_exist(files_root_path: Path, png_file: Path):
    """Test that required fixture files exist."""
    assert files_root_path.exists()
    assert png_file.exists()


def get_time(path: Path) -> float:
    """Return the modification time of a path."""
    return path.stat().st_mtime


@pytest.fixture
def mock_lm_extraction(monkeypatch: pytest.MonkeyPatch):
    """Provide patched Ollama extraction helpers that return a fixed mock result."""
    mock_extraction = fintl.common.extraction.ollama._BalanceInfoExtract(
        amount=MOCK_AMOUNT, currency=MOCK_CURRENCY
    )
    mock_completion = ChatCompletion.model_construct(
        id="test-id",
        choices=[],
        created=0,
        model="fake-model",
        object="chat.completion",
        usage=CompletionUsage.model_construct(
            completion_tokens=1,
            prompt_tokens=1,
            total_tokens=2,
            completion_tokens_details=CompletionTokensDetails.model_construct(reasoning_tokens=0),
        ),
    )

    monkeypatch.setattr(availability, "check_ollama_availability", lambda *a, **kw: None)
    monkeypatch.setattr(availability, "check_ollama_model_availability", lambda *a, **kw: None)
    monkeypatch.setattr(ollama, "_get_client", lambda **kw: object())
    monkeypatch.setattr(
        ollama, "_get_extraction", lambda *a, **kw: (mock_extraction, mock_completion)
    )


def test_main(tmp_path: Path, mock_lm_extraction, png_file: Path, logger_config_path: Path):
    """Test that the broker20260309 parser runs end-to-end and produces expected output files."""
    broker_source_dir = png_file.parent
    assert broker_source_dir.exists()

    logger_path = logger_config_path
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(scalable=Provider(broker=broker_source_dir)),
        logging=Logging(config_file=logger_path),
        ollama=OllamaConfig(model="fake-model"),
        model_provider=ModelProvider.ollama,
    )

    # paths
    raw_dir = config.get_raw_dir(broker.CASE)
    file = Path(PNG_FILENAME)
    copied_file_path = raw_dir / file

    parsed_dir = config.get_parsed_dir(broker.CASE)
    path_balance_json_single = parsed_dir / balance_htm_name_to_json(file)
    path_balance_parquet_single = parsed_dir / balance_htm_name_to_parquet(file)
    path_transactions_parquet_single = parsed_dir / transaction_htm_name_to_parquet(file)
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


def test_get_date_from_string_raises_when_name_does_not_match():
    """get_date_from_string raises ValueError for non-matching filename."""
    with pytest.raises(ValueError, match="Could not extract date"):
        get_date_from_string("not_a_screenshot.txt")


def test_parse_new_files_skips_when_ollama_not_configured(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, png_fname: str, logger_config_path: Path
):
    """parse_new_files logs a warning and returns early when ollama_config is None."""
    dummy = tmp_path / png_fname
    dummy.write_bytes(b"\x89PNG")

    config = _config(tmp_path, logger_config_path)
    config.model_provider = ModelProvider.ollama
    config.ollama = None

    with caplog.at_level(logging.WARNING, logger="fintl.etl.scalable.broker20260309"):
        result = broker.parse_new_files(broker.CASE, [dummy], tmp_path / "parsed", config=config)

    assert result == []
    assert "Ollama configuration missing" in caplog.text
    assert not (tmp_path / "parsed").exists()


def test_parse_new_files_aborts_on_ollama_unavailable(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, logger_config_path: Path
):
    """parse_new_files stops before the loop when _check_ollama_availability raises."""
    files = [
        tmp_path / "Screenshot 2026-03-09 at 14.30.53.png",
        tmp_path / "Screenshot 2026-03-10 at 14.30.53.png",
    ]
    for f in files:
        f.write_bytes(b"\x89PNG")
    parsed_dir = tmp_path / "parsed"

    config = _config(tmp_path, logger_config_path)
    config.model_provider = ModelProvider.ollama

    with patch.object(
        availability,
        "check_ollama_availability",
        side_effect=OllamaUnavailableError("server down"),
    ):
        with caplog.at_level(logging.WARNING, logger="fintl.etl.scalable.broker20260309"):
            broker.parse_new_files(broker.CASE, files, parsed_dir, config=config)

    assert "Ollama is not available" in caplog.text
    assert not parsed_dir.exists()


def test_parse_new_files_aborts_on_model_unavailable(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, logger_config_path: Path
):
    """parse_new_files stops before the loop when _check_model_available raises."""
    dummy = tmp_path / "Screenshot 2026-03-09 at 14.30.53.png"
    dummy.write_bytes(b"\x89PNG")
    parsed_dir = tmp_path / "parsed"

    config = _config(tmp_path, logger_config_path)
    config.model_provider = ModelProvider.ollama
    config.ollama = OllamaConfig.model_validate({"model": "m"})

    with (
        patch.object(availability, "check_ollama_availability"),
        patch.object(
            availability,
            "check_ollama_model_availability",
            side_effect=OllamaModelUnavailableError("model not found"),
        ),
    ):
        with caplog.at_level(logging.WARNING, logger="fintl.etl.scalable.broker20260309"):
            broker.parse_new_files(broker.CASE, [dummy], parsed_dir, config=config)

    assert "Ollama model (m) not available" in caplog.text
    assert not parsed_dir.exists()


def test_parse_new_files_continues_on_generic_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    logger_config_path: Path,
):
    """parse_new_files skips a file on generic Exception and continues with remaining files."""
    files = [
        tmp_path / "Screenshot 2026-03-09 at 14.30.53.png",
        tmp_path / "Screenshot 2026-03-10 at 14.30.53.png",
    ]
    for f in files:
        f.write_bytes(b"\x89PNG")

    parsed_dir = tmp_path / "parsed"

    call_count = 0

    def _raise_generic(*args, **kwargs):
        """Increment call count and always raise ValueError."""
        nonlocal call_count
        call_count += 1
        raise ValueError("parse failed")

    (monkeypatch.setattr(availability, "check_ollama_availability", lambda *a, **kw: None),)
    (monkeypatch.setattr(availability, "check_ollama_model_availability", lambda *a, **kw: None),)
    (monkeypatch.setattr(broker, "parse_image_file", _raise_generic),)

    config = _config(tmp_path, logger_config_path)
    config.model_provider = ModelProvider.ollama
    config.ollama = OllamaConfig.model_validate({"model": "m"})

    with caplog.at_level(logging.WARNING, logger="fintl.etl.scalable.broker20260309"):
        broker.parse_new_files(broker.CASE, files, parsed_dir, config=config)

    assert "parse failed" in caplog.text
    # Both files attempted (error is per-file, not fatal)
    assert call_count == 2


def test_main_no_ollama_png_files_exist(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    png_file: Path,
    logger_config_path: Path,
):
    """main() completes without error when PNG files exist but ollama is not configured.

    ETL should: copy PNGs to raw_dir, skip parsing (no parquets), skip history
    concatenation (nothing was parsed), and log a warning about missing ollama config.
    """
    broker_source_dir = png_file.parent
    assert broker_source_dir.exists()

    logger_path = logger_config_path
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(scalable=Provider(broker=broker_source_dir)),
        logging=Logging(config_file=logger_path),
        ollama=None,  # opt-out
        model_provider=ModelProvider.ollama,
    )

    with caplog.at_level(logging.WARNING, logger="fintl.etl.scalable.broker20260309"):
        broker.main(config)  # must not raise

    raw_dir = config.get_raw_dir(broker.CASE)
    parsed_dir = config.get_parsed_dir(broker.CASE)
    parser_dir = config.get_parser_dir(broker.CASE)

    # PNG is copied to raw_dir
    assert raw_dir.exists()
    assert (raw_dir / PNG_FILENAME).exists()

    # No parquets created (parsing was skipped)
    assert not parsed_dir.exists()

    # No parser-level history created (nothing was parsed to concatenate)
    assert not (parser_dir / "balances.parquet").exists()
    assert not (parser_dir / "transactions.parquet").exists()

    assert "Ollama configuration missing" in caplog.text
