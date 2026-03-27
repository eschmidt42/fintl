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
        patch.object(broker, "_check_ollama_availability"),
        patch.object(broker, "_check_model_available"),
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


def test_check_ollama_availability_raises_on_connection_failure():
    """_check_ollama_availability raises OllamaUnavailableError when the server is unreachable."""
    from unittest.mock import patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import (
        OllamaUnavailableError,
        _check_ollama_availability,
    )

    with patch.object(
        httpx, "get", side_effect=httpx.ConnectError("connection refused")
    ):
        with pytest.raises(OllamaUnavailableError, match="not reachable"):
            _check_ollama_availability("http://localhost:11434/v1")


def test_parse_new_files_skips_when_ollama_not_configured(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """parse_new_files logs a warning and returns early when ollama_config is None."""
    import logging

    from fintl.accounts_etl.scalable import broker20260309 as broker

    dummy = tmp_path / "Screenshot 2026-03-09 at 14.30.53.png"
    dummy.write_bytes(b"\x89PNG")

    with caplog.at_level(
        logging.WARNING, logger="fintl.accounts_etl.scalable.broker20260309"
    ):
        broker.parse_new_files(
            broker.CASE, [dummy], tmp_path / "parsed", ollama_config=None
        )

    assert "Ollama is not configured" in caplog.text
    assert not (tmp_path / "parsed").exists()


def test_check_ollama_availability_strips_v1_suffix():
    """_check_ollama_availability GET-s the root URL (without /v1)."""
    from unittest.mock import MagicMock, patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import _check_ollama_availability

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch.object(httpx, "get", return_value=mock_response) as mock_get:
        _check_ollama_availability("http://localhost:11434/v1")

    mock_get.assert_called_once_with("http://localhost:11434", timeout=5.0)


def test_get_ollama_client_propagates_provider_error():
    """_get_ollama_client lets exceptions from instructor.from_provider bubble up."""
    from unittest.mock import patch

    from fintl.accounts_etl.scalable import broker20260309 as broker

    with patch.object(
        broker.instructor,
        "from_provider",
        side_effect=ValueError("bad model"),
    ):
        with pytest.raises(ValueError, match="bad model"):
            broker._get_ollama_client(model="bad-model")


def test_check_ollama_availability_uses_base_url_as_is_without_v1_suffix():
    """_check_ollama_availability uses base_url unchanged when it has no /v1 suffix."""
    from unittest.mock import MagicMock, patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import _check_ollama_availability

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch.object(httpx, "get", return_value=mock_response) as mock_get:
        _check_ollama_availability("http://localhost:11434")

    mock_get.assert_called_once_with("http://localhost:11434", timeout=5.0)


def test_check_model_available_raises_when_bare_name_also_missing():
    """_check_model_available raises when model has no tag and no bare-name match."""
    from unittest.mock import MagicMock, patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import (
        OllamaModelUnavailableError,
        _check_model_available,
    )

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "llama3.2:latest"}]}
    with patch.object(httpx, "get", return_value=mock_response):
        with pytest.raises(OllamaModelUnavailableError, match="qwen3.5"):
            _check_model_available("http://localhost:11434/v1", "qwen3.5")


def test_check_model_available_uses_base_url_as_is_without_v1_suffix():
    """_check_model_available calls /api/tags on the URL when /v1 is absent."""
    from unittest.mock import MagicMock, patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import _check_model_available

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "qwen3.5:27b"}]}
    with patch.object(httpx, "get", return_value=mock_response) as mock_get:
        _check_model_available("http://localhost:11434", "qwen3.5:27b")

    mock_get.assert_called_once_with("http://localhost:11434/api/tags", timeout=5.0)


def test_parse_new_files_aborts_on_ollama_unavailable(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """parse_new_files stops before the loop when _check_ollama_availability raises."""
    import logging
    from unittest.mock import patch

    from fintl.accounts_etl.scalable import broker20260309 as broker
    from fintl.accounts_etl.schemas import OllamaConfig

    files = [
        tmp_path / "Screenshot 2026-03-09 at 14.30.53.png",
        tmp_path / "Screenshot 2026-03-10 at 14.30.53.png",
    ]
    for f in files:
        f.write_bytes(b"\x89PNG")
    parsed_dir = tmp_path / "parsed"

    with patch.object(
        broker,
        "_check_ollama_availability",
        side_effect=broker.OllamaUnavailableError("server down"),
    ):
        with caplog.at_level(
            logging.WARNING, logger="fintl.accounts_etl.scalable.broker20260309"
        ):
            broker.parse_new_files(
                broker.CASE, files, parsed_dir, ollama_config=OllamaConfig(model="m")
            )

    assert "Ollama is not available" in caplog.text
    assert not parsed_dir.exists()


def test_parse_new_files_aborts_on_model_unavailable(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """parse_new_files stops before the loop when _check_model_available raises."""
    import logging
    from unittest.mock import patch

    from fintl.accounts_etl.scalable import broker20260309 as broker
    from fintl.accounts_etl.schemas import OllamaConfig

    dummy = tmp_path / "Screenshot 2026-03-09 at 14.30.53.png"
    dummy.write_bytes(b"\x89PNG")
    parsed_dir = tmp_path / "parsed"

    with (
        patch.object(broker, "_check_ollama_availability"),
        patch.object(
            broker,
            "_check_model_available",
            side_effect=broker.OllamaModelUnavailableError("model not found"),
        ),
    ):
        with caplog.at_level(
            logging.WARNING, logger="fintl.accounts_etl.scalable.broker20260309"
        ):
            broker.parse_new_files(
                broker.CASE, [dummy], parsed_dir, ollama_config=OllamaConfig(model="m")
            )

    assert "Ollama model (m) not available" in caplog.text
    assert not parsed_dir.exists()


def test_check_model_available_passes_when_model_present():
    """_check_model_available does not raise when the model is in the tags response."""
    from unittest.mock import MagicMock, patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import _check_model_available

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "models": [{"name": "qwen3.5:27b"}, {"name": "llama3.2:latest"}]
    }
    with patch.object(httpx, "get", return_value=mock_response):
        _check_model_available("http://localhost:11434/v1", "qwen3.5:27b")  # no raise


def test_check_model_available_passes_on_bare_name_match():
    """_check_model_available accepts a bare model name that matches before the colon."""
    from unittest.mock import MagicMock, patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import _check_model_available

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "qwen3.5:27b"}]}
    with patch.object(httpx, "get", return_value=mock_response):
        _check_model_available("http://localhost:11434/v1", "qwen3.5")  # no raise


def test_check_model_available_raises_when_model_missing():
    """_check_model_available raises OllamaModelUnavailableError for an absent model."""
    from unittest.mock import MagicMock, patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import (
        OllamaModelUnavailableError,
        _check_model_available,
    )

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"models": [{"name": "llama3.2:latest"}]}
    with patch.object(httpx, "get", return_value=mock_response):
        with pytest.raises(OllamaModelUnavailableError, match="qwen3.5:27b"):
            _check_model_available("http://localhost:11434/v1", "qwen3.5:27b")


def test_check_model_available_raises_on_http_error():
    """_check_model_available raises OllamaModelUnavailableError when the tags call fails."""
    from unittest.mock import patch

    import httpx

    from fintl.accounts_etl.scalable.broker20260309 import (
        OllamaModelUnavailableError,
        _check_model_available,
    )

    with patch.object(
        httpx, "get", side_effect=httpx.ConnectError("connection refused")
    ):
        with pytest.raises(OllamaModelUnavailableError, match="Could not retrieve"):
            _check_model_available("http://localhost:11434/v1", "qwen3.5:27b")


def test_parse_new_files_continues_on_generic_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """parse_new_files skips a file on generic Exception and continues with remaining files."""
    import logging
    from unittest.mock import patch

    from fintl.accounts_etl.scalable import broker20260309 as broker
    from fintl.accounts_etl.schemas import OllamaConfig

    files = [
        tmp_path / "Screenshot 2026-03-09 at 14.30.53.png",
        tmp_path / "Screenshot 2026-03-10 at 14.30.53.png",
    ]
    for f in files:
        f.write_bytes(b"\x89PNG")
    parsed_dir = tmp_path / "parsed"

    call_count = 0

    def _raise_generic(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise ValueError("parse failed")

    with (
        patch.object(broker, "_check_ollama_availability"),
        patch.object(broker, "_check_model_available"),
        patch.object(broker, "parse_image_file", side_effect=_raise_generic),
    ):
        with caplog.at_level(
            logging.WARNING, logger="fintl.accounts_etl.scalable.broker20260309"
        ):
            broker.parse_new_files(
                broker.CASE, files, parsed_dir, ollama_config=OllamaConfig(model="m")
            )

    assert "parse failed" in caplog.text
    # Both files attempted (error is per-file, not fatal)
    assert call_count == 2
