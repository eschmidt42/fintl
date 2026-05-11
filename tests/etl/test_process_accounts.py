import shutil
from pathlib import Path
from typing import TypedDict

import polars as pl
import pytest

from fintl.common import Case, Config, OllamaConfig, Provider, Sources
from fintl.common.logging import Logging
from fintl.etl import process_accounts
from fintl.etl.common.schemas import (
    BalanceInfo,
)


# ── Shared fixture paths ───────────────────────────────────────────────────────
@pytest.fixture
def csv_dir(files_root_path: Path) -> Path:
    return files_root_path / "csv_files"


@pytest.fixture
def artefact_dir(files_root_path: Path) -> Path:
    return files_root_path / "artefacts"


class Dirs(TypedDict):
    dkb_giro: Path
    dkb_tagesgeld: Path
    dkb_credit: Path
    dkb_festgeld: Path
    postbank: Path
    scalable: Path
    gls_giro: Path
    gls_credit: Path


@pytest.fixture
def dirs(csv_dir: Path, artefact_dir: Path) -> Dirs:
    return {
        "dkb_giro": csv_dir / "DKB" / "kontoauszug",
        "dkb_tagesgeld": csv_dir / "DKB" / "tagesgeld",
        "dkb_credit": csv_dir / "DKB" / "credit",
        "dkb_festgeld": csv_dir / "DKB" / "festgeld",
        "postbank": csv_dir / "Postbank",
        "scalable": artefact_dir / "Scalable-Capital",
        "gls_giro": csv_dir / "GLS" / "giro",
        "gls_credit": csv_dir / "GLS" / "credit",
    }


@pytest.fixture
def dkb_giro0_file() -> str:
    return "0123456789_2022-09-15_to_2022-10-15.csv"


# _FILES = Path(__file__).parent / "providers" / "files"
# _CSV = _FILES / "csv_files"
# _ARTEFACTS = _FILES / "artefacts"
# _LOGGER_PATH = Path(__file__).parent.parent / "logger-config.json"

# _DKB_GIRO = _CSV / "DKB" / "kontoauszug"
# _DKB_TAGESGELD = _CSV / "DKB" / "tagesgeld"
# _DKB_CREDIT = _CSV / "DKB" / "credit"
# _DKB_FESTGELD = _CSV / "DKB" / "festgeld"
# _POSTBANK = _CSV / "Postbank"
# _SCALABLE = _ARTEFACTS / "Scalable-Capital"
# _GLS_GIRO = _CSV / "GLS" / "giro"
# _GLS_CREDIT = _CSV / "GLS" / "credit"

# Only the giro0 parser handles files whose name starts with 10 digits (e.g. "0123456789_...")
# _DKB_GIRO0_FILE = "0123456789_2022-09-15_to_2022-10-15.csv"


# ── Shared helpers ─────────────────────────────────────────────────────────────
def _config(target_dir: Path, sources: Sources, logger_config_path: Path) -> Config:
    return Config(
        target_dir=target_dir,
        sources=sources,
        logging=Logging(config_file=logger_config_path),
    )


def _triples(path: Path) -> set[tuple[str, str, str]]:
    """Distinct (provider, service, parser) tuples present in a parquet file."""
    df = pl.read_parquet(path)
    return set(df.select(["provider", "service", "parser"]).rows())


def _assert_labelled_output(config: Config) -> None:
    """Verify the labelled output file exists and contains the label_root column."""
    labelled_parquet = config.target_dir / "all-transactions-labelled.parquet"
    labelled_excel = config.target_dir / "all-transactions-labelled.xlsx"
    assert labelled_parquet.exists(), f"Expected {labelled_parquet} to exist"
    assert labelled_excel.exists(), f"Expected {labelled_excel} to exist"
    df = pl.read_parquet(labelled_parquet)
    assert "label_root" in df.columns
    # labelled output must have at least as many rows as all-transactions
    all_tx = pl.read_parquet(config.target_dir / "all-transactions.parquet")
    assert len(df) == len(all_tx)


def test_dirs_exist(dirs: Dirs):
    for p in dirs.values():
        assert isinstance(p, Path)
        assert p.exists()


def test_dkb_giro(tmp_path: Path, dirs: Dirs, logger_config_path: Path):

    logger_path = logger_config_path
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(giro=dirs["dkb_giro"])),
        logging=Logging(config_file=logger_path),
    )

    transactions_parquet_path = config.target_dir / "all-transactions.parquet"
    transactions_excel_path = config.target_dir / "all-transactions.xlsx"
    balances_parquet_path = config.target_dir / "all-balances.parquet"
    balances_excel_path = config.target_dir / "all-balances.xlsx"

    assert not transactions_parquet_path.exists()
    assert not transactions_excel_path.exists()
    assert not balances_parquet_path.exists()
    assert not balances_excel_path.exists()

    # run the etl
    process_accounts.main(config)

    # make sure the concatenated files exist
    assert transactions_parquet_path.exists()
    assert transactions_excel_path.exists()
    assert balances_parquet_path.exists()
    assert balances_excel_path.exists()

    transations = pl.read_parquet(transactions_parquet_path)
    balances = pl.read_parquet(balances_parquet_path)

    # run the etl again, without changing anything
    process_accounts.main(config)

    # ensuring the concatenations are reproducible
    new_transations = pl.read_parquet(transactions_parquet_path)
    new_balances = pl.read_parquet(balances_parquet_path)

    assert transations.equals(new_transations)
    assert balances.equals(new_balances)


def test_all(tmp_path: Path, dirs: Dirs, logger_config_path: Path):

    logger_path = logger_config_path
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(
            dkb=Provider(
                giro=dirs["dkb_giro"],
                tagesgeld=dirs["dkb_tagesgeld"],
                credit=dirs["dkb_credit"],
            ),
            postbank=Provider(giro=dirs["postbank"]),
            scalable=Provider(broker=dirs["scalable"]),
            gls=Provider(giro=dirs["gls_giro"], credit=dirs["gls_credit"]),
        ),
        logging=Logging(config_file=logger_path),
        ollama=None,  # keep this here - prevent s pydantic-settings to use the fallback
    )

    transactions_parquet_path = config.target_dir / "all-transactions.parquet"
    transactions_excel_path = config.target_dir / "all-transactions.xlsx"
    balances_parquet_path = config.target_dir / "all-balances.parquet"
    balances_excel_path = config.target_dir / "all-balances.xlsx"

    assert not transactions_parquet_path.exists()
    assert not transactions_excel_path.exists()
    assert not balances_parquet_path.exists()
    assert not balances_excel_path.exists()

    # run the etl
    process_accounts.main(config)

    # make sure the concatenated files exist
    assert transactions_parquet_path.exists()
    assert transactions_excel_path.exists()
    assert balances_parquet_path.exists()
    assert balances_excel_path.exists()

    transations = pl.read_parquet(transactions_parquet_path)
    balances = pl.read_parquet(balances_parquet_path)

    # run the etl again, without changing anything
    process_accounts.main(config)

    # ensuring the concatenations are reproducible
    new_transations = pl.read_parquet(transactions_parquet_path)
    new_balances = pl.read_parquet(balances_parquet_path)

    assert transations.equals(new_transations)
    assert balances.equals(new_balances)


# ── New orchestration test matrix ─────────────────────────────────────────────


def test_postbank_giro_only(tmp_path: Path, logger_config_path: Path, dirs: Dirs):
    """Postbank-only config: verifies provider/service/parser membership and labelled output."""
    config = _config(
        tmp_path, Sources(postbank=Provider(giro=dirs["postbank"])), logger_config_path
    )
    process_accounts.main(config)

    tx_path = config.target_dir / "all-transactions.parquet"
    bal_path = config.target_dir / "all-balances.parquet"
    assert tx_path.exists()
    assert bal_path.exists()

    expected = {
        ("postbank", "giro", "giro0"),
        ("postbank", "giro", "giro202305"),
    }
    assert _triples(tx_path) == expected
    assert _triples(bal_path) == expected

    _assert_labelled_output(config)


def test_scalable_broker_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    logger_config_path: Path,
    dirs: Dirs,
):
    """Scalable-only config: all three broker parsers produce balances.

    broker20260309 uses an LLM to extract data from a PNG screenshot.
    The LLM call (extract_balance) is monkeypatched to avoid a real Ollama call.
    All fixtures live in artefacts/Scalable-Capital/ (HTML + PNG merged).

    The HTML-based parsers (broker0, broker20231028) produce 0 transaction rows,
    so all-transactions.parquet is not written for this config.
    """
    from fintl.etl.providers.scalable import broker20260309

    def _fake_extract_balance(
        case: Case, file_path: Path, *, ollama_config: OllamaConfig
    ) -> BalanceInfo:
        date = broker20260309.get_date_from_string(file_path.name)
        return BalanceInfo(
            date=date,
            amount=12345.67,
            currency="EUR",
            provider=case.provider,
            service=case.service,
            parser=case.parser,
            file=str(file_path),
        )

    monkeypatch.setattr(
        broker20260309, "_check_ollama_availability", lambda *a, **kw: None
    )
    monkeypatch.setattr(broker20260309, "_check_model_available", lambda *a, **kw: None)
    monkeypatch.setattr(broker20260309, "extract_balance", _fake_extract_balance)
    scalable_src = tmp_path / "scalable_src"
    scalable_src.mkdir()
    for f in dirs["scalable"].iterdir():
        shutil.copy(f, scalable_src / f.name)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = _config(
        out_dir, Sources(scalable=Provider(broker=scalable_src)), logger_config_path
    )
    config = config.model_copy(update={"ollama": OllamaConfig(model="fake-model")})
    process_accounts.main(config)

    bal_path = config.target_dir / "all-balances.parquet"
    assert bal_path.exists()

    assert _triples(bal_path) == {
        ("scalable", "broker", "broker0"),
        ("scalable", "broker", "broker20231028"),
        ("scalable", "broker", "broker20260309"),
    }

    # no transaction rows → all-transactions.parquet is not written; labelled output absent
    assert not (config.target_dir / "all-transactions.parquet").exists()


def test_gls_giro_and_credit(tmp_path: Path, logger_config_path: Path, dirs: Dirs):
    """GLS with both giro and credit: both services appear in parquet output."""
    config = _config(
        tmp_path,
        Sources(gls=Provider(giro=dirs["gls_giro"], credit=dirs["gls_credit"])),
        logger_config_path,
    )
    process_accounts.main(config)

    tx_path = config.target_dir / "all-transactions.parquet"
    assert tx_path.exists()

    expected = {
        ("gls", "giro", "giro0"),
        ("gls", "credit", "credit0"),
    }
    assert _triples(tx_path) == expected

    _assert_labelled_output(config)


def test_dkb_selective_services(tmp_path: Path, logger_config_path: Path, dirs: Dirs):
    """DKB with giro+tagesgeld only: credit and festgeld must not appear in output."""
    config = _config(
        tmp_path,
        Sources(dkb=Provider(giro=dirs["dkb_giro"], tagesgeld=dirs["dkb_tagesgeld"])),
        logger_config_path,
    )
    process_accounts.main(config)

    tx_path = config.target_dir / "all-transactions.parquet"
    bal_path = config.target_dir / "all-balances.parquet"
    assert tx_path.exists()
    assert bal_path.exists()

    expected = {
        ("dkb", "giro", "giro0"),
        ("dkb", "giro", "giro202307"),
        ("dkb", "giro", "giro202312"),
        ("dkb", "tagesgeld", "tagesgeld0"),
        ("dkb", "tagesgeld", "tagesgeld202307"),
        ("dkb", "tagesgeld", "tagesgeld202312"),
    }
    assert _triples(tx_path) == expected
    assert _triples(bal_path) == expected

    _assert_labelled_output(config)


def test_dkb_and_postbank(tmp_path: Path, logger_config_path: Path, dirs: Dirs):
    """Two providers (DKB giro + Postbank giro): both appear in concatenated output."""
    config = _config(
        tmp_path,
        Sources(
            dkb=Provider(giro=dirs["dkb_giro"]),
            postbank=Provider(giro=dirs["postbank"]),
        ),
        logger_config_path,
    )
    process_accounts.main(config)

    tx_path = config.target_dir / "all-transactions.parquet"
    bal_path = config.target_dir / "all-balances.parquet"
    assert tx_path.exists()
    assert bal_path.exists()

    expected = {
        ("dkb", "giro", "giro0"),
        ("dkb", "giro", "giro202307"),
        ("dkb", "giro", "giro202312"),
        ("postbank", "giro", "giro0"),
        ("postbank", "giro", "giro202305"),
    }
    assert _triples(tx_path) == expected
    assert _triples(bal_path) == expected

    _assert_labelled_output(config)


def test_partial_giro_file_subset(
    tmp_path: Path, logger_config_path: Path, dirs: Dirs, dkb_giro0_file: str
):
    """Only the giro0-format file in the source dir: only the giro0 parser should run."""
    giro_source = tmp_path / "giro_source"
    giro_source.mkdir()
    shutil.copy(dirs["dkb_giro"] / dkb_giro0_file, giro_source / dkb_giro0_file)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = _config(
        out_dir, Sources(dkb=Provider(giro=giro_source)), logger_config_path
    )
    process_accounts.main(config)

    tx_path = config.target_dir / "all-transactions.parquet"
    assert tx_path.exists()

    assert _triples(tx_path) == {("dkb", "giro", "giro0")}

    _assert_labelled_output(config)


def test_mixed_dkb_giro_versions(tmp_path: Path, logger_config_path: Path, dirs: Dirs):
    """Full DKB giro dir (4 files, 3 parser versions): all versions appear, no duplicate hashes."""
    config = _config(
        tmp_path, Sources(dkb=Provider(giro=dirs["dkb_giro"])), logger_config_path
    )
    process_accounts.main(config)

    tx_path = config.target_dir / "all-transactions.parquet"
    assert tx_path.exists()

    assert _triples(tx_path) == {
        ("dkb", "giro", "giro0"),
        ("dkb", "giro", "giro202307"),
        ("dkb", "giro", "giro202312"),
    }

    df = pl.read_parquet(tx_path)
    n_unique_hashes = df["hash"].n_unique()
    assert n_unique_hashes == len(df), (
        f"Duplicate hashes found: {len(df)} rows but only {n_unique_hashes} unique hashes"
    )

    _assert_labelled_output(config)


# ── concatenate_all_providers: balances=None branch ───────────────────────────


def test_concatenate_all_providers_balances_none(
    tmp_path: Path, logger_config_path: Path, dirs: Dirs
):
    """When concatenate_parquets returns None for balances the warning branch is
    exercised and no balances parquet/xlsx files are written."""
    from unittest.mock import patch

    import polars as pl

    config = _config(
        tmp_path,
        sources=Sources(dkb=Provider(giro=dirs["dkb_giro"])),
        logger_config_path=logger_config_path,
    )

    dummy_transactions = pl.DataFrame(
        {
            col: pl.Series([], dtype=pl.Utf8)
            for col in [
                "date",
                "source",
                "recipient",
                "amount",
                "description",
                "hash",
                "provider",
                "service",
                "parser",
                "file",
            ]
        }
    )

    def _fake_concat(fname, cfg, cases, columns):
        if "balance" in fname:
            return None
        return dummy_transactions

    with patch.object(
        process_accounts, "concatenate_parquets", side_effect=_fake_concat
    ):
        process_accounts.concatenate_all_providers(config)

    assert not (config.target_dir / "all-balances.parquet").exists()
    assert not (config.target_dir / "all-balances.xlsx").exists()
