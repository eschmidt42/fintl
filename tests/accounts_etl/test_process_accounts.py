import shutil
from pathlib import Path

import polars as pl
import pytest

from fintl.accounts_etl import process_accounts
from fintl.accounts_etl.schemas import (
    BalanceInfo,
    Case,
    Config,
    Logging,
    Provider,
    Sources,
)

# ── Shared fixture paths ───────────────────────────────────────────────────────
_FILES = Path(__file__).parent / "files"
_CSV = _FILES / "csv_files"
_HTML = _FILES / "html_files"
_LOGGER_PATH = Path(__file__).parent.parent / "fine_logging" / "logger-config.json"

_DKB_GIRO = _CSV / "DKB" / "kontoauszug"
_DKB_TAGESGELD = _CSV / "DKB" / "tagesgeld"
_DKB_CREDIT = _CSV / "DKB" / "credit"
_DKB_FESTGELD = _CSV / "DKB" / "festgeld"
_POSTBANK = _CSV / "Postbank"
_SCALABLE = _HTML / "Scalable-Capital"
_SCALABLE_PNG = _FILES / "png_files" / "Scalable-Capital"
_GLS_GIRO = _CSV / "GLS" / "giro"
_GLS_CREDIT = _CSV / "GLS" / "credit"

# Only the giro0 parser handles files whose name starts with 10 digits (e.g. "0123456789_...")
_DKB_GIRO0_FILE = "0123456789_2022-09-15_to_2022-10-15.csv"


# ── Shared helpers ─────────────────────────────────────────────────────────────
def _config(target_dir: Path, sources: Sources) -> Config:
    return Config(
        target_dir=target_dir,
        sources=sources,
        logging=Logging(config_file=_LOGGER_PATH),
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


def test_dkb_giro(tmp_path: Path):
    # setup
    giro_source_dir = (
        Path(__file__).parent / "files" / "csv_files" / "DKB" / "kontoauszug"
    )
    assert giro_source_dir.exists()

    logger_path = Path(__file__).parent.parent / "fine_logging" / "logger-config.json"
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(giro=giro_source_dir)),
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


def test_all(tmp_path: Path):
    # setup
    data_root_dir = Path(__file__).parent / "files"
    assert data_root_dir.exists()
    csv_root_dir = data_root_dir / "csv_files"
    assert csv_root_dir.exists()
    html_root_dir = data_root_dir / "html_files"
    assert html_root_dir.exists()

    dkb_giro_source_dir = csv_root_dir / "DKB" / "kontoauszug"
    dkb_credit_source_dir = csv_root_dir / "DKB" / "credit"
    dkb_tagesgeld_source_dir = csv_root_dir / "DKB" / "tagesgeld"

    postbank_giro_source_dir = csv_root_dir / "Postbank"

    scalable_broker_source_dir = html_root_dir / "Scalable-Capital"

    gls_giro_source_dir = csv_root_dir / "GLS" / "giro"
    gls_credit_source_dir = csv_root_dir / "GLS" / "credit"

    assert dkb_giro_source_dir.exists()
    assert dkb_credit_source_dir.exists()
    assert dkb_tagesgeld_source_dir.exists()
    assert postbank_giro_source_dir.exists()
    assert scalable_broker_source_dir.exists()
    assert gls_giro_source_dir.exists()
    assert gls_credit_source_dir.exists()

    logger_path = Path(__file__).parent.parent / "fine_logging" / "logger-config.json"
    assert logger_path.exists()

    config = Config(
        target_dir=tmp_path,
        sources=Sources(
            dkb=Provider(
                giro=dkb_giro_source_dir,
                tagesgeld=dkb_tagesgeld_source_dir,
                credit=dkb_credit_source_dir,
            ),
            postbank=Provider(giro=postbank_giro_source_dir),
            scalable=Provider(broker=scalable_broker_source_dir),
            gls=Provider(giro=gls_giro_source_dir, credit=gls_credit_source_dir),
        ),
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


# ── New orchestration test matrix ─────────────────────────────────────────────


def test_postbank_giro_only(tmp_path: Path):
    """Postbank-only config: verifies provider/service/parser membership and labelled output."""
    config = _config(tmp_path, Sources(postbank=Provider(giro=_POSTBANK)))
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


def test_scalable_broker_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Scalable-only config: all three broker parsers produce balances.

    broker20260309 uses an LLM to extract data from a PNG screenshot.
    The LLM call (extract_balance) is monkeypatched to avoid a real Ollama call.
    The PNG fixture lives in png_files/, so a combined source dir is built from
    both html_files/ and png_files/ Scalable-Capital directories.

    The HTML-based parsers (broker0, broker20231028) produce 0 transaction rows,
    so all-transactions.parquet is not written for this config.
    """
    from fintl.accounts_etl.scalable import broker20260309

    def _fake_extract_balance(
        case: Case, file_path: Path, *, model: str = ""
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

    monkeypatch.setattr(broker20260309, "extract_balance", _fake_extract_balance)
    scalable_src = tmp_path / "scalable_src"
    scalable_src.mkdir()
    for f in _SCALABLE.iterdir():
        shutil.copy(f, scalable_src / f.name)
    for f in _SCALABLE_PNG.iterdir():
        shutil.copy(f, scalable_src / f.name)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = _config(out_dir, Sources(scalable=Provider(broker=scalable_src)))
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


def test_gls_giro_and_credit(tmp_path: Path):
    """GLS with both giro and credit: both services appear in parquet output."""
    config = _config(
        tmp_path,
        Sources(gls=Provider(giro=_GLS_GIRO, credit=_GLS_CREDIT)),
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


def test_dkb_selective_services(tmp_path: Path):
    """DKB with giro+tagesgeld only: credit and festgeld must not appear in output."""
    config = _config(
        tmp_path,
        Sources(dkb=Provider(giro=_DKB_GIRO, tagesgeld=_DKB_TAGESGELD)),
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


def test_dkb_and_postbank(tmp_path: Path):
    """Two providers (DKB giro + Postbank giro): both appear in concatenated output."""
    config = _config(
        tmp_path,
        Sources(
            dkb=Provider(giro=_DKB_GIRO),
            postbank=Provider(giro=_POSTBANK),
        ),
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


def test_partial_giro_file_subset(tmp_path: Path):
    """Only the giro0-format file in the source dir: only the giro0 parser should run."""
    giro_source = tmp_path / "giro_source"
    giro_source.mkdir()
    shutil.copy(_DKB_GIRO / _DKB_GIRO0_FILE, giro_source / _DKB_GIRO0_FILE)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = _config(out_dir, Sources(dkb=Provider(giro=giro_source)))
    process_accounts.main(config)

    tx_path = config.target_dir / "all-transactions.parquet"
    assert tx_path.exists()

    assert _triples(tx_path) == {("dkb", "giro", "giro0")}

    _assert_labelled_output(config)


def test_mixed_dkb_giro_versions(tmp_path: Path):
    """Full DKB giro dir (4 files, 3 parser versions): all versions appear, no duplicate hashes."""
    config = _config(tmp_path, Sources(dkb=Provider(giro=_DKB_GIRO)))
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


def test_concatenate_all_providers_balances_none(tmp_path: Path):
    """When concatenate_parquets returns None for balances the warning branch is
    exercised and no balances parquet/xlsx files are written."""
    from unittest.mock import patch

    import polars as pl

    config = _config(
        tmp_path,
        sources=Sources(dkb=Provider(giro=_DKB_GIRO)),
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
