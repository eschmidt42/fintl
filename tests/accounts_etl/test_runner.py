"""Unit tests for fintl.accounts_etl.runner.

Tests use lightweight fake ``ParserSpec`` instances and monkeypatch
``ALL_PARSERS`` so no real filesystem access or parser execution is required.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fintl.accounts_etl import runner
from fintl.accounts_etl.schemas import (
    Case,
    Config,
    Logging,
    ParserSpec,
    Provider,
    Sources,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

_LOGGER_PATH = Path(__file__).parent.parent / "fine_logging" / "logger-config.json"
_CSV_DIR = Path(__file__).parent / "files" / "csv_files" / "DKB" / "kontoauszug"


def _config(tmp_path: Path, sources: Sources) -> Config:
    return Config(
        target_dir=tmp_path,
        sources=sources,
        logging=Logging(config_file=_LOGGER_PATH),
    )


def _spec(provider: str, service: str, parser: str, precedence: int = 0) -> ParserSpec:
    """Build a fake ParserSpec with MagicMock callables."""
    return ParserSpec(
        case=Case(provider=provider, service=service, parser=parser),
        applies=MagicMock(return_value=True),
        run=MagicMock(),
        precedence=precedence,
    )


# ── parsers_for ───────────────────────────────────────────────────────────────


def test_parsers_for_filters_by_provider_and_service():
    specs = [
        _spec("dkb", "giro", "giro0", precedence=0),
        _spec("dkb", "giro", "giro202312", precedence=20),
        _spec("dkb", "credit", "credit0", precedence=0),
        _spec("postbank", "giro", "giro0", precedence=0),
    ]
    with patch.object(runner, "ALL_PARSERS", specs):
        result = runner.parsers_for("dkb", "giro")

    assert [s.case.parser for s in result] == ["giro0", "giro202312"]


def test_parsers_for_sorts_by_precedence():
    specs = [
        _spec("dkb", "giro", "giro202312", precedence=20),
        _spec("dkb", "giro", "giro0", precedence=0),
        _spec("dkb", "giro", "giro202307", precedence=10),
    ]
    with patch.object(runner, "ALL_PARSERS", specs):
        result = runner.parsers_for("dkb", "giro")

    assert [s.case.parser for s in result] == ["giro0", "giro202307", "giro202312"]


def test_parsers_for_returns_empty_for_unknown_pair():
    specs = [_spec("dkb", "giro", "giro0")]
    with patch.object(runner, "ALL_PARSERS", specs):
        result = runner.parsers_for("postbank", "giro")

    assert result == []


# ── all_cases ─────────────────────────────────────────────────────────────────


def test_all_cases_returns_all_when_no_filter():
    specs = [
        _spec("dkb", "giro", "giro0"),
        _spec("postbank", "giro", "giro0"),
    ]
    with patch.object(runner, "ALL_PARSERS", specs):
        cases = runner.all_cases()

    assert len(cases) == 2
    assert {c.provider for c in cases} == {"dkb", "postbank"}


def test_all_cases_filters_by_provider():
    specs = [
        _spec("dkb", "giro", "giro0"),
        _spec("dkb", "credit", "credit0"),
        _spec("postbank", "giro", "giro0"),
    ]
    with patch.object(runner, "ALL_PARSERS", specs):
        cases = runner.all_cases(provider="dkb")

    assert all(c.provider == "dkb" for c in cases)
    assert len(cases) == 2


def test_all_cases_preserves_registry_order():
    specs = [
        _spec("dkb", "giro", "giro0"),
        _spec("dkb", "giro", "giro202312"),
        _spec("postbank", "giro", "giro0"),
    ]
    with patch.object(runner, "ALL_PARSERS", specs):
        cases = runner.all_cases()

    assert [c.parser for c in cases] == ["giro0", "giro202312", "giro0"]


# ── check_service_overlap ─────────────────────────────────────────────────────


def test_check_service_overlap_passes_when_no_overlap(tmp_path: Path):
    file_a = tmp_path / "a.csv"
    file_b = tmp_path / "b.csv"

    spec_a = _spec("dkb", "giro", "giro0")
    spec_b = _spec("dkb", "giro", "giro202312")

    sources = Sources(dkb=Provider(giro=tmp_path))
    config = _config(tmp_path, sources)

    with (
        patch.object(runner, "ALL_PARSERS", [spec_a, spec_b]),
        patch.object(runner, "_get_source_files", side_effect=[[file_a], [file_b]]),
    ):
        runner.check_service_overlap(config, "dkb", "giro")  # must not raise


def test_check_service_overlap_raises_on_overlap(tmp_path: Path):
    shared_file = tmp_path / "shared.csv"

    spec_a = _spec("dkb", "giro", "giro0")
    spec_b = _spec("dkb", "giro", "giro202312")

    sources = Sources(dkb=Provider(giro=tmp_path))
    config = _config(tmp_path, sources)

    with (
        patch.object(runner, "ALL_PARSERS", [spec_a, spec_b]),
        patch.object(
            runner, "_get_source_files", side_effect=[[shared_file], [shared_file]]
        ),
    ):
        with pytest.raises(ValueError):
            runner.check_service_overlap(config, "dkb", "giro")


# ── run_service ───────────────────────────────────────────────────────────────


def test_run_service_calls_parsers_in_precedence_order(tmp_path: Path):
    call_order: list[str] = []

    def make_run(name: str):
        def run(_config):
            call_order.append(name)

        return run

    spec_low = ParserSpec(
        case=Case(provider="dkb", service="giro", parser="giro0"),
        applies=MagicMock(return_value=True),
        run=make_run("giro0"),
        precedence=0,
    )
    spec_high = ParserSpec(
        case=Case(provider="dkb", service="giro", parser="giro202312"),
        applies=MagicMock(return_value=True),
        run=make_run("giro202312"),
        precedence=20,
    )

    sources = Sources(dkb=Provider(giro=tmp_path))
    config = _config(tmp_path, sources)

    with (
        patch.object(runner, "ALL_PARSERS", [spec_high, spec_low]),
        patch.object(runner, "_get_source_files", return_value=[]),
    ):
        runner.run_service(config, "dkb", "giro")

    assert call_order == ["giro0", "giro202312"]


def test_run_service_raises_on_overlap(tmp_path: Path):
    shared_file = tmp_path / "shared.csv"

    spec_a = _spec("dkb", "giro", "giro0")
    spec_b = _spec("dkb", "giro", "giro202312")

    sources = Sources(dkb=Provider(giro=tmp_path))
    config = _config(tmp_path, sources)

    with (
        patch.object(runner, "ALL_PARSERS", [spec_a, spec_b]),
        patch.object(
            runner, "_get_source_files", side_effect=[[shared_file], [shared_file]]
        ),
    ):
        with pytest.raises(ValueError):
            runner.run_service(config, "dkb", "giro")


def test_run_service_does_not_run_parser_after_overlap(tmp_path: Path):
    shared_file = tmp_path / "shared.csv"

    spec_a = _spec("dkb", "giro", "giro0")
    spec_b_run = MagicMock()
    spec_b = ParserSpec(
        case=Case(provider="dkb", service="giro", parser="giro202312"),
        applies=MagicMock(return_value=True),
        run=spec_b_run,
        precedence=20,
    )

    sources = Sources(dkb=Provider(giro=tmp_path))
    config = _config(tmp_path, sources)

    with (
        patch.object(runner, "ALL_PARSERS", [spec_a, spec_b]),
        patch.object(
            runner, "_get_source_files", side_effect=[[shared_file], [shared_file]]
        ),
    ):
        with pytest.raises(ValueError):
            runner.run_service(config, "dkb", "giro")

    spec_b_run.assert_not_called()


# ── run_provider ──────────────────────────────────────────────────────────────


def test_run_provider_skips_services_with_no_path(tmp_path: Path):
    spec = _spec("dkb", "giro", "giro0")

    # credit has no source path configured
    sources = Sources(dkb=Provider(giro=tmp_path))
    config = _config(tmp_path, sources)

    with (
        patch.object(runner, "ALL_PARSERS", [spec]),
        patch.object(runner, "run_service") as mock_run_service,
    ):
        runner.run_provider(config, "dkb")

    # only "giro" should be dispatched, not tagesgeld/credit/festgeld
    mock_run_service.assert_called_once_with(config, "dkb", "giro")


def test_run_provider_calls_run_service_for_each_enabled_service(tmp_path: Path):
    spec_giro = _spec("dkb", "giro", "giro0")
    spec_credit = _spec("dkb", "credit", "credit0")

    sources = Sources(dkb=Provider(giro=tmp_path, credit=tmp_path))
    config = _config(tmp_path, sources)

    with (
        patch.object(runner, "ALL_PARSERS", [spec_giro, spec_credit]),
        patch.object(runner, "run_service") as mock_run_service,
    ):
        runner.run_provider(config, "dkb")

    assert mock_run_service.call_count == 2
    called_services = {c.args[2] for c in mock_run_service.call_args_list}
    assert called_services == {"giro", "credit"}


# ── run_enabled_services ──────────────────────────────────────────────────────


def test_run_enabled_services_skips_unconfigured_providers(tmp_path: Path):
    sources = Sources(dkb=Provider(giro=tmp_path))  # postbank/scalable/gls are None
    config = _config(tmp_path, sources)

    with patch.object(runner, "run_provider") as mock_run_provider:
        runner.run_enabled_services(config)

    mock_run_provider.assert_called_once_with(config, "dkb")


def test_run_enabled_services_calls_all_configured_providers(tmp_path: Path):
    sources = Sources(
        dkb=Provider(giro=tmp_path),
        postbank=Provider(giro=tmp_path),
    )
    config = _config(tmp_path, sources)

    with patch.object(runner, "run_provider") as mock_run_provider:
        runner.run_enabled_services(config)

    called_providers = {c.args[1] for c in mock_run_provider.call_args_list}
    assert called_providers == {"dkb", "postbank"}


# ── _get_source_files (default vs custom getter) ──────────────────────────────


def test_get_source_files_uses_custom_getter_when_provided(tmp_path: Path):
    custom_getter = MagicMock(return_value=[tmp_path / "file.htm"])
    spec = ParserSpec(
        case=Case(provider="scalable", service="broker", parser="broker0"),
        applies=MagicMock(return_value=True),
        run=MagicMock(),
        source_files_getter=custom_getter,
    )
    sources = Sources(scalable=Provider(broker=tmp_path))
    config = _config(tmp_path, sources)

    result = runner._get_source_files(spec, config)

    custom_getter.assert_called_once_with(spec.case, config, spec.applies)
    assert result == [tmp_path / "file.htm"]


def test_get_source_files_falls_back_to_csv_getter(tmp_path: Path):
    spec = ParserSpec(
        case=Case(provider="dkb", service="giro", parser="giro0"),
        applies=MagicMock(return_value=True),
        run=MagicMock(),
        source_files_getter=None,
    )
    sources = Sources(dkb=Provider(giro=tmp_path))
    config = _config(tmp_path, sources)

    with patch.object(runner, "csv_get_source_files", return_value=[]) as mock_csv:
        runner._get_source_files(spec, config)

    mock_csv.assert_called_once_with(spec.case, config, spec.applies)
