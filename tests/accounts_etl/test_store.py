"""Unit tests for fintl.accounts_etl.store.

Tests use lightweight fake ``ParserSpec`` instances and a temporary filesystem;
no real parser execution or network access is required.
"""

from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock

from fintl.accounts_etl.schemas import (
    Case,
    Config,
    Logging,
    ParserSpec,
    Provider,
    Sources,
)
from fintl.accounts_etl.store import (
    _copy_file,
    deduplicate_by_provider_service,
    find_candidate_files,
    match_file_to_parsers,
    store_files,
)

_NO_CHOOSE: Callable[[Path, list[ParserSpec]], ParserSpec | None] = lambda _f, _s: None

# ── Helpers ───────────────────────────────────────────────────────────────────

_LOGGER_PATH = Path(__file__).parent.parent / "fine_logging" / "logger-config.json"


def _config(tmp_path: Path) -> Config:
    src_dir = tmp_path / "src" / "dkb" / "giro"
    src_dir.mkdir(parents=True, exist_ok=True)
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True, exist_ok=True)
    return Config(
        target_dir=target_dir,
        sources=Sources(dkb=Provider(giro=src_dir)),
        logging=Logging(config_file=_LOGGER_PATH),
    )


def _spec(
    provider: str,
    service: str,
    parser: str,
    applies_result: bool = True,
    precedence: int = 0,
) -> ParserSpec:
    return ParserSpec(
        case=Case(provider=provider, service=service, parser=parser),
        applies=MagicMock(return_value=applies_result),
        run=MagicMock(),
        precedence=precedence,
    )


def _config_two_services(tmp_path: Path) -> Config:
    """Config with dkb/giro and dkb/credit source dirs, for ambiguity tests."""
    giro_dir = tmp_path / "src" / "dkb" / "giro"
    credit_dir = tmp_path / "src" / "dkb" / "credit"
    giro_dir.mkdir(parents=True, exist_ok=True)
    credit_dir.mkdir(parents=True, exist_ok=True)
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True, exist_ok=True)
    return Config(
        target_dir=target_dir,
        sources=Sources(dkb=Provider(giro=giro_dir, credit=credit_dir)),
        logging=Logging(config_file=_LOGGER_PATH),
    )


# ── find_candidate_files ──────────────────────────────────────────────────────


def test_find_candidate_files_returns_expected_extensions(tmp_path: Path):
    (tmp_path / "a.csv").touch()
    (tmp_path / "b.html").touch()
    (tmp_path / "c.htm").touch()
    (tmp_path / "d.png").touch()
    (tmp_path / "e.txt").touch()  # should be ignored
    (tmp_path / "f.pdf").touch()  # should be ignored

    result = find_candidate_files(tmp_path)
    names = {f.name for f in result}
    assert names == {"a.csv", "b.html", "c.htm", "d.png"}


def test_find_candidate_files_empty_dir(tmp_path: Path):
    assert find_candidate_files(tmp_path) == []


def test_find_candidate_files_returns_sorted(tmp_path: Path):
    for name in ("z.csv", "a.png", "m.htm"):
        (tmp_path / name).touch()
    result = find_candidate_files(tmp_path)
    assert result == sorted(result)


# ── match_file_to_parsers ─────────────────────────────────────────────────────


def test_match_file_to_parsers_single_match(tmp_path: Path):
    f = tmp_path / "file.csv"
    f.touch()
    parsers = [_spec("dkb", "giro", "giro0", applies_result=True)]
    matches = match_file_to_parsers(f, parsers)
    assert len(matches) == 1
    assert matches[0].case.parser == "giro0"


def test_match_file_to_parsers_no_match(tmp_path: Path):
    f = tmp_path / "file.csv"
    f.touch()
    parsers = [_spec("dkb", "giro", "giro0", applies_result=False)]
    assert match_file_to_parsers(f, parsers) == []


def test_match_file_to_parsers_multiple_matches(tmp_path: Path):
    f = tmp_path / "file.csv"
    f.touch()
    parsers = [
        _spec("dkb", "giro", "giro0", applies_result=True),
        _spec("dkb", "giro", "giro202312", applies_result=True),
        _spec("postbank", "giro", "giro0", applies_result=False),
    ]
    matches = match_file_to_parsers(f, parsers)
    assert len(matches) == 2


def test_match_file_to_parsers_swallows_exceptions(tmp_path: Path):
    f = tmp_path / "file.csv"
    f.touch()

    def _raises(_: Path) -> bool:
        raise RuntimeError("boom")

    raising_spec = ParserSpec(
        case=Case(provider="dkb", service="giro", parser="giro0"),
        applies=_raises,
        run=MagicMock(),
    )
    ok_spec = _spec("dkb", "giro", "giro202312", applies_result=True)
    matches = match_file_to_parsers(f, [raising_spec, ok_spec])
    assert len(matches) == 1
    assert matches[0].case.parser == "giro202312"


# ── deduplicate_by_provider_service ──────────────────────────────────────────


def test_deduplicate_empty_list():
    assert deduplicate_by_provider_service([]) == []


def test_deduplicate_single_spec():
    spec = _spec("dkb", "giro", "giro0")
    assert deduplicate_by_provider_service([spec]) == [spec]


def test_deduplicate_same_provider_service_keeps_lower_precedence():
    low = _spec("dkb", "giro", "giro0", precedence=0)
    high = _spec("dkb", "giro", "giro202312", precedence=1)
    assert deduplicate_by_provider_service([low, high]) == [low]
    assert deduplicate_by_provider_service([high, low]) == [low]


def test_deduplicate_same_precedence_keeps_first():
    first = _spec("dkb", "giro", "giro0", precedence=0)
    second = _spec("dkb", "giro", "giro202312", precedence=0)
    assert deduplicate_by_provider_service([first, second]) == [first]


def test_deduplicate_different_provider_service_keeps_both():
    spec_a = _spec("dkb", "giro", "giro0")
    spec_b = _spec("dkb", "credit", "credit0")
    result = deduplicate_by_provider_service([spec_a, spec_b])
    assert result == [spec_a, spec_b]


def test_deduplicate_preserves_insertion_order():
    spec_a = _spec("dkb", "giro", "giro0")
    spec_b = _spec("dkb", "credit", "credit0")
    spec_c = _spec("postbank", "giro", "giro0")
    result = deduplicate_by_provider_service([spec_a, spec_b, spec_c])
    assert result == [spec_a, spec_b, spec_c]


# ── _copy_file ────────────────────────────────────────────────────────────────


def test_copy_file_copies_to_raw_dir(tmp_path: Path):
    src = tmp_path / "src" / "file.csv"
    src.parent.mkdir()
    src.write_text("data")
    raw_dir = tmp_path / "raw"

    copied = _copy_file(src, raw_dir)

    assert copied is True
    assert (raw_dir / "file.csv").read_text() == "data"


def test_copy_file_skips_existing(tmp_path: Path):
    src = tmp_path / "file.csv"
    src.write_text("new")
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "file.csv").write_text("old")

    copied = _copy_file(src, raw_dir)

    assert copied is False
    assert (raw_dir / "file.csv").read_text() == "old"


def test_copy_file_creates_raw_dir(tmp_path: Path):
    src = tmp_path / "file.csv"
    src.touch()
    raw_dir = tmp_path / "deep" / "nested" / "raw"

    _copy_file(src, raw_dir)

    assert raw_dir.exists()
    assert (raw_dir / "file.csv").exists()


# ── store_files ───────────────────────────────────────────────────────────────


def test_store_files_copies_confirmed(tmp_path: Path):
    (tmp_path / "downloads").mkdir()
    src_file = tmp_path / "downloads" / "export.csv"
    src_file.write_text("data")

    config = _config(tmp_path)
    spec = _spec("dkb", "giro", "giro202312", applies_result=True)

    counts = store_files(
        tmp_path / "downloads",
        config,
        [spec],
        confirm=lambda _: True,
        choose=_NO_CHOOSE,
    )

    source_dir = config.get_source_dir_from_case(spec.case)
    assert (source_dir / "export.csv").exists()
    assert counts["copied"] == 1
    assert counts["skipped"] == 0
    assert counts["unmatched"] == 0


def test_store_files_skips_on_rejection(tmp_path: Path):
    (tmp_path / "downloads").mkdir()
    src_file = tmp_path / "downloads" / "export.csv"
    src_file.write_text("data")

    config = _config(tmp_path)
    spec = _spec("dkb", "giro", "giro202312", applies_result=True)

    counts = store_files(
        tmp_path / "downloads",
        config,
        [spec],
        confirm=lambda _: False,
        choose=_NO_CHOOSE,
    )

    source_dir = config.get_source_dir_from_case(spec.case)
    assert not (source_dir / "export.csv").exists()
    assert counts["copied"] == 0
    assert counts["skipped"] == 1


def test_store_files_counts_unmatched(tmp_path: Path):
    (tmp_path / "downloads").mkdir()
    (tmp_path / "downloads" / "unknown.csv").write_text("?")

    config = _config(tmp_path)
    spec = _spec("dkb", "giro", "giro202312", applies_result=False)

    counts = store_files(
        tmp_path / "downloads",
        config,
        [spec],
        confirm=lambda _: True,
        choose=_NO_CHOOSE,
    )

    assert counts["unmatched"] == 1
    assert counts["copied"] == 0


def test_store_files_no_candidates(tmp_path: Path):
    source_dir = tmp_path / "empty"
    source_dir.mkdir()
    config = _config(tmp_path)

    counts = store_files(
        source_dir, config, [], confirm=lambda _: True, choose=_NO_CHOOSE
    )

    assert counts == {
        "matched": 0,
        "copied": 0,
        "skipped": 0,
        "unmatched": 0,
        "ambiguous": 0,
    }


# ── store_files – ambiguous (multi-match) path ────────────────────────────────


def test_store_files_ambiguous_counts_and_skips_when_choose_returns_none(
    tmp_path: Path,
):
    """A file matching multiple parsers is counted as ambiguous, not matched.
    When choose returns None the file is not copied."""
    (tmp_path / "downloads").mkdir()
    src_file = tmp_path / "downloads" / "export.csv"
    src_file.write_text("data")

    config = _config_two_services(tmp_path)
    spec_a = _spec("dkb", "giro", "giro0", applies_result=True)
    spec_b = _spec("dkb", "credit", "credit0", applies_result=True)

    choose_calls: list[tuple[Path, list[ParserSpec]]] = []

    def choose(file: Path, specs: list[ParserSpec]) -> ParserSpec | None:
        choose_calls.append((file, specs))
        return None

    counts = store_files(
        tmp_path / "downloads",
        config,
        [spec_a, spec_b],
        confirm=lambda _: True,
        choose=choose,
    )

    assert counts["ambiguous"] == 1
    assert counts["matched"] == 0
    assert counts["copied"] == 0
    assert counts["skipped"] == 0
    assert len(choose_calls) == 1
    assert choose_calls[0][1] == [spec_a, spec_b]
    assert not (config.get_source_dir_from_case(spec_a.case) / "export.csv").exists()
    assert not (config.get_source_dir_from_case(spec_b.case) / "export.csv").exists()


def test_store_files_ambiguous_choose_copies_selected_spec_only(tmp_path: Path):
    """When choose returns one spec, the file is copied only to that parser's source dir."""
    (tmp_path / "downloads").mkdir()
    src_file = tmp_path / "downloads" / "export.csv"
    src_file.write_text("data")

    config = _config_two_services(tmp_path)
    spec_a = _spec("dkb", "giro", "giro0", applies_result=True)
    spec_b = _spec("dkb", "credit", "credit0", applies_result=True)

    counts = store_files(
        tmp_path / "downloads",
        config,
        [spec_a, spec_b],
        confirm=lambda _: True,
        choose=lambda _f, specs: specs[1],  # always pick second
    )

    assert counts["ambiguous"] == 1
    assert counts["copied"] == 1
    assert counts["matched"] == 0
    assert (config.get_source_dir_from_case(spec_b.case) / "export.csv").exists()
    assert not (config.get_source_dir_from_case(spec_a.case) / "export.csv").exists()


def test_store_files_confirm_not_called_for_ambiguous_files(tmp_path: Path):
    """confirm must never be invoked for a multi-match file; only choose is called."""
    (tmp_path / "downloads").mkdir()
    (tmp_path / "downloads" / "export.csv").write_text("data")

    config = _config_two_services(tmp_path)
    spec_a = _spec("dkb", "giro", "giro0", applies_result=True)
    spec_b = _spec("dkb", "credit", "credit0", applies_result=True)

    confirm_calls: list[str] = []

    store_files(
        tmp_path / "downloads",
        config,
        [spec_a, spec_b],
        confirm=lambda p: confirm_calls.append(p) or True,  # type: ignore[func-returns-value]
        choose=_NO_CHOOSE,
    )

    assert confirm_calls == [], "confirm should not be called for ambiguous files"


def test_store_files_ambiguous_choose_skips_when_copy_already_exists(tmp_path: Path):
    """Ambiguous path: when choose returns a spec but _copy_file returns False
    (file already at destination), skipped count is incremented."""
    (tmp_path / "downloads").mkdir()
    src_file = tmp_path / "downloads" / "export.csv"
    src_file.write_text("data")

    config = _config_two_services(tmp_path)
    spec_a = _spec("dkb", "giro", "giro0", applies_result=True)
    spec_b = _spec("dkb", "credit", "credit0", applies_result=True)

    # Pre-place the file at the chosen spec's source dir so _copy_file returns False.
    chosen_source = config.get_source_dir_from_case(spec_b.case)
    chosen_source.mkdir(parents=True, exist_ok=True)
    (chosen_source / "export.csv").write_text("existing")

    counts = store_files(
        tmp_path / "downloads",
        config,
        [spec_a, spec_b],
        confirm=lambda _: True,
        choose=lambda _f, specs: specs[1],
    )

    assert counts["skipped"] == 1
    assert counts["copied"] == 0
    assert (chosen_source / "export.csv").read_text() == "existing"


def test_store_files_single_match_skips_when_copy_already_exists(tmp_path: Path):
    """Single-match path: when confirm returns True but _copy_file returns False
    (file already at destination), skipped count is incremented."""
    (tmp_path / "downloads").mkdir()
    src_file = tmp_path / "downloads" / "export.csv"
    src_file.write_text("data")

    config = _config(tmp_path)
    spec = _spec("dkb", "giro", "giro202312", applies_result=True)

    # Pre-place the file in the parser's source directory so _copy_file returns False.
    source_dir = config.get_source_dir_from_case(spec.case)
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "export.csv").write_text("existing")

    counts = store_files(
        tmp_path / "downloads",
        config,
        [spec],
        confirm=lambda _: True,
        choose=_NO_CHOOSE,
    )

    assert counts["skipped"] == 1
    assert counts["copied"] == 0
    assert (source_dir / "export.csv").read_text() == "existing"
