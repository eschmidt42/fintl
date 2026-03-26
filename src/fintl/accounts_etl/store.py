"""Business logic for the ``fintl store`` command.

Scans a source directory for downloaded bank files (CSV, HTM, HTML, PNG),
matches each candidate against all registered parser applicability predicates,
and copies confirmed files into the appropriate ETL raw input directory.

A file that matches exactly one parser is confirmed via the *confirm* callback.
A file that matches two or more parsers is treated as **ambiguous**: the caller
must resolve the ambiguity via the *choose* callback (returning the desired
``ParserSpec``, or ``None`` to skip).  Ambiguous files are counted separately so
callers can surface the issue without silently duplicating data across parsers.
"""

import logging
import shutil
from pathlib import Path
from typing import Callable

from fintl.accounts_etl.schemas import Config, ParserSpec

logger = logging.getLogger(__name__)

_CANDIDATE_PATTERNS = ("*.csv", "*.htm", "*.html", "*.png")


def find_candidate_files(source_dir: Path) -> list[Path]:
    """Return all files in *source_dir* that could be bank export files.

    Searches for CSV, HTM, HTML, and PNG files non-recursively.

    Args:
        source_dir: Directory to scan.

    Returns:
        Sorted list of matching file paths.
    """
    found: list[Path] = []
    for pattern in _CANDIDATE_PATTERNS:
        found.extend(source_dir.glob(pattern))
    found = sorted(set(found))
    logger.debug("Found %d candidate file(s) in %s", len(found), source_dir)
    return found


def match_file_to_parsers(file: Path, parsers: list[ParserSpec]) -> list[ParserSpec]:
    """Return all parser specs whose applicability predicate accepts *file*.

    Args:
        file: Candidate file path.
        parsers: All registered ``ParserSpec`` instances to test.

    Returns:
        List of matching specs (may be empty or contain more than one).
    """
    matches: list[ParserSpec] = []
    for spec in parsers:
        try:
            if spec.applies(file):
                matches.append(spec)
        except Exception:
            logger.debug(
                "Parser %s raised while checking %s — treating as non-match.",
                spec.case.name,
                file,
                exc_info=True,
            )
    return matches


def _copy_file(file: Path, raw_dir: Path) -> bool:
    """Copy *file* into *raw_dir*, skipping if already present.

    Args:
        file: Source file to copy.
        raw_dir: Destination directory (created if absent).

    Returns:
        ``True`` if the file was copied, ``False`` if it was already present.
    """
    dest = raw_dir / file.name
    if dest.exists():
        logger.info("Already present, skipping: %s", dest)
        return False
    raw_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file, dest)
    logger.info("Copied %s → %s", file, dest)
    return True


def store_files(
    source_dir: Path,
    config: Config,
    parsers: list[ParserSpec],
    *,
    confirm: Callable[[str], bool],
    choose: Callable[[Path, list[ParserSpec]], ParserSpec | None],
) -> dict[str, int]:
    """Scan *source_dir*, match files to parsers, and copy on confirmation.

    Files that match **exactly one** parser are presented to the caller via
    *confirm* before being copied.  Files that match **two or more** parsers are
    treated as ambiguous: *choose* is called so the caller can select the single
    correct parser (or return ``None`` to skip the file entirely).  Copying the
    same source file into multiple parser raw directories is intentionally
    prevented to avoid duplicate parsing runs.

    Args:
        source_dir: Directory to scan for candidate files.
        config: Loaded ETL configuration (supplies target paths).
        parsers: All registered ``ParserSpec`` instances.
        confirm: Callable that receives a human-readable prompt string and
            returns ``True`` when the file should be copied (single-match path).
        choose: Callable that receives the candidate ``Path`` and the list of
            matching ``ParserSpec`` instances and returns the one spec the file
            should be routed to, or ``None`` to skip the file (multi-match path).

    Returns:
        Summary counts: ``{"matched": int, "copied": int, "skipped": int,
        "unmatched": int, "ambiguous": int}``.
        *matched* counts files with exactly one parser match.
        *ambiguous* counts files that matched two or more parsers.
    """
    candidates = find_candidate_files(source_dir)
    logger.info("Scanning %d candidate file(s) in %s", len(candidates), source_dir)

    counts = {"matched": 0, "copied": 0, "skipped": 0, "unmatched": 0, "ambiguous": 0}

    for file in candidates:
        matches = match_file_to_parsers(file, parsers)

        if not matches:
            counts["unmatched"] += 1
            logger.debug("No parser matched %s", file.name)
            continue

        if len(matches) > 1:
            counts["ambiguous"] += 1
            logger.warning(
                "%s matched %d parsers (%s) — ambiguous; requesting user selection.",
                file.name,
                len(matches),
                ", ".join(s.case.name for s in matches),
            )
            chosen = choose(file, matches)
            if chosen is None:
                logger.debug("Ambiguous file skipped by user: %s", file.name)
                continue
            if _copy_file(file, config.get_raw_dir(chosen.case)):
                counts["copied"] += 1
            else:
                counts["skipped"] += 1
            continue

        counts["matched"] += 1
        spec = matches[0]
        raw_dir = config.get_raw_dir(spec.case)
        prompt = (
            f"{file.name}  →  {spec.case.provider} / {spec.case.service} / {spec.case.parser}\n"
            f"    target: {raw_dir}"
        )
        if confirm(prompt):
            if _copy_file(file, raw_dir):
                counts["copied"] += 1
            else:
                counts["skipped"] += 1
        else:
            counts["skipped"] += 1

    return counts
