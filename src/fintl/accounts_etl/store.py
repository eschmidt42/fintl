"""Business logic for the ``fintl store`` command.

Scans a source directory for downloaded bank files (CSV, HTM, HTML, PNG),
matches each candidate against all registered parser applicability predicates,
and copies confirmed files into the appropriate ETL raw input directory.
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
) -> dict[str, int]:
    """Scan *source_dir*, match files to parsers, and copy on confirmation.

    For each candidate file every matching parser spec is presented to the
    caller via *confirm*.  The caller decides (interactively or otherwise)
    whether to copy the file to the target raw directory.

    Args:
        source_dir: Directory to scan for candidate files.
        config: Loaded ETL configuration (supplies target paths).
        parsers: All registered ``ParserSpec`` instances.
        confirm: Callable that receives a human-readable prompt string and
            returns ``True`` when the file should be copied.

    Returns:
        Summary counts: ``{"matched": int, "copied": int, "skipped": int,
        "unmatched": int}``.
    """
    candidates = find_candidate_files(source_dir)
    logger.info("Scanning %d candidate file(s) in %s", len(candidates), source_dir)

    counts = {"matched": 0, "copied": 0, "skipped": 0, "unmatched": 0}

    for file in candidates:
        matches = match_file_to_parsers(file, parsers)

        if not matches:
            counts["unmatched"] += 1
            logger.debug("No parser matched %s", file.name)
            continue

        counts["matched"] += 1

        for spec in matches:
            raw_dir = config.get_raw_dir(spec.case)
            prompt = (
                f"{file.name}  →  {spec.case.provider} / {spec.case.service} / {spec.case.parser}\n"
                f"    target: {raw_dir}"
            )
            if confirm(prompt):
                copied = _copy_file(file, raw_dir)
                if copied:
                    counts["copied"] += 1
                else:
                    counts["skipped"] += 1
            else:
                counts["skipped"] += 1

    return counts
