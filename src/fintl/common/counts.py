"""Functionality to counts things."""

from typing import TypedDict


class FileCounts(TypedDict):
    """Counts file operations done in `fintl store`."""

    matched: int
    copied: int
    skipped: int
    unmatched: int
    ambiguous: int
