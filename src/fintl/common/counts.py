"""Functionality to counts things."""

from enum import StrEnum, auto
from typing import TypedDict


class FileCounts(TypedDict):
    """Counts file operations done in `fintl store`."""

    matched: int
    copied: int
    skipped: int
    unmatched: int
    ambiguous: int


class FileOutcome(StrEnum):
    """Outcomes of file operations after prompting the user."""

    matched = auto()
    copied = auto()
    skipped = auto()
    unmatched = auto()
    ambiguous = auto()
