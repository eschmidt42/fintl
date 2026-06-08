"""Testing counts."""

from fintl.common import FileCounts, FileOutcome


def test_counts_and_outcomes_sync():
    """Ensure perfect match between FileOutcome members and FileCounts keys."""
    enum_names = {member.name for member in FileOutcome}
    typed_dict_keys = set(FileCounts.__annotations__.keys())
    assert enum_names == typed_dict_keys
