"""Generic helper for parser applicability predicates.

Combines a filename guard with a content check to avoid unnecessary file I/O.
"""

import re
from pathlib import Path
from typing import Callable

from fintl.etl.io.files.detect import detect_encoding
from fintl.etl.io.files.utils import load_lines


def check_applies(
    file_path: Path,
    filename_pattern: str,
    content_check: Callable[[list[str]], bool],
) -> bool:
    """Return True if the filename matches and the content check passes.

    Short-circuits immediately when the filename does not match, avoiding any
    file I/O (and the spurious chardet warnings that come with it).

    Args:
        file_path: Candidate file to test.
        filename_pattern: Regex applied to ``file_path.name``.
        content_check: Called with the decoded file lines; should return True
            when the file content is consistent with this parser.

    Returns:
        True only when both the filename pattern and the content check pass.
    """
    if not re.search(filename_pattern, file_path.name):
        return False
    encoding = detect_encoding(file_path)
    lines = load_lines(file_path, encoding)
    return content_check(lines)
