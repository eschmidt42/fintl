"""Path utility functions for normalization and validation."""

from pathlib import Path


def normalize_path(path: Path) -> Path:
    """Expand user home and resolve path to an absolute path."""
    if str(path).startswith("~"):
        path = path.expanduser()

    path = path.resolve().absolute()

    return path


def sanity_check_path(path: Path):
    """Raise ValueError if path is not a Path instance or does not exist."""
    if not isinstance(path, Path):
        msg = f"{path=} is not of type pathlib.Path but {type(path)=}."
        raise ValueError(msg)

    if not path.exists():
        msg = f"Path {path} does not exist"
        raise ValueError(msg)
