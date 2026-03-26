from pathlib import Path


def normalize_path(path: Path) -> Path:
    if str(path).startswith("~"):
        path = path.expanduser()

    path = path.resolve().absolute()

    return path


def sanity_check_path(path: Path):
    if not isinstance(path, Path):
        msg = f"{path=} is not of type pathlib.Path but {type(path)=}."
        raise ValueError(msg)

    if not path.exists():
        msg = f"Path {path} does not exist"
        raise ValueError(msg)
