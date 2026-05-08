from pathlib import Path


def balance_csv_name_to_parquet(file: Path) -> str:
    """Converts a balance CSV file name to the corresponding Parquet name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding Parquet file.
    """
    return file.name.replace(".csv", "-balance.parquet")


def balance_htm_name_to_parquet(file: Path) -> str:
    """Converts a balance HTM/HTML/PNG file name to the corresponding Parquet name.

    Args:
        file: The input HTM/HTML/PNG file path.

    Returns:
        The name of the corresponding Parquet file.
    """
    return file.name.replace(file.suffix, "-balance.parquet")


def balance_name_to_parquet(file: Path) -> str:
    """Determines the correct Parquet output name for a balance file.

    Dispatches to the appropriate converter based on the file's suffix.

    Args:
        file: The input balance file path.

    Returns:
        The name of the corresponding Parquet balance file.

    Raises:
        ValueError: If the file suffix is unsupported.
    """
    if file.name.endswith("csv"):
        return balance_csv_name_to_parquet(file)
    elif (
        file.name.endswith("htm")
        or file.name.endswith("html")
        or file.name.endswith("png")
    ):
        return balance_htm_name_to_parquet(file)
    else:
        raise ValueError(f"Unexpected suffix of {file=}")


def transaction_csv_name_to_parquet(file: Path) -> str:
    """Converts a transaction CSV file name to the corresponding Parquet name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding Parquet file (e.g., 'export.csv' -> 'export-transactions.parquet').
    """
    return file.name.replace(".csv", "-transactions.parquet")


def transaction_htm_name_to_parquet(file: Path) -> str:
    """Converts a transaction HTM/HTML/PNG file name to the corresponding Parquet name.

    Args:
        file: The input HTM/HTML/PNG file path.

    Returns:
        The name of the corresponding Parquet file.
    """
    return file.name.replace(file.suffix, "-transactions.parquet")


def transaction_name_to_parquet(file: Path) -> str:
    """Determines the correct Parquet output name for a transaction file.

    Dispatches to the appropriate converter based on the file's suffix.

    Args:
        file: The input transaction file path.

    Returns:
        The name of the corresponding Parquet transaction file.

    Raises:
        ValueError: If the file suffix is unsupported.
    """
    if file.name.endswith("csv"):
        return transaction_csv_name_to_parquet(file)
    elif (
        file.name.endswith("htm")
        or file.name.endswith("html")
        or file.name.endswith("png")
    ):
        return transaction_htm_name_to_parquet(file)
    else:
        raise ValueError(f"Unexpected suffix of {file=}")


def balance_csv_name_to_json(file: Path) -> str:
    """Converts a balance CSV file name to the corresponding JSON name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding JSON file.
    """
    return file.name.replace(".csv", "-balance.json")


def transaction_csv_name_to_xlsx(file: Path) -> str:
    """Converts a transaction CSV file name to the corresponding XLSX name.

    Args:
        file: The input CSV file path.

    Returns:
        The name of the corresponding XLSX file (e.g., 'export.csv' -> 'export-transactions.xlsx').
    """
    return file.name.replace(".csv", "-transactions.xlsx")


def transaction_htm_name_to_xlsx(file: Path) -> str:
    """Converts a transaction HTM/HTML/PNG file name to the corresponding XLSX name.

    Args:
        file: The input HTM/HTML/PNG file path.

    Returns:
        The name of the corresponding XLSX file.
    """
    return file.name.replace(file.suffix, "-transactions.xlsx")


def balance_htm_name_to_json(file: Path) -> str:
    """Converts a balance HTM/HTML/PNG file name to the corresponding JSON name.

    Args:
        file: The input HTM/HTML/PNG file path.

    Returns:
        The name of the corresponding JSON file.
    """
    return file.name.replace(file.suffix, "-balance.json")
