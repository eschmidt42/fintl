### Commands

# Type check a single file by path
uv run ty check path/to/file.py

# Format a single file by path
uv run ruff format path/to/file.py

# Lint a single file by path
uv run ruff check path/to/file.py

# Run unit tests for a specific file
uv run pytest path/to/file.py

# Full build (only when explicitly needed)
uv run prek run --all-files
