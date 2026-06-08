## Commands

```bash
# Type check a single file by path
uv run ty check path/to/file.py

# Format a single file by path
uv run ruff format path/to/file.py

# Lint a single file by path
uv run ruff check path/to/file.py --fix

# Run unit tests for a specific file
uv run pytest path/to/file.py

# Test coverage (only when explicitly needed)
uv run pytest tests --cov --cov-branch --cov-report=xml

# Full build (only when explicitly needed)
uv run prek run --all-files
```

## Architecture

**fintl** is a local-only financial ETL CLI. It parses bank statement exports (CSV, HTML, PNG) from multiple providers, normalises them into Polars DataFrames, and stores the results as Parquet files for analysis.

### CLI commands (`src/fintl/cli/commands/`)

| Command | File | Purpose |
|---------|------|---------|
| `fintl store` | `commands/store/` | Discovers files in Downloads and copies them to the correct provider source directories |
| `fintl etl` | `commands/etl.py` | Runs the full ETL pipeline; produces `all-transactions.parquet`, `all-balances.parquet`, and labelled variants |
| `fintl plot` | `commands/plot/` | Renders an Altair HTML chart of account balances over time |
| `fintl search` | `commands/search/` | Launches an interactive Textual TUI for filtering/searching transactions |

User config lives at `~/.config/petprojects/fintl.toml` (Pydantic-Settings backed; schema in `src/fintl/common/config.py`).

### Plugin/parser architecture (`src/fintl/etl/providers/`)

Providers (DKB, Postbank, GLS, Scalable) each expose a `ProviderPlugin` containing `ServicePlugin` instances (giro, tagesgeld, credit, festgeld, broker, etc.). Each service holds a list of `ParserSpec` objects that encode:

- `applies(path) -> bool` — claims ownership of a source file
- `run(config)` — executes the parse and returns a normalised DataFrame
- `precedence: int` — run order within a service (lower = first); enables versioned parsers for different file format generations
- `source_files_getter` — optional override for file discovery (used by Scalable for HTML/PNG inputs)

All plugins are registered centrally in `src/fintl/etl/engine/registry.py` (`ALL_PLUGINS`, `ALL_PARSERS`). The generic runner in `runner.py` iterates `ALL_PLUGINS`, checks overlap (no file may be claimed by two parsers), and calls each applicable `ParserSpec.run()`.

### Data flow

```
~/.config/petprojects/fintl.toml
        │
        ▼
runner.py → ProviderPlugin → ServicePlugin → ParserSpec.run()
        │
        ▼
Polars DataFrames  →  process_accounts.py  →  labels.py
        │
        ▼
{target_dir}/{provider}/{service}/{parser}/parsed/
all-transactions.parquet, all-balances.parquet, *.xlsx
```

### Key conventions

- **Parquet + Polars** throughout; Excel exports are a secondary artefact.
- **Structured logging**: `src/fintl/common/logging.py` writes JSON lines to `fintl-etl.log.jsonl`; a `WarningBufferHandler` collects warnings and displays a Rich panel summary at the end of each `fintl etl` run.
- **PNG parsing** is optional and requires a locally running Ollama instance with a multimodal model; the runner skips it gracefully when not configured.
- **Snapshot tests** use the `inline-snapshot` library.

## Adding a new parser

Each parser lives in `src/fintl/etl/providers/<provider>/<parserN>.py` and exposes three things:

1. `CASE = Case(provider=..., service=..., parser=...)` — logical identity
2. `check_if_parser_applies(path: Path) -> bool` — file ownership predicate
3. `main(config: Config) -> None` — full ETL for that parser

Wire it into `src/fintl/etl/providers/<provider>/plugin.py` as a `ParserSpec(case=CASE, applies=check_if_parser_applies, run=main, precedence=N)`. Lower `precedence` runs first within a service; the overlap check in `runner.py` raises if two parsers claim the same file.

For a brand-new provider: create `plugin.py` exporting `PLUGIN: ProviderPlugin`, then add it to `ALL_PLUGINS` in `src/fintl/etl/engine/registry.py`.

## Writing tests for parsers

- Fixture files go in `tests/files/csv_files/<Provider>/` (CSV) or `tests/files/artefacts/<Provider>/` (HTML/PNG)
- Build a `Config` with `make_config(tmp_path, sources, logger_config_path)` (defined in `tests/cli/conftest.py`); the `logger_config_path` fixture is in `tests/conftest.py`
- Assert parsed output with `from inline_snapshot import snapshot`; regenerate with `--inline-snapshot=update`
- Ollama-dependent parsers (e.g. `broker20260309`) need `OllamaConfig` set in the `Config`; guard those tests with `@pytest.mark.ollama` and run them explicitly with `pytest -m ollama`
