# fintl CLI

This module is wired up in `pyproject.toml` as:

```toml
[project.scripts]
fintl = "fintl.cli.main:app"
```

You can run it either from an installed environment:

```bash
fintl --help
```

or directly from the repository with `uv`:

```bash
uv run fintl --help
```

The `etl`, `store`, `search`, and `plot` commands read from the directory configured by `fintl.accounts_etl.schemas.Config`, especially `Config.target_dir`.

## `fintl.toml`

By default, `Config` reads `~/.config/petprojects/fintl.toml`. Only configure the providers you actually use — unused `[sources.*]` sections can be omitted. As a starting point, copy this dummy config and edit the paths for your machine:

```toml
target_dir = "YOURPATH/accounts-data"

[sources.dkb]
giro = "YOURPATH/DKB/giro"
tagesgeld = "YOURPATH/DKB/tagesgeld"
credit = "YOURPATH/DKB/credit"
festgeld = "YOURPATH/DKB/festgeld"

[sources.postbank]
giro = "YOURPATH/Postbank/giro"

[sources.scalable]
broker = "YOURPATH/Scalable/broker-artefacts"

[sources.gls]
giro = "YOURPATH/GLS/giro"
credit = "YOURPATH/GLS/credit"

[logging]
third_party_filter_level = 20
handlers_stdout_level = "INFO"
handlers_file_json_level = "DEBUG"
handlers_file_json_filename = "YOURPATH/fintl-etl.log.jsonl"
handlers_file_json_maxbytes = 10_000_000
handlers_file_json_backup_count = 3
root_level = "DEBUG"

# Optional: enable local ollama for Scalable broker PNG parsing.
# Remove this section (or omit it) to skip PNG parsing entirely.
[ollama]
model = "qwen3.5:27b"
# base_url = "http://localhost:11434/v1"  # default; override if your ollama runs elsewhere
```


## Top-level usage

```bash
fintl [OPTIONS] COMMAND [ARGS]...
```

Available commands:

- `fintl etl` — run the accounts ETL pipeline
- `fintl store` — copy downloaded bank files into the ETL input directories
- `fintl search` — interactive transaction search
- `fintl plot` — open a balances chart in your browser

You can inspect the live CLI help with:

```bash
fintl --help
fintl etl --help
fintl store --help
fintl plot --help
fintl search --help
```


## `fintl etl`

Loads configuration and runs the accounts ETL pipeline.

```bash
fintl etl
```

This command produces the consolidated parquet files used by the other commands, including:

- `all-balances.parquet`
- `all-transactions.parquet`

If you have not run the ETL yet, run this command before using `fintl search` or `fintl plot`.

### Ollama (PNG parsing)

The Scalable broker parser `broker20260309` extracts balance data from PNG screenshots using a local [ollama](https://ollama.com) multimodal model. This is **opt-in**: PNG files are skipped, and a warning is logged, unless an `[ollama]` section is present in `fintl.toml` and [`ollama` locally running](https://docs.ollama.com/quickstart#get-started).

To enable it, add to your `~/.config/petprojects/fintl.toml`:

```toml
[ollama]
model = "qwen3.5:27b"          # any multimodal model available in your ollama instance
# base_url = "http://localhost:11434/v1"  # optional; default shown
```

If ollama is configured but unreachable (not running, model not pulled, wrong URL), the affected PNG files are skipped with a warning and the rest of the ETL continues normally.

Source: `src/fintl/cli/etl.py`


## `fintl store`

Scans a directory for downloaded bank files and copies them into the correct ETL input directories.

```bash
fintl store
```

By default, the current working directory is scanned. To specify a different directory:

```bash
fintl store --from-dir ~/Downloads
```

Each file is tested against all registered parser applicability predicates. For every match you are asked to confirm before the file is copied. To auto-confirm all matches:

```bash
fintl store --yes
```

When a file matches multiple parsers you are prompted to choose one (or skip). With `--yes`, ambiguous files are skipped automatically.

A summary is printed at the end:

```
Done. Files matched: 3 | Copied: 2 | Skipped: 1 | Unmatched: 0 | Ambiguous: 0
```

Source: `src/fintl/cli/store.py`


## `fintl search`

Launches a Textual terminal UI for browsing and filtering bank transactions.

```bash
fintl search
```

The app reads `all-transactions.parquet` from `Config.target_dir`.

Filter fields available in the UI:

| Field | Description |
| --- | --- |
| `source` | Who initiated the transaction |
| `recipient` | Who received it |
| `description` | Free-text transaction description |
| `after date` / `before date` | Date range filter |
| `above amount` / `below amount` | Amount range filter |
| `provider` | Bank provider such as `DKB` |
| `service` | Account type such as `giro` |

Useful interactions:

- filters apply automatically about 1 second after typing stops
- clicking a column header sorts by that column (click again to reverse)
- `Esc` focuses the results table
- `Ctrl+X` clears all filters
- selecting a row opens a detail dialog
- pressing `Enter` in the detail dialog copies the selected value to the clipboard
- `Ctrl+Q` quits

Source: `src/fintl/cli/search.py`


## `fintl plot`

Builds a scatter chart of account balances over time and opens it in your default browser.

```bash
fintl plot
```

To save the chart to a specific HTML file:

```bash
fintl plot --save chart.html
```

The command reads `all-balances.parquet` from `Config.target_dir`.

When `--save` is omitted, the chart is written to a temporary HTML file and opened automatically. When `--save` is provided, the chart is saved to that path and then opened.

Source: `src/fintl/cli/plot.py`


## Extending the ETL

The ETL is organised around three levels:

- **Provider** — a bank or broker (e.g. `dkb`, `scalable`)
- **Service** — an account type at that provider (e.g. `giro`, `broker`)
- **Parser** — a versioned format handler for a specific file layout exported by that service

Each parser module is registered as a `ParserSpec` inside a provider's `plugin.py`. The runner discovers all specs through the central `ALL_PLUGINS` list in `src/fintl/accounts_etl/registry.py` and calls each parser in precedence order.

The ETL is designed so that adding a new parser version, service, or provider
requires changes in as few places as possible.

### Adding a new parser version for an existing service

A "parser version" handles a specific file format for an existing bank
account type, e.g. a new CSV layout that DKB started exporting in 2025.

1. **Create the parser module** in the appropriate provider package, e.g.
   `src/fintl/accounts_etl/dkb/giro202501.py`.  The module must expose:

   - `CASE: Case` — a `Case(provider=..., service=..., parser=...)` instance
     that uniquely names this parser.
   - `check_if_parser_applies(file_path: Path) -> bool` — a predicate that
     returns `True` for exactly the files this version should handle and
     `False` for every file handled by a sibling parser.
   - `main(config: Config) -> None` — runs the full parse-and-store pipeline.

2. **Register the parser** in the provider's plugin module, e.g.
   `src/fintl/accounts_etl/dkb/plugin.py`, by adding a `ParserSpec` to
   the relevant `ServicePlugin`:

   ```python
   from fintl.accounts_etl.dkb import giro202501

   GIRO = ServicePlugin(
       name="giro",
       parsers=(
           ...
           ParserSpec(
               case=giro202501.CASE,
               applies=giro202501.check_if_parser_applies,
               run=giro202501.main,
               precedence=30,   # higher than the existing giro202312 (20)
           ),
       ),
   )
   ```

   The `precedence` value controls execution order within a `(provider,
   service)` group — lower values run first.  Use 0 for generic/fallback
   parsers and 10, 20, 30, … in chronological order for versioned parsers.

That's it.  The runner picks up the new spec automatically, checks for
applicability overlap, and runs the parser in the correct order.

### Adding a new service to an existing provider

A "service" is a new account type at a bank that already has a provider
entry, e.g. adding a `depot` service to Postbank.

1. Add the service field to `Provider` and `ServiceEnum` in
   `src/fintl/accounts_etl/schemas.py`:

   ```python
   class ServiceEnum(str, Enum):
       ...
       depot = "depot"

   class Provider(BaseModel):
       ...
       depot: Path | None = None
   ```

   The `@field_validator` already covers any new `Path` fields listed in its
   decorator arguments — add the new field name there too.

2. Create the parser module(s) following the same steps as above.

3. Add a new `ServicePlugin` to the provider's `plugin.py`, e.g.
   `src/fintl/accounts_etl/postbank/plugin.py`:

   ```python
   DEPOT = ServicePlugin(
       name="depot",
       parsers=(
           ParserSpec(
               case=depot0.CASE,
               applies=depot0.check_if_parser_applies,
               run=depot0.main,
               precedence=0,
           ),
       ),
   )

   PLUGIN = ProviderPlugin(name="postbank", services=(GIRO, DEPOT))
   ```

4. Update `~/.config/petprojects/fintl.toml` with the new service path.

### Adding a new provider (bank)

1. Add the provider to `ProviderEnum` and `Sources` in
   `src/fintl/accounts_etl/schemas.py`:

   ```python
   class ProviderEnum(str, Enum):
       ...
       n26 = "n26"

   class Sources(BaseModel):
       ...
       n26: Provider | None = None
   ```

2. Create a provider sub-package, e.g.
   `src/fintl/accounts_etl/n26/`, with an `__init__.py`, at least one
   parser module, and a `plugin.py` that exports `PLUGIN`:

   ```python
   # src/fintl/accounts_etl/n26/plugin.py
   from fintl.accounts_etl.n26 import giro0
   from fintl.accounts_etl.schemas import ParserSpec, ProviderPlugin, ServicePlugin

   GIRO = ServicePlugin(
       name="giro",
       parsers=(
           ParserSpec(
               case=giro0.CASE,
               applies=giro0.check_if_parser_applies,
               run=giro0.main,
               precedence=0,
           ),
       ),
   )

   PLUGIN = ProviderPlugin(name="n26", services=(GIRO,))
   ```

3. Import and add the plugin to `ALL_PLUGINS` in
   `src/fintl/accounts_etl/registry.py`:

   ```python
   from fintl.accounts_etl.n26.plugin import PLUGIN as N26_PLUGIN

   ALL_PLUGINS: list[ProviderPlugin] = [
       DKB_PLUGIN,
       POSTBANK_PLUGIN,
       SCALABLE_PLUGIN,
       GLS_PLUGIN,
       N26_PLUGIN,
   ]
   ```

4. Add the new provider's source paths to `~/.config/petprojects/fintl.toml`.

The runner iterates `config.sources` dynamically, so no changes to
`src/fintl/accounts_etl/process_accounts.py` or any other orchestration file are required.

### Parsers with non-CSV source files

For parsers whose input files are not CSVs (e.g. HTML or PNG files, as used
by the Scalable broker parsers), pass a custom `source_files_getter` to
`ParserSpec` inside the provider's `plugin.py`:

```python
from fintl.accounts_etl.scalable.files import (
    get_parser_source_files as scalable_get_source_files,
)

BROKER = ServicePlugin(
    name="broker",
    parsers=(
        ParserSpec(
            case=my_module.CASE,
            applies=my_module.check_if_parser_applies,
            run=my_module.main,
            precedence=0,
            source_files_getter=scalable_get_source_files,
        ),
    ),
)
```

The getter must have the signature
`(case: Case, config: Config, applies: Callable[[Path], bool]) -> list[Path]`.
