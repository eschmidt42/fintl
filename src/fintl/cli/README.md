# `cli`

CLI entry point for the `money` command.

This module is wired up in `pyproject.toml` as:

```toml
[project.scripts]
money = "cli.main:app"
```

You can run it either from an installed environment:

```bash
money --help
```

or directly from the repository with `uv`:

```bash
uv run money --help
```

The `etl`, `search`, and `plot` commands read from the directory configured by `packages.accounts_etl.schemas.Config`, especially `Config.target_dir`.

By default, `Config` reads `~/.config/petprojects/money.toml`. As a starting point, copy this dummy config and edit the paths for your machine:

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
handlers_file_json_filename = "YOURPATH/money-etl.log.jsonl"
handlers_file_json_maxbytes = 10_000_000
handlers_file_json_backup_count = 3
root_level = "DEBUG"
```


## Top-level usage

```bash
money [OPTIONS] COMMAND [ARGS]...
```

Available commands:

- `money search` — interactive transaction search
- `money etl run` — run the accounts ETL pipeline
- `money plot run` — open a balances chart in your browser

You can inspect the live CLI help with:

```bash
money --help
money etl --help
money plot --help
money search --help
```


## `money etl run`

Loads configuration and runs the accounts ETL pipeline.

```bash
money etl run
```

This command produces the consolidated parquet files used by the other commands, including:

- `all-balances.parquet`
- `all-transactions.parquet`

If you have not run the ETL yet, run this command before using `money search` or `money plot run`.

Source: `apps/accounts/etl.py`


## `money search`

Launches a Textual terminal UI for browsing and filtering bank transactions.

```bash
money search
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
- `Esc` focuses the results table
- `Ctrl+X` clears all filters
- selecting a row opens a detail dialog
- pressing `Enter` in the detail dialog copies the selected value
- `Ctrl+Q` quits

Source: `apps/accounts/search.py`


## `money plot run`

Builds a scatter chart of account balances over time and opens it in your default browser.

```bash
money plot run
```

To save the chart to a specific HTML file first:

```bash
money plot run --save chart.html
```

The command reads `all-balances.parquet` from `Config.target_dir`.

When `--save` is omitted, the chart is written to a temporary HTML file and opened automatically. When `--save` is provided, the chart is saved to that path and then opened.

Source: `apps/accounts/plot_cli.py`


## Extending the ETL

The ETL is designed so that adding a new parser version, service, or provider
requires changes in as few places as possible.

### Adding a new parser version for an existing service

A "parser version" handles a specific file format for an existing bank
account type, e.g. a new CSV layout that DKB started exporting in 2025.

1. **Create the parser module** in the appropriate provider package, e.g.
   `src/packages/accounts_etl/dkb/giro202501.py`.  The module must expose:

   - `CASE: Case` — a `Case(provider=..., service=..., parser=...)` instance
     that uniquely names this parser.
   - `check_if_parser_applies(file_path: Path) -> bool` — a predicate that
     returns `True` for exactly the files this version should handle and
     `False` for every file handled by a sibling parser.
   - `main(config: Config) -> None` — runs the full parse-and-store pipeline.

2. **Register the parser** in the provider's plugin module, e.g.
   `src/packages/accounts_etl/dkb/plugin.py`, by adding a `ParserSpec` to
   the relevant `ServicePlugin`:

   ```python
   from packages.accounts_etl.dkb import giro202501

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
   `src/packages/accounts_etl/schemas.py`:

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
   `src/packages/accounts_etl/postbank/plugin.py`:

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

4. Update `~/.config/petprojects/money.toml` with the new service path.

### Adding a new provider (bank)

1. Add the provider to `ProviderEnum` and `Sources` in
   `src/packages/accounts_etl/schemas.py`:

   ```python
   class ProviderEnum(str, Enum):
       ...
       n26 = "n26"

   class Sources(BaseModel):
       ...
       n26: Provider | None = None
   ```

2. Create a provider sub-package, e.g.
   `src/packages/accounts_etl/n26/`, with an `__init__.py`, at least one
   parser module, and a `plugin.py` that exports `PLUGIN`:

   ```python
   # src/packages/accounts_etl/n26/plugin.py
   from packages.accounts_etl.n26 import giro0
   from packages.accounts_etl.schemas import ParserSpec, ProviderPlugin, ServicePlugin

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
   `src/packages/accounts_etl/registry.py`:

   ```python
   from packages.accounts_etl.n26.plugin import PLUGIN as N26_PLUGIN

   ALL_PLUGINS: list[ProviderPlugin] = [
       DKB_PLUGIN,
       POSTBANK_PLUGIN,
       SCALABLE_PLUGIN,
       GLS_PLUGIN,
       N26_PLUGIN,
   ]
   ```

4. Add the new provider's source paths to `~/.config/petprojects/money.toml`.

The runner iterates `config.sources` dynamically, so no changes to
`process_accounts.py` or any other orchestration file are required.

### Parsers with non-CSV source files

For parsers whose input files are not CSVs (e.g. HTML or PNG files, as used
by the Scalable broker parsers), pass a custom `source_files_getter` to
`ParserSpec` inside the provider's `plugin.py`:

```python
from packages.accounts_etl.scalable.files import (
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
