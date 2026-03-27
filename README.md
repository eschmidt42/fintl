# `fintl`

[![CI](https://github.com/eschmidt42/fintl/actions/workflows/ci.yml/badge.svg)](https://github.com/eschmidt42/fintl/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/eschmidt42/fintl/branch/main/graph/badge.svg?token=FM1L1A7BQ8)](https://codecov.io/gh/eschmidt42/fintl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![ty](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ty/main/assets/badge/v0.json)](https://github.com/astral-sh/ty)

> Financial ETL CLI: parse, normalize, and explore your bank transaction data.

## TL;DR

This tool helps you process, visualize and search your balance and transaction information that you have exported from your bank accounts.

Currently supports DKB, Postbank, GLS and Scalable Capital Broker.

Supported file formats: CSV, HTML, and PNG. PNG parsing uses a local [ollama](https://ollama.com) instance with a multimodal model — opt-in via `fintl.toml` (required only for Scalable broker PNG statements; gracefully skipped when not configured).

**All your data stays on your machine. No need to trust another entity that is PSD2 certified.**

## How to install

```bash
git clone https://github.com/eschmidt42/fintl.git
cd fintl
uv sync
uv tool install .
```

After installation, `fintl` should be available on your `PATH`:

```bash
which fintl
# e.g. /Users/YOURUSER/.local/bin/fintl
```

## How to use

1. Configure your `~/.config/petprojects/fintl.toml`. For details see [here](./src/fintl/cli/README.md#fintltoml).
2. Go to your bank account.
3. Select your service, e.g. Giro.
4. Export csv file or similar to `~/Downloads`, or directly your source dir for your bank / service.
5. Optionally, if you've stored your file in `~/Downloads`, run `cd ~/Downloads` followed by `fintl store` (uses your `fintl.toml` from step 1).
6. Optionally, if you want to process png screenshots via ollama, start ollama.
7. Run the etl via `fintl etl` (also uses your `fintl.toml` from step 1).
8. Upon success visualize / search your data via `fintl plot` or `fintl search`.

[Please see here and below](./src/fintl/cli/README.md#top-level-usage) for more usage details.

## Repo structure

* `src/fintl/accounts_etl/` — core ETL logic: schemas, parsers, registry, runner
* `src/fintl/cli/` — CLI entry point and subcommands (`etl`, `store`, `search`, `plot`)
* `tests/` — tests for packages of this repo

## Development

Run tests:

```bash
uv run pytest -n auto tests
```

Type check:

```bash
uv run ty check src
```

Lint, format, type check, test and all the other good stuff:

```bash
pre-commit run --all-files
```
