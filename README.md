# `fintl`

[![CI](https://github.com/eschmidt42/fintl/actions/workflows/ci.yml/badge.svg)](https://github.com/eschmidt42/fintl/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/eschmidt42/fintl/branch/main/graph/badge.svg)](https://codecov.io/gh/eschmidt42/fintl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

## How to use

### Bank / investment accounts ETL

Install as a `uv tool` via

  uv tool install .

from the project root. If you are somewhere else than in the project repo, point to the path where you cloned the repo.

After the installation `fintl` should become available as a cli tool. See [this README.md](./src/fintl/cli/README.md) for details.

## Repo structure

* `src/fintl/`: core packages providing base functionality
* `src/fintl/cli/`: fintl CLI main and components
* `tests/`: tests for packages of this repo
