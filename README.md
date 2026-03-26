# `fintl`

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
