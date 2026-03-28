# Release Guide

This document describes how to release new versions of `fintl` to PyPI and TestPyPI.

## Overview

Releases are fully automated via the [publish workflow](../.github/workflows/publish.yml) and triggered by pushing a version tag. There is no manual upload step.

| Tag format | Publishes to |
|---|---|
| `v1.2.3a1`, `v1.2.3b1`, `v1.2.3rc1`, `v1.2.3.dev1` | TestPyPI only |
| `v1.2.3` | PyPI only |

Version numbers follow [PEP 440](https://peps.python.org/pep-0440/).

## Prerequisites

- You have push access to the repository.
- The `pypi` and `testpypi` GitHub Actions environments are configured with trusted publishing (OIDC). No API tokens are needed.

## Steps

### 1. Update the version

Edit `pyproject.toml` and bump the `version` field:

```toml
[project]
version = "1.2.3"
```

Commit the change:

```bash
git add pyproject.toml
git commit -m "chore: bump version to 1.2.3"
```

### 2. Tag the release

**Pre-release** (publishes to TestPyPI):

```bash
git tag v1.2.3rc1
git push origin v1.2.3rc1
```

**Final release** (publishes to PyPI):

```bash
git tag v1.2.3
git push origin v1.2.3
```

### 3. Verify

- Check the [Actions tab](https://github.com/eschmidt42/fintl/actions) to confirm the workflow passes.
- For TestPyPI: https://test.pypi.org/p/fintl
- For PyPI: https://pypi.org/p/fintl

## Pre-release workflow

Use pre-releases to validate that the package builds and installs correctly before cutting a final release:

```bash
# Tag an alpha, beta, or release candidate
git tag v1.2.3a1 && git push origin v1.2.3a1

# Install from TestPyPI to verify
uv pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple fintl==1.2.3a1

# Once satisfied, tag the final release
git tag v1.2.3 && git push origin v1.2.3
```
