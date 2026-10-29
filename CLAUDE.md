# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Scripts that analyse HDX datasets and organisations and produce statistics as CSV files. There are
two entry points:

- `datasets` writes `datasets.csv` (one row per HDX dataset) and `non_script_updates.csv`.
- `orgs` writes `org_stats.csv` (one row per HDX organisation).

Data comes from the HDX CKAN API (datasets, organisations, users, package links, requests) and
Mixpanel (download counts). Both scripts run daily via GitHub Actions
(`.github/workflows/run-python-script-*.yaml`), which deploy the output folders `datasets_info/` and
`org_stats/` to the `gh-pages` branch. Analysts also run them manually each quarter.

## Commands

Environment setup (Python 3.13, managed with `uv`):

```shell
uv sync
```

Run the scripts (requires `~/.hdx_configuration.yaml` with an HDX key, `~/.useragents.yaml` with
a `hdx-analysis-scripts` entry, and Mixpanel credentials — see README.md):

```shell
uv run python -m hdx.analysis_scripts.datasets --output_dir=datasets_info
uv run python -m hdx.analysis_scripts.orgs --output_dir=org_stats
```

`--saved_dir` saves the downloaded raw data to a folder.

Run all tests (with coverage, configured in `pyproject.toml`):

```shell
uv run pytest
```

Lint and format:

```shell
uv run ruff check
uv run ruff format
```

Pre-commit (runs ruff on every commit once installed):

```shell
pre-commit install
pre-commit run --all-files
```

Build:

```shell
uv build
```

Adding a dependency: add it to `project.dependencies` in `pyproject.toml` (or `[dependency-groups]`
for test-only deps), then run `uv lock --upgrade` to refresh `uv.lock` (pre-commit also does this
automatically on commit).

## Architecture

`src/hdx/analysis_scripts/`:

- `datasets/__main__.py` — datasets script: builds one row per dataset from `DatasetStatistics`.
- `orgs/__main__.py` — org stats script: aggregates per-organisation counts and percentages.
- `common/__init__.py` — shared helpers (explorer/grid mappings, request mappings, aging config,
  previous quarter).
- `common/downloads.py` — `Downloads` class wrapping all external fetches (CKAN, Mixpanel), with
  optional save/load of raw data via `saved_dir`.
- `common/dataset_statistics.py` — `DatasetStatistics`, which derives per-dataset fields (status,
  COD, freshness, update frequency, updated by script, maintainer, etc.).
- `common/config/project_configuration.yaml` — org type mapping, aging thresholds and other
  config.

Tests (`tests/`) replace `Downloads` with a mock that reads JSON from `tests/fixtures/input/` and
compare generated CSVs against expected files in `tests/fixtures/`.

## Code Style

- Formatted with `ruff` via pre-commit hooks. After changing any Python code, run:

```bash
pre-commit run --all-files
```

- Python ≥ 3.13

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag
  tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions,
  external systems). Search or read the actual source first. If you lack verified information, say
  so rather than speculate.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific
issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements
in the same commit unless they are directly required by the fix. Unrelated changes obscure the
intent of the fix and complicate review and blame.

## Decision Records

Non-trivial design decisions are recorded in `docs/decisions/` (see `docs/decisions/README.md`) —
the distilled decision, not the full planning narrative, belongs here.
