# hdx-analysis-scripts

Scripts used by the Centre for Humanitarian Data to analyse and produce statistics on HDX datasets.

These scripts are run manually on a quarterly basis by analysts. They are typically invoked with:

```shell
    uv run python -m hdx.analysis_scripts.orgs --output_dir=org_stats
```

and

```shell
    uv run python -m hdx.analysis_scripts.datasets --output_dir=datasets_info
```

The runtime for the datasets_info is approximately 5 minutes and generates two CSV files `datasets.csv` which is approximately 13MB and `non_script_updates.csv which is approximately 2Kb.

The runtime for the org_stats is approximately 5 minutes and generates one CSV file `org_stats.csv` which is approximately 36Kb.



## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

`hdx-analysis-scripts` uses the `hdx-python-api` library, configuration for which is done in the usual way [described here](https://hdx-python-api.readthedocs.io/en/latest/).

For local use the user agent (`AnalysisScripts`) is specified in the `~/.useragents.yaml` file.
```yaml
hdx-analysis-scripts:
    preprefix: [YOUR_ORGANISATION]
    user_agent: AnalysisScripts
```

These scripts require Mixpanel account credentials to operate, for local development the following environment variables need to be set:

```
MIXPANEL_API_SECRET
MIXPANEL_PROJECT_ID
MIXPANEL_TOKEN
```

Mixpanel accounts for users authenticate by sending a login email to registered users on login request.

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you’ve introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
