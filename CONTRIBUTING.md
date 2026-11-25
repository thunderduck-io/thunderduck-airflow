# Contributing

Thanks for taking the time to look at `thunderduck-airflow`.

## This repository is a mirror

The source of truth for this driver lives in Thunderduck's internal monorepo.
This repository is a one-way snapshot: each update replaces the tree with the
current state of the source, as a single commit. That means the history here is
not the development history of the project, and a pull request cannot be merged
directly into it.

## Issues are welcome

Bug reports and feature requests are read here. Please include:

- the provider and Airflow versions (`pip show thunderduck-airflow apache-airflow`);
- the Python version and platform;
- what you ran, what you expected, and what happened;
- if relevant, the smallest DAG and SQL statement (with the token removed)
  that reproduces it.

For anything that looks like a security problem, use
[SECURITY.md](SECURITY.md) instead of a public issue.

## Pull requests

Pull requests are welcome as a place to discuss a change, and small fixes are
genuinely useful. If a change is accepted, it is applied to the internal source
and appears in the next snapshot; the pull request is then closed rather than
merged, so don't be surprised if your commit does not appear here verbatim.

Please keep a change focused: one behaviour per pull request, with a test that
fails before it and passes after.

## Development

```bash
uv venv --python 3.12
uv pip install -e . "apache-airflow==3.4.0" pytest ruff
AIRFLOW_HOME=$(mktemp -d) uv run --no-sync pytest
uv run --no-sync ruff check . && uv run --no-sync ruff format --check .
```

The provider supports Airflow 3.0 and newer. Check the lower bound too when
touching the hook, trigger or sensor:

```bash
uv venv --python 3.11 .af30
uv pip install --python .af30 -e . "apache-airflow==3.0.0" pytest
AIRFLOW_HOME=$(mktemp -d) .af30/bin/pytest -q
```

The test suite uses an in-process HTTP stub; it needs no API token and no
metadata database.

## License

By contributing you agree that your contribution is licensed under the Apache
License 2.0, the license this project uses. See [LICENSE](LICENSE).
