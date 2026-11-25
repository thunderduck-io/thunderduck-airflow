# thunderduck-airflow

An Apache Airflow provider for [Thunderduck](https://thunderduck.ai): submit SQL
to the REST API, wait for the stored result, cancel cleanly, and hand the
execution id on to the next task.

```bash
pip install thunderduck-airflow
```

Requires Apache Airflow 3.0+ and Python 3.11+. Airflow 2 reached end of life on
2026-04-22; this provider does not carry the compatibility shims for it.

## The connection

Create an Airflow connection of type **Thunderduck** (or via the CLI/env):

| Field | Value |
| --- | --- |
| Conn Id | `thunderduck_default` (or your own) |
| Host | `api.thunderduck.ai` — `https://` is optional |
| Password | your Thunderduck API token |
| Extra | optional JSON, see below |

```bash
airflow connections add thunderduck_default \
  --conn-type thunderduck \
  --conn-host api.thunderduck.ai \
  --conn-password "$THUNDERDUCK_TOKEN"
```

Extras:

```json
{
  "endpoint": "https://api.thunderduck.ai",
  "timeout": 3600,
  "poll_interval": 2,
  "token": "tdk_..."
}
```

`endpoint` wins over host/port, `timeout` is how long to wait for a *run*
(seconds, default 3600), `poll_interval` is the delay between status checks
(default 2), and `token` is an alternative to the password field. The **Test
connection** button performs one authenticated `GET /catalog/schema`.

## Running a query

```python
from thunderduck_airflow.operators.query import ThunderduckQueryOperator

preview = ThunderduckQueryOperator(
    task_id="preview",
    sql="""
        SELECT region, sum(total) AS revenue
        FROM sales_pg.public.orders
        GROUP BY region
        ORDER BY revenue DESC
    """,
    fetch_rows=50,
)
```

The task returns, and pushes to XCom:

```python
{"execution_id": "…", "row_count": 12, "columns": ["region", "revenue"], "rows": [...]}
```

`columns`/`rows` are only present when `fetch_rows` is set. XCom lives in the
metadata database, so keep it small: for a real hand-off use the execution id
and fetch the Parquet export instead.

`sql` is templated, and a value ending in `.sql` is loaded from the DAG folder:

```python
ThunderduckQueryOperator(task_id="daily", sql="sql/daily_revenue.sql")
```

A failed, errored or timed-out run fails the task. The operator asks Thunderduck
to cancel the run first, so a retry does not leave the previous execution
burning compute; ``on_kill`` does the same when the task is cleared or killed.

## Deferring to the triggerer

```python
from thunderduck_airflow.operators.query_async import ThunderduckQueryAsyncOperator

rollup = ThunderduckQueryAsyncOperator(
    task_id="daily_rollup",
    sql="CREATE OR REPLACE TABLE lake.default.daily_revenue AS …",
    poll_interval=10,
    timeout=1800,
)
```

After submitting, the task defers: the triggerer polls the execution and no
worker slot is held. The trigger resolves the connection itself, so the token
never travels through the metadata database. Requires a running `triggerer`.

## Waiting on somebody else's execution

```python
from thunderduck_airflow.sensors.execution import ThunderduckExecutionSensor

wait = ThunderduckExecutionSensor(
    task_id="wait_for_export",
    execution_id="{{ ti.xcom_pull(task_ids='submit_elsewhere')['execution_id'] }}",
    mode="reschedule",
    poke_interval=30,
)
```

Use it when the submit happens in another DAG or outside Airflow. A failed or
cancelled execution fails the sensor immediately instead of waiting out its
timeout.

## Errors

Everything derives from `airflow.exceptions.AirflowException`:

| Exception | Meaning |
| --- | --- |
| `ThunderduckAuthError` | the token was rejected (401/403) — fix the connection |
| `ThunderduckQueryError` | the run finished in the `error` state |
| `ThunderduckTimeout` | the run was still going at the deadline |
| `ThunderduckError` | anything else: unreachable API, 4xx/5xx, cancelled run |

The token never appears in an exception message or the logs.

## Limitations

* One statement per task. Thunderduck runs one statement per execution by
  design; split multi-statement scripts into tasks.
* Results stay in the lakehouse. This provider does not pull large results
  through XCom — the execution id is the hand-off, and
  `GET /executions/{id}/download-url` returns a presigned Parquet URL.
* Airflow 3 only, as above.

## Development

```bash
uv venv --python 3.12
uv pip install -e . "apache-airflow==3.4.0" pytest ruff
AIRFLOW_HOME=$(mktemp -d) uv run --no-sync pytest
uv run --no-sync ruff check . && uv run --no-sync ruff format --check .
```

The tests talk to an `httpx.MockTransport`; they never need a live API or a
metadata database.

## License

Apache-2.0. Source: https://github.com/thunderduck-io/thunderduck-airflow
