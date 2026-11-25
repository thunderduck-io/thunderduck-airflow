"""Example DAG for the Thunderduck provider.

Copy the tasks you need; the connection is the only prerequisite. Create an
Airflow connection ``thunderduck_default`` with host ``api.thunderduck.ai`` and
the API token in the password field.
"""

from __future__ import annotations

import datetime as dt

from airflow.sdk import DAG

from thunderduck_airflow.operators.query import ThunderduckQueryOperator
from thunderduck_airflow.operators.query_async import ThunderduckQueryAsyncOperator

with DAG(
    dag_id="example_thunderduck",
    description="Preview rows, then build a rollup table on Thunderduck.",
    start_date=dt.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "thunderduck"],
) as dag:
    # A small query: the first 50 rows ride along in XCom.
    preview = ThunderduckQueryOperator(
        task_id="preview",
        sql="""
            SELECT region, sum(total) AS revenue
            FROM sales_pg.public.orders
            GROUP BY region
            ORDER BY revenue DESC
        """,
        fetch_rows=50,
        poll_interval=2,
        timeout=600,
    )

    # A long statement: defer to the triggerer instead of holding a worker.
    rollup = ThunderduckQueryAsyncOperator(
        task_id="daily_rollup",
        sql="""
            CREATE OR REPLACE TABLE lake.default.daily_revenue AS
            SELECT region, sum(total) AS revenue
            FROM sales_pg.public.orders
            GROUP BY region
        """,
        poll_interval=10,
        timeout=1800,
    )

    preview >> rollup
