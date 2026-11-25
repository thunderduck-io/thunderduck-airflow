"""Shared behaviour for the Thunderduck query operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.sdk import BaseOperator

from ..client import Execution, ThunderduckClient
from ..exceptions import ThunderduckError
from ..hooks.thunderduck import DEFAULT_CONN_ID


class ThunderduckQueryBaseOperator(BaseOperator):
    """Submit one statement and turn the finished execution into an XCom value.

    The XCom is deliberately small by default: an execution id and a row count.
    Set ``fetch_rows`` to also pull that many rows into XCom, and remember that
    XCom is a metadata-database column, not a data channel — hand off big
    results with the Parquet URL or the download operator instead.
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#f4e3b1"

    def __init__(
        self,
        *,
        sql: str,
        thunderduck_conn_id: str = DEFAULT_CONN_ID,
        fetch_rows: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if fetch_rows < 0:
            raise ValueError("fetch_rows must be zero or positive")
        self.sql = sql
        self.thunderduck_conn_id = thunderduck_conn_id
        self.fetch_rows = fetch_rows
        #: Set by ``execute``; cancellation uses it.
        self.execution_id: str | None = None

    def _result(
        self, client: ThunderduckClient, exec_id: str, row_count: int | None
    ) -> dict[str, Any]:
        result: dict[str, Any] = {"execution_id": exec_id, "row_count": row_count or 0}
        if self.fetch_rows:
            page = client.fetch_rows(exec_id, limit=self.fetch_rows)
            result["columns"] = page.columns
            result["rows"] = page.rows
            result["row_count"] = page.total or len(page.rows)
        return result

    def _log_execution(self, execution: Execution) -> None:
        self.log.info(
            "Execution %s finished as %s (%s rows)",
            execution.uuid,
            execution.status,
            execution.result_row_count if execution.result_row_count is not None else "unknown",
        )

    def _cancel_quietly(self, client: ThunderduckClient, exec_id: str | None = None) -> None:
        exec_id = exec_id or self.execution_id
        if not exec_id:
            return
        try:
            client.cancel(exec_id)
            self.log.info("Cancelled execution %s", exec_id)
        except ThunderduckError as err:  # never mask the original failure
            self.log.warning("Could not cancel execution %s: %s", exec_id, err)


__all__ = ["ThunderduckQueryBaseOperator"]
