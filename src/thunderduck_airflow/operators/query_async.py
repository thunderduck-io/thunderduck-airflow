"""Deferrable operator: submit on the worker, wait on the triggerer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..exceptions import ThunderduckError, ThunderduckQueryError
from ..hooks.thunderduck import ThunderduckHook
from ..triggers.execution import ThunderduckExecutionTrigger
from .base import ThunderduckQueryBaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context


class ThunderduckQueryAsyncOperator(ThunderduckQueryBaseOperator):
    """Submit SQL and defer, so no worker slot is held while it runs.

    Identical results to :class:`ThunderduckQueryOperator`, but after the
    submit the task is deferred and the triggerer watches the execution. Use it
    for long statements on a deployment with a triggerer; use the
    synchronous operator on small deployments that have none.
    """

    def __init__(
        self,
        *,
        timeout: float = 3600.0,
        poll_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.timeout = timeout
        self.poll_interval = poll_interval

    def execute(self, context: Context) -> None:
        with ThunderduckHook(self.thunderduck_conn_id).get_conn() as client:
            self.execution_id = client.submit(self.sql)
        self.log.info(
            "Submitted %r as execution %s; deferring to the triggerer",
            _shorten(self.sql),
            self.execution_id,
        )
        self.defer(
            trigger=ThunderduckExecutionTrigger(
                execution_id=self.execution_id,
                thunderduck_conn_id=self.thunderduck_conn_id,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        status = event.get("status")
        exec_id = event.get("execution_id") or self.execution_id
        if status == "success":
            with ThunderduckHook(self.thunderduck_conn_id).get_conn() as client:
                return self._result(client, exec_id, event.get("row_count"))

        message = event.get("message") or f"execution {exec_id} ended as {status!r}"
        if status == "error":
            raise ThunderduckQueryError(message)
        raise ThunderduckError(message)


def _shorten(sql: str, limit: int = 120) -> str:
    flat = " ".join(sql.split())
    return flat if len(flat) <= limit else f"{flat[:limit]}…"


__all__ = ["ThunderduckQueryAsyncOperator"]
