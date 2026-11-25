"""Sensor that waits for an execution submitted elsewhere."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.sdk import BaseSensorOperator

from ..exceptions import ThunderduckError, ThunderduckQueryError
from ..hooks.thunderduck import DEFAULT_CONN_ID, ThunderduckHook

if TYPE_CHECKING:
    from airflow.sdk import Context


class ThunderduckExecutionSensor(BaseSensorOperator):
    """Wait for an execution id that another task or system submitted.

    Use it when the submit and the wait belong to different DAGs, or when a
    service outside Airflow starts the query. For "submit here and wait here",
    :class:`~thunderduck_airflow.operators.query.ThunderduckQueryOperator` is
    simpler.

    ``execution_id`` is templated, so it can come from XCom:
    ``"{{ ti.xcom_pull(task_ids='submit')['execution_id'] }}"``. A failed or
    cancelled execution fails the sensor instead of waiting out its timeout.
    """

    template_fields = ("execution_id",)

    def __init__(
        self,
        *,
        execution_id: str,
        thunderduck_conn_id: str = DEFAULT_CONN_ID,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.execution_id = execution_id
        self.thunderduck_conn_id = thunderduck_conn_id

    def poke(self, context: Context) -> bool:
        with ThunderduckHook(self.thunderduck_conn_id).get_conn() as client:
            execution = client.get_execution(self.execution_id)

        if execution.status == "error":
            raise ThunderduckQueryError(
                execution.error_message or f"execution {self.execution_id} failed"
            )
        if execution.status == "cancelled":
            raise ThunderduckError(f"execution {self.execution_id} was cancelled")
        if execution.status == "success":
            self.log.info(
                "Execution %s succeeded (%s rows)",
                self.execution_id,
                execution.result_row_count if execution.result_row_count is not None else "unknown",
            )
            return True
        self.log.info("Execution %s is %s", self.execution_id, execution.status or "unknown")
        return False


__all__ = ["ThunderduckExecutionSensor"]
