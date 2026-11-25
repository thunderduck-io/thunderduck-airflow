"""Operator that submits a statement and waits for the stored result."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..exceptions import ThunderduckError
from ..hooks.thunderduck import ThunderduckHook
from .base import ThunderduckQueryBaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context


class ThunderduckQueryOperator(ThunderduckQueryBaseOperator):
    """Run SQL on Thunderduck and wait for the execution to finish.

    The operator submits to ``POST /queries``, polls ``GET /executions/{id}``
    and fails the task if the run ends in ``error`` or is ``cancelled``. On a
    failure it asks Thunderduck to cancel the run, so a retry does not leave
    the previous execution burning compute.

    Returns ``{"execution_id": ..., "row_count": ...}``; with
    ``fetch_rows=N`` it also returns ``columns`` and the first ``N`` rows.
    """

    def __init__(
        self,
        *,
        timeout: float | None = None,
        poll_interval: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.timeout = timeout
        self.poll_interval = poll_interval

    def execute(self, context: Context) -> dict[str, Any]:
        hook = ThunderduckHook(self.thunderduck_conn_id)
        with hook.get_conn() as client:
            self.execution_id = client.submit(self.sql)
            self.log.info("Submitted %r as execution %s", _shorten(self.sql), self.execution_id)
            try:
                execution = client.wait(
                    self.execution_id,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                )
            except ThunderduckError:
                self._cancel_quietly(client)
                raise
            self._log_execution(execution)
            return self._result(client, self.execution_id, execution.result_row_count)

    def on_kill(self) -> None:
        if not self.execution_id:
            return
        try:
            with ThunderduckHook(self.thunderduck_conn_id).get_conn() as client:
                self._cancel_quietly(client)
        except ThunderduckError as err:
            self.log.warning("Could not reach Thunderduck to cancel: %s", err)


def _shorten(sql: str, limit: int = 120) -> str:
    flat = " ".join(sql.split())
    return flat if len(flat) <= limit else f"{flat[:limit]}…"


__all__ = ["ThunderduckQueryOperator"]
