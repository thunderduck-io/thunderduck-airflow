"""Operators and sensor: what they send, what they return, and what they cancel."""

from __future__ import annotations

import pytest

try:  # Airflow 3.4 moved TaskDeferred; 3.0 keeps it in airflow.exceptions
    from airflow.sdk.exceptions import TaskDeferred
except ImportError:  # pragma: no cover - Airflow 3.0 only
    from airflow.exceptions import TaskDeferred

from thunderduck_airflow.client import Execution, ResultPage
from thunderduck_airflow.exceptions import (
    ThunderduckError,
    ThunderduckQueryError,
    ThunderduckTimeout,
)
from thunderduck_airflow.hooks.thunderduck import ThunderduckHook
from thunderduck_airflow.operators.query import ThunderduckQueryOperator
from thunderduck_airflow.operators.query_async import ThunderduckQueryAsyncOperator
from thunderduck_airflow.sensors.execution import ThunderduckExecutionSensor
from thunderduck_airflow.triggers.execution import ThunderduckExecutionTrigger

from .conftest import EXEC_ID


class FakeClient:
    def __init__(
        self,
        *,
        execution: Execution | None = None,
        wait_error: Exception | None = None,
        page: ResultPage | None = None,
        cancel_error: Exception | None = None,
    ) -> None:
        self.execution = execution or Execution(uuid=EXEC_ID, status="success", result_row_count=3)
        self.wait_error = wait_error
        self.cancel_error = cancel_error
        self.page = page or ResultPage(
            columns=["n"], column_types=["BIGINT"], rows=[[1]], total=1, offset=0
        )
        self.submitted: list[str] = []
        self.cancelled: list[str] = []
        self.closed = False
        self.wait_kwargs: dict[str, object] = {}
        self.fetch_limit: int | None = None

    def submit(self, sql: str) -> str:
        self.submitted.append(sql)
        return EXEC_ID

    def wait(self, exec_id: str, *, timeout=None, poll_interval=None) -> Execution:
        self.wait_kwargs = {"timeout": timeout, "poll_interval": poll_interval}
        if self.wait_error:
            raise self.wait_error
        return self.execution

    def get_execution(self, exec_id: str) -> Execution:
        return self.execution

    def fetch_rows(self, exec_id: str, limit: int) -> ResultPage:
        self.fetch_limit = limit
        return self.page

    def cancel(self, exec_id: str) -> None:
        self.cancelled.append(exec_id)
        if self.cancel_error:
            raise self.cancel_error

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@pytest.fixture
def use_client(monkeypatch):
    def _use(client: FakeClient) -> FakeClient:
        monkeypatch.setattr(ThunderduckHook, "get_conn", lambda self: client)
        return client

    return _use


# ---- submit and wait ----


def test_execute_submits_waits_and_returns_the_execution(use_client):
    client = use_client(FakeClient())
    operator = ThunderduckQueryOperator(
        task_id="query", sql="select 1", timeout=120, poll_interval=3
    )

    result = operator.execute({})

    assert result == {"execution_id": EXEC_ID, "row_count": 3}
    assert client.submitted == ["select 1"]
    assert client.wait_kwargs == {"timeout": 120, "poll_interval": 3}
    assert client.closed is True
    assert operator.execution_id == EXEC_ID


def test_execute_keeps_rows_out_of_xcom_by_default(use_client):
    client = use_client(FakeClient())
    result = ThunderduckQueryOperator(task_id="query", sql="select 1").execute({})

    assert "rows" not in result
    assert client.fetch_limit is None


def test_fetch_rows_adds_columns_and_rows(use_client):
    use_client(FakeClient())
    operator = ThunderduckQueryOperator(task_id="query", sql="select 1", fetch_rows=25)

    result = operator.execute({})

    assert result["columns"] == ["n"]
    assert result["rows"] == [[1]]
    assert result["row_count"] == 1


def test_a_failed_run_is_cancelled_before_the_error_propagates(use_client):
    client = use_client(FakeClient(wait_error=ThunderduckQueryError("syntax error")))
    operator = ThunderduckQueryOperator(task_id="query", sql="select")

    with pytest.raises(ThunderduckQueryError, match="syntax error"):
        operator.execute({})

    assert client.cancelled == [EXEC_ID]


def test_a_failed_cancel_does_not_mask_the_original_error(use_client):
    client = use_client(
        FakeClient(
            wait_error=ThunderduckTimeout("still running"),
            cancel_error=ThunderduckError("cannot cancel"),
        )
    )
    operator = ThunderduckQueryOperator(task_id="query", sql="select 1")

    with pytest.raises(ThunderduckTimeout, match="still running"):
        operator.execute({})

    assert client.cancelled == [EXEC_ID]
    assert client.closed is True


def test_on_kill_cancels_the_active_execution(use_client):
    client = use_client(FakeClient())
    operator = ThunderduckQueryOperator(task_id="query", sql="select 1")
    operator.execution_id = EXEC_ID

    operator.on_kill()

    assert client.cancelled == [EXEC_ID]


def test_on_kill_before_a_submit_is_a_no_op(use_client):
    client = use_client(FakeClient())
    ThunderduckQueryOperator(task_id="query", sql="select 1").on_kill()

    assert client.cancelled == []


def test_negative_fetch_rows_is_rejected():
    with pytest.raises(ValueError, match="fetch_rows"):
        ThunderduckQueryOperator(task_id="query", sql="select 1", fetch_rows=-1)


def test_sql_is_templated_from_a_file():
    operator = ThunderduckQueryOperator(task_id="query", sql="sql/daily.sql")
    assert operator.template_ext == (".sql",)
    assert operator.template_fields == ("sql",)


# ---- deferred ----


def test_the_async_operator_submits_then_defers(use_client):
    client = use_client(FakeClient())
    operator = ThunderduckQueryAsyncOperator(
        task_id="query", sql="select 1", timeout=600, poll_interval=15
    )

    with pytest.raises(TaskDeferred) as caught:
        operator.execute({})

    assert client.submitted == ["select 1"]
    assert client.closed is True
    assert caught.value.method_name == "execute_complete"
    trigger = caught.value.trigger
    assert isinstance(trigger, ThunderduckExecutionTrigger)
    assert trigger.execution_id == EXEC_ID
    assert trigger.poll_interval == 15
    assert trigger.timeout == 600


def test_the_deferred_trigger_carries_no_token():
    trigger = ThunderduckExecutionTrigger(
        execution_id=EXEC_ID, thunderduck_conn_id="thunderduck_default"
    ).serialize()

    assert trigger[0] == ("thunderduck_airflow.triggers.execution.ThunderduckExecutionTrigger")
    assert trigger[1] == {
        "execution_id": EXEC_ID,
        "thunderduck_conn_id": "thunderduck_default",
        "poll_interval": 5.0,
        "timeout": 3600.0,
    }
    assert "tok" not in str(trigger[1]).lower()


def test_execute_complete_returns_the_result(use_client):
    use_client(FakeClient())
    operator = ThunderduckQueryAsyncOperator(task_id="query", sql="select 1")

    result = operator.execute_complete(
        {}, {"status": "success", "execution_id": EXEC_ID, "row_count": 9}
    )

    assert result == {"execution_id": EXEC_ID, "row_count": 9}


def test_execute_complete_can_fetch_rows(use_client):
    use_client(FakeClient())
    operator = ThunderduckQueryAsyncOperator(task_id="query", sql="select 1", fetch_rows=5)

    result = operator.execute_complete(
        {}, {"status": "success", "execution_id": EXEC_ID, "row_count": 9}
    )

    assert result["rows"] == [[1]]
    assert result["columns"] == ["n"]


def test_execute_complete_fails_on_an_error_event(use_client):
    use_client(FakeClient())
    operator = ThunderduckQueryAsyncOperator(task_id="query", sql="select")

    with pytest.raises(ThunderduckQueryError, match="boom"):
        operator.execute_complete(
            {}, {"status": "error", "execution_id": EXEC_ID, "message": "boom"}
        )


@pytest.mark.parametrize("status", ["timeout", "cancelled"])
def test_execute_complete_fails_on_a_non_success_event(use_client, status):
    use_client(FakeClient())
    operator = ThunderduckQueryAsyncOperator(task_id="query", sql="select 1")

    with pytest.raises(ThunderduckError, match=f"ended as '{status}'"):
        operator.execute_complete({}, {"status": status, "execution_id": EXEC_ID})


# ---- sensor ----


def test_the_sensor_waits_while_the_run_is_active(use_client):
    use_client(FakeClient(execution=Execution(uuid=EXEC_ID, status="running")))
    sensor = ThunderduckExecutionSensor(task_id="wait", execution_id=EXEC_ID)

    assert sensor.poke({}) is False


def test_the_sensor_succeeds_when_the_run_does(use_client):
    use_client(FakeClient(execution=Execution(uuid=EXEC_ID, status="success", result_row_count=4)))
    sensor = ThunderduckExecutionSensor(task_id="wait", execution_id=EXEC_ID)

    assert sensor.poke({}) is True


def test_the_sensor_fails_fast_on_an_errored_run(use_client):
    use_client(
        FakeClient(execution=Execution(uuid=EXEC_ID, status="error", error_message="bad sql"))
    )
    sensor = ThunderduckExecutionSensor(task_id="wait", execution_id=EXEC_ID)

    with pytest.raises(ThunderduckQueryError, match="bad sql"):
        sensor.poke({})


def test_the_sensor_fails_on_a_cancelled_run(use_client):
    use_client(FakeClient(execution=Execution(uuid=EXEC_ID, status="cancelled")))
    sensor = ThunderduckExecutionSensor(task_id="wait", execution_id=EXEC_ID)

    with pytest.raises(ThunderduckError, match="cancelled"):
        sensor.poke({})


def test_the_sensor_execution_id_is_templated():
    sensor = ThunderduckExecutionSensor(task_id="wait", execution_id=EXEC_ID)
    assert sensor.template_fields == ("execution_id",)
