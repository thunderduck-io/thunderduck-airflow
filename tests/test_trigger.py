"""The triggerer's poll loop, driven over a loopback transport."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from thunderduck_airflow.exceptions import ThunderduckError
from thunderduck_airflow.hooks.thunderduck import ThunderduckHook
from thunderduck_airflow.triggers.execution import ThunderduckExecutionTrigger

from .conftest import BASE_URL, EXEC_ID, TOKEN, json_response


@pytest.fixture
def connection(monkeypatch):
    monkeypatch.setattr(ThunderduckHook, "connection_config", lambda self: (BASE_URL, TOKEN))


def run(trigger: ThunderduckExecutionTrigger) -> list[dict]:
    async def collect() -> list[dict]:
        return [event.payload async for event in trigger.run()]

    return asyncio.run(collect())


def trigger_for(handler, **kwargs) -> ThunderduckExecutionTrigger:
    kwargs.setdefault("poll_interval", 0)
    return ThunderduckExecutionTrigger(
        execution_id=EXEC_ID,
        thunderduck_conn_id="thunderduck_default",
        transport=httpx.MockTransport(handler),
        **kwargs,
    )


def test_success_after_polling(connection):
    statuses = iter(["running", "running", "success"])

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/executions/{EXEC_ID}"
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        return json_response({"status": next(statuses), "result_row_count": 42})

    events = run(trigger_for(handler))

    assert events == [{"status": "success", "execution_id": EXEC_ID, "row_count": 42}]


def test_an_errored_run_becomes_an_error_event(connection):
    events = run(
        trigger_for(
            lambda request: json_response({"status": "error", "error_message": "table not found"})
        )
    )

    assert events == [{"status": "error", "execution_id": EXEC_ID, "message": "table not found"}]


def test_a_cancelled_run_becomes_a_cancelled_event(connection):
    events = run(trigger_for(lambda request: json_response({"status": "cancelled"})))

    assert events[0]["status"] == "cancelled"


def test_the_deadline_produces_a_timeout_event(connection):
    events = run(trigger_for(lambda request: json_response({"status": "queued"}), timeout=0))

    assert events[0]["status"] == "timeout"
    assert "queued" in events[0]["message"]


def test_an_authentication_failure_ends_the_trigger(connection):
    events = run(trigger_for(lambda request: json_response({"detail": "bad"}, status=401)))

    assert events[0]["status"] == "error"
    assert "authentication failed" in events[0]["message"]
    assert TOKEN not in events[0]["message"]


def test_a_network_blip_is_retried(connection):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection reset")
        return json_response({"status": "success", "result_row_count": 1})

    events = run(trigger_for(handler, timeout=5))

    assert events[0]["status"] == "success"
    assert calls["n"] == 2


def test_a_missing_connection_is_an_error_event(monkeypatch):
    def explode(self):
        raise ThunderduckError("connection 'nope' has no token")

    monkeypatch.setattr(ThunderduckHook, "connection_config", explode)
    events = run(trigger_for(lambda request: json_response({"status": "running"})))

    assert events == [
        {
            "status": "error",
            "execution_id": EXEC_ID,
            "message": "connection 'nope' has no token",
        }
    ]
