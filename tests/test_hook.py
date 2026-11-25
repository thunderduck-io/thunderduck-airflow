"""Connection handling: host/port/endpoint, token sources, timing extras."""

from __future__ import annotations

import pytest

from thunderduck_airflow.exceptions import ThunderduckAuthError, ThunderduckError
from thunderduck_airflow.hooks.thunderduck import ThunderduckHook

from .conftest import CONN_ID, TOKEN


class StubClient:
    """Stands in for the REST client where only the call matters."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[str] = []

    def catalog_schema(self):
        self.calls.append("catalog_schema")
        if self.error:
            raise self.error
        return {"catalogs": []}

    def submit(self, sql: str) -> str:
        self.calls.append("submit")
        return "exec-1"

    def __enter__(self) -> StubClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.calls.append("close")


def test_a_bare_host_becomes_https(make_connection):
    make_connection(host="api.thunderduck.ai", password=TOKEN)
    assert ThunderduckHook().connection_config() == ("https://api.thunderduck.ai", TOKEN)


def test_a_host_with_a_scheme_is_kept(make_connection):
    make_connection(host="http://localhost", password=TOKEN)
    assert ThunderduckHook().connection_config()[0] == "http://localhost"


def test_the_port_is_appended(make_connection):
    make_connection(host="api.thunderduck.ai", port=8443, password=TOKEN)
    assert ThunderduckHook().connection_config()[0] == "https://api.thunderduck.ai:8443"


def test_an_endpoint_extra_wins_over_host_and_port(make_connection):
    make_connection(
        host="ignored.example",
        port=8443,
        password=TOKEN,
        extra='{"endpoint": "http://thunderduck.internal:8080"}',
    )
    assert ThunderduckHook().connection_config()[0] == "http://thunderduck.internal:8080"


def test_a_connection_without_a_host_is_rejected(make_connection):
    make_connection(password=TOKEN)
    with pytest.raises(ThunderduckError, match="no host"):
        ThunderduckHook().connection_config()


def test_a_connection_without_a_token_is_rejected(make_connection):
    make_connection(host="api.thunderduck.ai")
    with pytest.raises(ThunderduckError, match="no token"):
        ThunderduckHook().connection_config()


def test_the_token_can_come_from_extras(make_connection):
    make_connection(host="api.thunderduck.ai", extra='{"token": "tdk_from_extra"}')
    assert ThunderduckHook().connection_config()[1] == "tdk_from_extra"


def test_the_password_wins_over_the_extra_token(make_connection):
    make_connection(
        host="api.thunderduck.ai", password="tdk_password", extra='{"token": "tdk_extra"}'
    )
    assert ThunderduckHook().connection_config()[1] == "tdk_password"


def test_get_conn_reads_the_timing_extras(make_connection):
    make_connection(
        host="api.thunderduck.ai",
        password=TOKEN,
        extra='{"timeout": 42, "poll_interval": 7}',
    )
    client = ThunderduckHook().get_conn()
    try:
        assert client._timeout == 42.0
        assert client._poll_interval == 7.0
    finally:
        client.close()


def test_the_connection_id_is_the_one_requested(make_connection):
    make_connection(host="api.thunderduck.ai", password=TOKEN)
    hook = ThunderduckHook("analytics")
    assert hook.thunderduck_conn_id == "analytics"
    assert ThunderduckHook().thunderduck_conn_id == CONN_ID


def test_test_connection_reports_success(make_connection, monkeypatch):
    make_connection(host="api.thunderduck.ai", password=TOKEN)
    stub = StubClient()
    monkeypatch.setattr(ThunderduckHook, "get_conn", lambda self: stub)

    assert ThunderduckHook().test_connection() == (
        True,
        "Connected to the Thunderduck API.",
    )
    assert stub.calls == ["catalog_schema", "close"]


def test_test_connection_reports_the_api_message(make_connection, monkeypatch):
    make_connection(host="api.thunderduck.ai", password=TOKEN)
    monkeypatch.setattr(
        ThunderduckHook,
        "get_conn",
        lambda self: StubClient(ThunderduckAuthError("authentication failed")),
    )

    ok, message = ThunderduckHook().test_connection()

    assert ok is False
    assert message == "authentication failed"


def test_submit_closes_the_client(make_connection, monkeypatch):
    make_connection(host="api.thunderduck.ai", password=TOKEN)
    stub = StubClient()
    monkeypatch.setattr(ThunderduckHook, "get_conn", lambda self: stub)

    assert ThunderduckHook().submit("select 1") == "exec-1"
    assert stub.calls == ["submit", "close"]
