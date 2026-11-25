"""The REST transport.

Payload shapes are the API's; where a test is specific about a field name,
that is deliberate.
"""

from __future__ import annotations

import json

import httpx
import pytest

from thunderduck_airflow.client import ThunderduckClient
from thunderduck_airflow.exceptions import (
    NoStoredResult,
    ThunderduckAuthError,
    ThunderduckError,
    ThunderduckQueryError,
    ThunderduckTimeout,
)

from .conftest import BASE_URL, EXEC_ID, TOKEN, json_response


def test_submit_posts_the_sql_and_returns_the_id(make_client):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/queries"
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        assert json.loads(request.content) == {"sql": "select 1"}
        return json_response({"id": EXEC_ID}, status=201)

    assert make_client(handler).submit("select 1") == EXEC_ID


def test_submit_without_an_id_is_an_error(make_client):
    client = make_client(lambda request: json_response({}, status=201))
    with pytest.raises(ThunderduckError, match="execution id"):
        client.submit("select 1")


def test_wait_polls_until_success(make_client):
    statuses = iter(["running", "running", "success"])
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        calls.append(status)
        assert request.url.path == f"/executions/{EXEC_ID}"
        return json_response({"uuid": EXEC_ID, "status": status, "result_row_count": 7})

    execution = make_client(handler).wait(EXEC_ID)

    assert execution.status == "success"
    assert execution.result_row_count == 7
    assert calls == ["running", "running", "success"]


def test_wait_raises_the_api_error_message(make_client):
    client = make_client(
        lambda request: json_response(
            {"status": "error", "error_message": "Catalog 'nope' does not exist"}
        )
    )
    with pytest.raises(ThunderduckQueryError, match="Catalog 'nope' does not exist"):
        client.wait(EXEC_ID)


def test_wait_raises_on_a_cancelled_run(make_client):
    client = make_client(lambda request: json_response({"status": "cancelled"}))
    with pytest.raises(ThunderduckError, match="cancelled"):
        client.wait(EXEC_ID)


def test_wait_gives_up_at_the_deadline(make_client):
    client = make_client(lambda request: json_response({"status": "running"}))
    with pytest.raises(ThunderduckTimeout, match=EXEC_ID):
        client.wait(EXEC_ID, timeout=0)


def test_fetch_page_sends_the_window_and_maps_the_body(make_client):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/executions/{EXEC_ID}/result"
        assert request.url.params["limit"] == "2"
        assert request.url.params["offset"] == "4"
        return json_response(
            {
                "columns": ["region", "revenue"],
                "column_types": ["VARCHAR", "DOUBLE"],
                "rows": [["eu", 12.5], ["us", 9]],
                "total": 6,
                "offset": 4,
            }
        )

    page = make_client(handler).fetch_page(EXEC_ID, limit=2, offset=4)

    assert page.columns == ["region", "revenue"]
    assert page.column_types == ["VARCHAR", "DOUBLE"]
    assert page.rows == [["eu", 12.5], ["us", 9]]
    assert page.total == 6


def test_fetch_page_without_a_stored_result_is_its_own_signal(make_client):
    client = make_client(lambda request: json_response({"detail": "no stored result"}, status=404))
    with pytest.raises(NoStoredResult):
        client.fetch_page(EXEC_ID, limit=1)


def test_fetch_rows_turns_a_missing_result_into_an_empty_page(make_client):
    client = make_client(lambda request: json_response({"detail": "no stored result"}, status=404))
    page = client.fetch_rows(EXEC_ID, limit=10)

    assert page.rows == []
    assert page.total == 0


def test_cancel_is_best_effort(make_client):
    client = make_client(lambda request: json_response({"detail": "already done"}, status=409))
    client.cancel(EXEC_ID)  # does not raise


def test_cancel_keeps_authentication_failures_visible(make_client):
    client = make_client(lambda request: json_response({"detail": "invalid token"}, status=401))
    with pytest.raises(ThunderduckAuthError):
        client.cancel(EXEC_ID)


def test_auth_failure_never_echoes_the_token(make_client):
    client = make_client(lambda request: json_response({"detail": "invalid token"}, status=401))
    with pytest.raises(ThunderduckAuthError) as caught:
        client.submit("select 1")
    assert TOKEN not in str(caught.value)


def test_download_url_is_returned_verbatim(make_client):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/executions/{EXEC_ID}/download-url"
        return json_response({"url": "https://s3.example/presigned", "expires_in": 900})

    assert make_client(handler).download_url(EXEC_ID)["url"] == "https://s3.example/presigned"


def test_download_does_not_send_the_bearer_token(make_client):
    def handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        assert str(request.url) == "https://s3.example/presigned?signature=x"
        return httpx.Response(200, content=b"PAR1")

    client = make_client(handler)
    assert client.download_bytes("https://s3.example/presigned?signature=x") == b"PAR1"


def test_a_non_json_success_body_is_an_error(make_client):
    client = make_client(lambda request: httpx.Response(200, text="<html>proxy</html>"))
    with pytest.raises(ThunderduckError, match="non-JSON"):
        client.submit("select 1")


def test_an_unreachable_api_names_the_endpoint_but_not_the_token(make_client):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = make_client(handler)
    with pytest.raises(ThunderduckError) as caught:
        client.submit("select 1")
    message = str(caught.value)
    assert BASE_URL in message
    assert TOKEN not in message


def test_the_client_is_a_context_manager(make_client):
    client: ThunderduckClient = make_client(lambda request: json_response({}))
    with client as entered:
        assert entered is client
