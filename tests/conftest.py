"""Test setup.

``AIRFLOW_HOME`` has to be set *before* Airflow is imported, otherwise the
config module creates ``~/airflow`` as a side effect of the first test import.
"""

from __future__ import annotations

import os
import tempfile

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="thunderduck-airflow-tests-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

import httpx
import pytest
from airflow.sdk import Connection

from thunderduck_airflow.client import ThunderduckClient
from thunderduck_airflow.hooks.thunderduck import ThunderduckHook

CONN_ID = "thunderduck_default"
BASE_URL = "https://api.thunderduck.test"
TOKEN = "tdk_test_token_not_real"
EXEC_ID = "11111111-2222-3333-4444-555555555555"


def json_response(payload: dict, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


@pytest.fixture
def make_connection(monkeypatch):
    """Install a connection the hook resolves without a metadata database."""

    def _make(**kwargs) -> Connection:
        kwargs.setdefault("conn_type", "thunderduck")
        conn = Connection(conn_id=CONN_ID, **kwargs)
        monkeypatch.setattr(ThunderduckHook, "get_connection", staticmethod(lambda conn_id: conn))
        return conn

    return _make


@pytest.fixture
def make_client():
    """A client wired to a loopback handler instead of the network."""
    created: list[ThunderduckClient] = []

    def _make(handler, **kwargs) -> ThunderduckClient:
        kwargs.setdefault("poll_interval", 0)
        client = ThunderduckClient(
            BASE_URL, TOKEN, transport=httpx.MockTransport(handler), **kwargs
        )
        created.append(client)
        return client

    yield _make
    for client in created:
        client.close()
