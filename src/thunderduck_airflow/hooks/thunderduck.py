"""Hook for the Thunderduck REST API.

One connection type, ``thunderduck``, carries everything the operators need:

* **Host** — the API endpoint, with or without ``https://`` (``api.thunderduck.ai``).
* **Password** — the API token. Airflow masks it in logs and the UI.
* **Extra** — optional ``endpoint`` (a full URL that wins over host/port),
  ``timeout`` (seconds to wait for a run, default 3600), ``poll_interval``
  (seconds between status checks, default 2) and ``token`` if the token is
  stored in extras instead of the password field.
"""

from __future__ import annotations

from typing import Any

from ..client import Execution, ResultPage, ThunderduckClient
from ..compat import BaseHook
from ..exceptions import ThunderduckError

DEFAULT_CONN_ID = "thunderduck_default"


class ThunderduckHook(BaseHook):
    """Thin wrapper that turns a connection into a REST client."""

    conn_name_attr = "thunderduck_conn_id"
    default_conn_name = DEFAULT_CONN_ID
    conn_type = "thunderduck"
    hook_name = "Thunderduck"

    def __init__(self, thunderduck_conn_id: str = DEFAULT_CONN_ID) -> None:
        super().__init__()
        self.thunderduck_conn_id = thunderduck_conn_id

    # ---- connection ----

    def get_conn(self) -> ThunderduckClient:
        """Build a client from the connection; the caller owns (and closes) it."""
        conn = self.get_connection(self.thunderduck_conn_id)
        extra = conn.extra_dejson or {}
        return ThunderduckClient(
            base_url=_base_url(conn),
            token=_token(conn, extra),
            poll_interval=float(extra.get("poll_interval", 2.0)),
            timeout=float(extra.get("timeout", 3600.0)),
        )

    def connection_config(self) -> tuple[str, str]:
        """``(base_url, token)`` without opening a client.

        The triggerer runs in its own process, where building a full client is
        pointless; it needs exactly these two values.
        """
        conn = self.get_connection(self.thunderduck_conn_id)
        return _base_url(conn), _token(conn, conn.extra_dejson or {})

    def test_connection(self) -> tuple[bool, str]:
        """Backs the UI's "Test connection" button: one authenticated GET."""
        try:
            with self.get_conn() as client:
                client.catalog_schema()
        except ThunderduckError as err:
            return False, str(err)
        return True, "Connected to the Thunderduck API."

    # ---- convenience, for DAGs that only need one call ----

    def submit(self, sql: str) -> str:
        with self.get_conn() as client:
            return client.submit(sql)

    def wait(self, exec_id: str, *, timeout: float | None = None) -> Execution:
        with self.get_conn() as client:
            return client.wait(exec_id, timeout=timeout)

    def fetch_rows(self, exec_id: str, limit: int) -> ResultPage:
        with self.get_conn() as client:
            return client.fetch_rows(exec_id, limit=limit)


def _base_url(conn: Any) -> str:
    extra = conn.extra_dejson or {}
    endpoint = extra.get("endpoint")
    if endpoint:
        return str(endpoint).rstrip("/")

    host = (conn.host or "").strip()
    if not host:
        raise ThunderduckError(
            f"connection {conn.conn_id!r} has no host: set it to the API endpoint, "
            "for example api.thunderduck.ai"
        )
    base_url = host if "://" in host else f"https://{host}"
    if conn.port and ":" not in base_url.split("://", 1)[-1]:
        base_url = f"{base_url}:{conn.port}"
    return base_url.rstrip("/")


def _token(conn: Any, extra: dict[str, Any]) -> str:
    token = conn.password or extra.get("token") or ""
    if isinstance(token, dict):
        # A secret-backend reference may arrive as {"value": ...} depending on
        # where the connection came from.
        token = token.get("value", "")
    if not token:
        raise ThunderduckError(
            f"connection {conn.conn_id!r} has no token: put the API token in the "
            "password field (or in extras as 'token')"
        )
    return str(token)
