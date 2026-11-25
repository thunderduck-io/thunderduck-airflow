"""HTTP transport for the Thunderduck REST API.

This is the only module that speaks HTTP. It knows nothing about Airflow: it
submits SQL, reports execution state, pages results, cancels runs and hands out
the Parquet download URL. The Java JDBC driver, ``thunderduck-sqlalchemy`` and
the Node client implement the same protocol.

Two properties are deliberate:

* **Polling, not streaming.** ``POST /queries`` returns immediately with an
  execution id; the run happens in a Kubernetes Job. ``wait`` polls
  ``GET /executions/{id}`` until a terminal status.
* **The token never appears in a URL or an error message.** It travels only in
  the ``Authorization`` header, and error construction never interpolates it.
  Download URLs are presigned and fetched by a separate, header-free client.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx

from .exceptions import (
    NoStoredResult,
    ThunderduckAuthError,
    ThunderduckError,
    ThunderduckQueryError,
    ThunderduckTimeout,
)

#: Statuses that mean the run is over, one way or another.
TERMINAL_STATUSES = frozenset({"success", "error", "cancelled"})

#: How long a single HTTP round trip may take. Waiting for a *run* is bounded
#: separately by ``wait(timeout=...)``.
HTTP_TIMEOUT = 30.0


@dataclass(frozen=True)
class Execution:
    """State of one query run."""

    uuid: str
    status: str
    error_message: str | None = None
    result_location: str | None = None
    result_row_count: int | None = None


@dataclass(frozen=True)
class ResultPage:
    """One page of a stored result set."""

    columns: list[str]
    column_types: list[str]
    rows: list[list[Any]]
    total: int
    offset: int


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def check_response(
    response: httpx.Response, *, no_result_is_signal: bool = False
) -> dict[str, Any]:
    """Decode a response, or raise the matching provider error.

    Shared by the synchronous client and the triggerer's async poll so both
    map status codes onto the same exceptions.
    """
    if response.is_success:
        if not response.content:
            return {}
        try:
            payload = response.json()
        except ValueError:
            raise ThunderduckError("the Thunderduck API returned a non-JSON success body") from None
        return payload if isinstance(payload, dict) else {"data": payload}

    detail = _detail(response)
    status = response.status_code
    if status in (401, 403):
        raise ThunderduckAuthError(
            f"authentication failed; check the token in the connection ({detail})"
        )
    if status == 404 and no_result_is_signal:
        raise NoStoredResult(detail)
    if status == 404:
        raise ThunderduckError(f"not found: {detail}")
    if status == 409:
        raise ThunderduckError(f"conflict: {detail}")
    if status in (400, 422):
        raise ThunderduckError(f"rejected: {detail}")
    raise ThunderduckError(f"the Thunderduck API returned HTTP {status}: {detail}")


def _detail(response: httpx.Response) -> str:
    """FastAPI puts the human-readable message in a JSON ``detail`` field."""
    try:
        body = response.json()
    except ValueError:
        return response.text.strip()[:200] or f"HTTP {response.status_code}"
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])
    return str(body)[:200]


class ThunderduckClient:
    """Synchronous client, one per task (the hook owns it)."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        poll_interval: float = 2.0,
        timeout: float = 3600.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._transport = transport
        self._http = httpx.Client(
            base_url=self._base_url,
            transport=transport,
            timeout=httpx.Timeout(HTTP_TIMEOUT),
            headers=auth_headers(token),
        )

    # ---- public API ----

    def submit(self, sql: str) -> str:
        payload = self._request("POST", "/queries", json={"sql": sql})
        exec_id = payload.get("id")
        if not exec_id:
            raise ThunderduckError("Thunderduck did not return an execution id")
        return str(exec_id)

    def get_execution(self, exec_id: str) -> Execution:
        return _execution(self._request("GET", f"/executions/{exec_id}"), exec_id)

    def wait(
        self,
        exec_id: str,
        *,
        timeout: float | None = None,
        poll_interval: float | None = None,
    ) -> Execution:
        """Poll until the run reaches a terminal status.

        Returns only a successful execution: ``error`` and ``cancelled`` raise,
        so a caller never mistakes a failed run for an empty result.
        """
        deadline = time.monotonic() + (self._timeout if timeout is None else timeout)
        interval = self._poll_interval if poll_interval is None else poll_interval
        while True:
            execution = self.get_execution(exec_id)
            if execution.status in TERMINAL_STATUSES:
                if execution.status == "error":
                    raise ThunderduckQueryError(
                        execution.error_message or "query failed without a message"
                    )
                if execution.status == "cancelled":
                    raise ThunderduckError(f"execution {exec_id} was cancelled")
                return execution
            if time.monotonic() >= deadline:
                raise ThunderduckTimeout(
                    f"execution {exec_id} was still {execution.status!r} after "
                    f"{timeout if timeout is not None else self._timeout:g}s"
                )
            if interval:
                time.sleep(interval)

    def fetch_page(self, exec_id: str, limit: int, offset: int = 0) -> ResultPage:
        payload = self._request(
            "GET",
            f"/executions/{exec_id}/result",
            params={"limit": limit, "offset": offset},
            no_result_is_signal=True,
        )
        return ResultPage(
            columns=list(payload.get("columns") or []),
            column_types=list(payload.get("column_types") or []),
            rows=[list(row) for row in (payload.get("rows") or [])],
            total=int(payload.get("total") or 0),
            offset=int(payload.get("offset") or offset),
        )

    def fetch_rows(self, exec_id: str, limit: int) -> ResultPage:
        """First ``limit`` rows, or an empty page when nothing was stored."""
        try:
            return self.fetch_page(exec_id, limit=limit)
        except NoStoredResult:
            return ResultPage(columns=[], column_types=[], rows=[], total=0, offset=0)

    def cancel(self, exec_id: str) -> None:
        """Best-effort cancel; a finished or unknown run is not an error.

        "Best effort" covers the run's *state*, not the credentials: a rejected
        token must stay visible, otherwise a stop button would look like it
        worked while quietly hiding that the token is wrong.
        """
        try:
            self._request("POST", f"/executions/{exec_id}/cancel")
        except ThunderduckAuthError:
            raise
        except ThunderduckError:
            pass

    def catalog_schema(self) -> dict[str, Any]:
        return self._request("GET", "/catalog/schema")

    def download_url(self, exec_id: str) -> dict[str, Any]:
        return self._request("GET", f"/executions/{exec_id}/download-url")

    def download_bytes(self, url: str) -> bytes:
        """GET a presigned URL without the bearer token.

        A presigned URL is self-authenticating through its query string, so it
        must never carry our own ``Authorization`` header.
        """
        with httpx.Client(transport=self._transport, timeout=httpx.Timeout(HTTP_TIMEOUT)) as http:
            try:
                response = http.get(url)
            except httpx.HTTPError as err:
                # The URL carries a signature; keep it out of the message.
                raise ThunderduckError(
                    f"could not download the result: {type(err).__name__}"
                ) from None
            if not response.is_success:
                raise ThunderduckError(
                    f"could not download the result: HTTP {response.status_code}"
                )
            return response.content

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> ThunderduckClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ---- internals ----

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Any = None,
        no_result_is_signal: bool = False,
    ) -> dict[str, Any]:
        try:
            response = self._http.request(method, path, json=json, params=params)
        except httpx.HTTPError as err:
            # Never chain: httpx puts the full URL in its message.
            raise ThunderduckError(
                f"could not reach Thunderduck at {self._base_url}: {type(err).__name__}"
            ) from None
        return check_response(response, no_result_is_signal=no_result_is_signal)


def _execution(payload: dict[str, Any], exec_id: str) -> Execution:
    return Execution(
        uuid=str(payload.get("uuid", exec_id)),
        status=str(payload.get("status", "")),
        error_message=payload.get("error_message"),
        result_location=payload.get("result_location"),
        result_row_count=payload.get("result_row_count"),
    )
