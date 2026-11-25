"""Trigger that watches one execution until it reaches a terminal status."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
from airflow.triggers.base import BaseTrigger, TriggerEvent

from ..client import HTTP_TIMEOUT, auth_headers, check_response
from ..exceptions import ThunderduckError
from ..hooks.thunderduck import ThunderduckHook

log = logging.getLogger(__name__)


class ThunderduckExecutionTrigger(BaseTrigger):
    """Poll ``GET /executions/{id}`` until the run finishes or times out.

    The token is never put into the serialized trigger: only the connection id
    travels through the metadata database, and the triggerer resolves the
    connection itself.
    """

    def __init__(
        self,
        *,
        execution_id: str,
        thunderduck_conn_id: str,
        poll_interval: float = 5.0,
        timeout: float = 3600.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__()
        self.execution_id = execution_id
        self.thunderduck_conn_id = thunderduck_conn_id
        self.poll_interval = poll_interval
        self.timeout = timeout
        #: Injected in tests; the triggerer always uses the real network.
        self._transport = transport

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "thunderduck_airflow.triggers.execution.ThunderduckExecutionTrigger",
            {
                "execution_id": self.execution_id,
                "thunderduck_conn_id": self.thunderduck_conn_id,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            base_url, token = await asyncio.to_thread(self._connection_config)
        except ThunderduckError as err:
            yield self._event("error", str(err))
            return

        deadline = time.monotonic() + self.timeout
        async with httpx.AsyncClient(
            base_url=base_url,
            headers=auth_headers(token),
            timeout=httpx.Timeout(HTTP_TIMEOUT),
            transport=self._transport,
        ) as http:
            while True:
                try:
                    response = await http.get(f"/executions/{self.execution_id}")
                    payload = check_response(response)
                except httpx.HTTPError as err:
                    # A blip should not lose a run that is still executing:
                    # keep polling until the deadline.
                    if time.monotonic() >= deadline:
                        yield self._event("timeout", self._timeout_message("unreachable"))
                        return
                    log.warning(
                        "could not poll execution %s (%s); retrying",
                        self.execution_id,
                        type(err).__name__,
                    )
                    await asyncio.sleep(self.poll_interval)
                    continue
                except ThunderduckError as err:
                    yield self._event("error", str(err))
                    return

                status = str(payload.get("status", ""))
                if status == "success":
                    yield self._event("success", row_count=payload.get("result_row_count"))
                    return
                if status == "error":
                    yield self._event("error", payload.get("error_message") or "query failed")
                    return
                if status == "cancelled":
                    yield self._event("cancelled", "the execution was cancelled")
                    return
                if time.monotonic() >= deadline:
                    yield self._event("timeout", self._timeout_message(status))
                    return
                await asyncio.sleep(self.poll_interval)

    # ---- internals ----

    def _connection_config(self) -> tuple[str, str]:
        return ThunderduckHook(self.thunderduck_conn_id).connection_config()

    def _timeout_message(self, status: str) -> str:
        return f"execution {self.execution_id} was still {status!r} after {self.timeout:g}s"

    def _event(
        self, status: str, message: str | None = None, row_count: Any = None
    ) -> TriggerEvent:
        payload: dict[str, Any] = {"status": status, "execution_id": self.execution_id}
        if message is not None:
            payload["message"] = message
        if row_count is not None:
            payload["row_count"] = row_count
        return TriggerEvent(payload)


__all__ = ["ThunderduckExecutionTrigger"]
