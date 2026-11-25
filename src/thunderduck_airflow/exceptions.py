"""Exceptions raised by the provider.

Everything derives from ``AirflowException`` so a failure surfaces as a normal
task failure carrying the API's own message, instead of a parallel hierarchy
that DAG authors would have to catch separately.
"""

from __future__ import annotations

from airflow.exceptions import AirflowException


class ThunderduckError(AirflowException):
    """Base class for every Thunderduck failure."""


class ThunderduckAuthError(ThunderduckError):
    """The API rejected the token (HTTP 401/403).

    Kept apart from the other errors because it is a connection problem, not a
    query problem: retrying the task will not help, and the message must be
    unmistakable in the task log.
    """


class ThunderduckQueryError(ThunderduckError):
    """The query was submitted and finished in the ``error`` state."""


class ThunderduckTimeout(ThunderduckError):
    """The run did not reach a terminal status before the deadline."""


class NoStoredResult(Exception):
    """The execution stored no result set, e.g. an INSERT or a DDL statement.

    Not a :class:`ThunderduckError`: callers that only want rows treat it as an
    empty result, not as a task failure.
    """
