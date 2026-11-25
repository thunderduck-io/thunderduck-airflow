"""The one place that knows about Airflow 3 minor-version differences.

Airflow 3.0 does not re-export ``BaseHook`` from ``airflow.sdk``; 3.4 does, and
deprecates ``airflow.hooks.base``. Everything else the provider uses lives at a
stable path across the 3.x line.
"""

from __future__ import annotations

try:
    from airflow.sdk import BaseHook as BaseHook
except ImportError:  # pragma: no cover - exercised on Airflow 3.0 only
    from airflow.hooks.base import BaseHook as BaseHook

__all__ = ["BaseHook"]
