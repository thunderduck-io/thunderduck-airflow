"""Provider metadata for the Airflow provider manager.

The ``apache_airflow_provider`` entry point in ``pyproject.toml`` points here.
Returning the contents of ``provider.yaml`` keeps one source of truth for the
provider name, the connection type and the modules listed in the UI.
"""

from __future__ import annotations

from importlib.resources import files
from typing import Any

import yaml


def get_provider_info() -> dict[str, Any]:
    text = files("thunderduck_airflow").joinpath("provider.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(text)
