"""Apache Airflow provider for Thunderduck.

Submits SQL to the Thunderduck REST API, waits for the stored result and hands
back an execution id — synchronously on a worker or deferrably on a triggerer.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("thunderduck-airflow")
except PackageNotFoundError:  # running from a source tree without metadata
    __version__ = "0.0.0"

__all__ = ["__version__"]
