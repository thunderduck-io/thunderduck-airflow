"""Provider metadata: the parts Airflow reads at startup."""

from __future__ import annotations

import importlib
from importlib.metadata import entry_points, version
from importlib.resources import files

from thunderduck_airflow import __version__
from thunderduck_airflow.compat import BaseHook
from thunderduck_airflow.hooks.thunderduck import ThunderduckHook
from thunderduck_airflow.provider import get_provider_info

INFO = get_provider_info()


def test_provider_info_names_the_package_and_connection_type():
    assert INFO["package-name"] == "thunderduck-airflow"
    assert INFO["name"] == "Thunderduck"
    (connection_type,) = INFO["connection-types"]
    assert connection_type["connection-type"] == "thunderduck"
    assert connection_type["hook-name"] == "Thunderduck"
    assert (
        connection_type["hook-class-name"]
        == "thunderduck_airflow.hooks.thunderduck.ThunderduckHook"
    )


def test_the_declared_hook_matches_the_class():
    assert ThunderduckHook.conn_type == "thunderduck"
    assert ThunderduckHook.conn_name_attr == "thunderduck_conn_id"
    assert ThunderduckHook.default_conn_name == "thunderduck_default"
    assert ThunderduckHook.hook_name == "Thunderduck"
    assert issubclass(ThunderduckHook, BaseHook)


def test_the_version_comes_from_the_installed_metadata():
    assert __version__ == version("thunderduck-airflow")
    assert INFO["versions"][-1] == __version__


def test_the_provider_metadata_file_is_packaged():
    assert files("thunderduck_airflow").joinpath("provider.yaml").is_file()


def test_every_listed_module_imports():
    for section in ("hooks", "operators", "sensors", "triggers"):
        for entry in INFO[section]:
            for module in entry["python-modules"]:
                importlib.import_module(module)


def test_the_entry_point_is_installed():
    points = entry_points(group="apache_airflow_provider")
    assert any(point.value == "thunderduck_airflow.provider:get_provider_info" for point in points)


def test_the_example_dag_parses():
    module = importlib.import_module("thunderduck_airflow.example_dags.example_thunderduck")

    assert module.dag.dag_id == "example_thunderduck"
    assert set(module.dag.task_ids) == {"preview", "daily_rollup"}
    preview = module.dag.get_task("preview")
    assert preview.downstream_task_ids == {"daily_rollup"}
