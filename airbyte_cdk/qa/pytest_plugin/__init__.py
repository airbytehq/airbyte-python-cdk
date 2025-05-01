"""Pytest plugin for running QA checks on connectors."""

from airbyte_cdk.qa.pytest_plugin.plugin import (
    connector,
    pytest_addoption,
    pytest_collection_modifyitems,
    pytest_configure,
    pytest_sessionfinish,
)

__all__ = [
    "connector",
    "pytest_addoption",
    "pytest_collection_modifyitems",
    "pytest_configure",
    "pytest_sessionfinish",
]
