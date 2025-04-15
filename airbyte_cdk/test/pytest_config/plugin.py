"""Global pytest configuration for the Airbyte CDK tests."""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        name="markers",
        line="requires_creds: mark test as a test that requires credentials",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-connector"):
        return
    skip_connector = pytest.mark.skip(reason="need --run-connector option to run")
    for item in items:
        if "connector" in item.keywords:
            item.add_marker(skip_connector)
