"""Global pytest configuration for the Airbyte CDK tests."""

from pathlib import Path
from typing import cast

import pytest


def pytest_collect_file(parent: pytest.Module | None, path: Path) -> pytest.Module | None:
    """Collect test files based on their names."""
    if path.name == "test_connector.py":
        return cast(pytest.Module, pytest.Module.from_parent(parent, path=path))

    return None


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "connector: mark test as a connector test")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-connector",
        action="store_true",
        default=False,
        help="run connector tests",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-connector"):
        return
    skip_connector = pytest.mark.skip(reason="need --run-connector option to run")
    for item in items:
        if "connector" in item.keywords:
            item.add_marker(skip_connector)


def pytest_runtest_setup(item: pytest.Item) -> None:
    # This hook is called before each test function is executed
    print(f"Setting up test: {item.name}")


def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:
    # This hook is called after each test function is executed
    print(f"Tearing down test: {item.name}")
