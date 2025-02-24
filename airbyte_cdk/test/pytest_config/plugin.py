from pathlib import Path

import pytest


def pytest_collect_file(parent, path):
    if path.basename == "test_connector.py":
        return pytest.Module.from_parent(parent, path=path)


def pytest_configure(config):
    config.addinivalue_line("markers", "connector: mark test as a connector test")


def pytest_addoption(parser):
    parser.addoption(
        "--run-connector",
        action="store_true",
        default=False,
        help="run connector tests",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-connector"):
        return
    skip_connector = pytest.mark.skip(reason="need --run-connector option to run")
    for item in items:
        if "connector" in item.keywords:
            item.add_marker(skip_connector)


def pytest_runtest_setup(item):
    # This hook is called before each test function is executed
    print(f"Setting up test: {item.name}")


def pytest_runtest_teardown(item, nextitem):
    # This hook is called after each test function is executed
    print(f"Tearing down test: {item.name}")
