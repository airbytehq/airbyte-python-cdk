#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime

import freezegun
import pytest

from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import (
    ENV_VAR_ALLOW_CUSTOM_CODE,
)


@pytest.fixture(autouse=True)
def _allow_custom_code(monkeypatch):
    """Enable custom-component code execution for unit tests by default.

    `ModelToComponentFactory.create_custom_component` requires
    `AIRBYTE_ENABLE_UNSAFE_CODE` to be set before it will resolve and instantiate a
    component's `class_name`, matching the gate already applied to injected
    `components.py` code. The unit tests exercise custom components extensively, so
    enable the flag by default here. Tests that verify the gate itself opt out by
    deleting the env var through their own `monkeypatch` fixture.
    """
    monkeypatch.setenv(ENV_VAR_ALLOW_CUSTOM_CODE, "true")


@pytest.fixture()
def mock_sleep(monkeypatch):
    with freezegun.freeze_time(
        datetime.datetime.now(), ignore=["_pytest.runner", "_pytest.terminal"]
    ) as frozen_datetime:
        monkeypatch.setattr("time.sleep", lambda x: frozen_datetime.tick(x))
        yield


def pytest_addoption(parser):
    parser.addoption("--skipslow", action="store_true", default=False, help="skip slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--skipslow"):
        skip_slow = pytest.mark.skip(
            reason="--skipslow option has been provided and this test is marked as slow"
        )
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)
