#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime

import freezegun
import pytest

from airbyte_cdk.sources.streams.http.cache_stats import HTTP_CACHE_STATS


@pytest.fixture(autouse=True)
def reset_http_cache_stats():
    """Isolate the process-wide HTTP counters between tests.

    They are module-level by design -- one `requests_cache` backend is shared by
    a whole run -- but a test process is many runs, and a leftover count makes
    `AirbyteEntrypoint.run` emit analytics messages in a test that recorded no
    requests of its own.
    """
    HTTP_CACHE_STATS.reset()
    yield
    HTTP_CACHE_STATS.reset()


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
