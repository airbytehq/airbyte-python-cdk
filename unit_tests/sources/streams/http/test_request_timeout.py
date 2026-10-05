# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from airbyte_cdk.sources.streams.http import request_timeout
from airbyte_cdk.sources.streams.http.request_timeout import (
    connect_only_request_timeout,
    default_request_timeout,
)


def test_request_timeout_defaults():
    assert default_request_timeout() == (30.0, 300.0)
    assert connect_only_request_timeout() == (30.0, None)


def test_connect_only_request_timeout_follows_default_connect_timeout(monkeypatch):
    monkeypatch.setattr(request_timeout, "DEFAULT_CONNECT_TIMEOUT_SECONDS", 7.0)

    assert connect_only_request_timeout() == (7.0, None)
