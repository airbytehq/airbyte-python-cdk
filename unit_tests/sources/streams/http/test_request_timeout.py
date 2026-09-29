# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from airbyte_cdk.sources.streams.http.request_timeout import (
    ENV_HTTP_CONNECT_TIMEOUT_SECONDS,
    connect_only_request_timeout,
)


def test_connect_only_request_timeout_defaults(monkeypatch):
    monkeypatch.delenv(ENV_HTTP_CONNECT_TIMEOUT_SECONDS, raising=False)

    assert connect_only_request_timeout() == (30.0, None)


def test_connect_only_request_timeout_honours_env_var(monkeypatch):
    monkeypatch.setenv(ENV_HTTP_CONNECT_TIMEOUT_SECONDS, "7")

    assert connect_only_request_timeout() == (7.0, None)
