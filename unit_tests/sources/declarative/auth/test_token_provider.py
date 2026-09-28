#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import json
from unittest.mock import MagicMock

import freezegun
import pytest
from isodate import parse_duration

from airbyte_cdk.sources.declarative.auth.token_provider import (
    InterpolatedSessionTokenProvider,
    InterpolatedStringTokenProvider,
    SessionTokenProvider,
)
from airbyte_cdk.sources.declarative.exceptions import ReadException


def create_session_token_provider():
    login_requester = MagicMock()
    login_response = MagicMock()
    login_response.content = json.dumps({"nested": {"token": "my_token"}}).encode()
    login_requester.send_request.return_value = login_response

    return SessionTokenProvider(
        login_requester=login_requester,
        session_token_path=["nested", "token"],
        expiration_duration=parse_duration("PT1H"),
        parameters={"test": "test"},
    )


def test_interpolated_string_token_provider():
    provider = InterpolatedStringTokenProvider(
        config={"config_key": "val"},
        api_token="{{ config.config_key }}-{{ parameters.test }}",
        parameters={"test": "test"},
    )
    assert provider.get_token() == "val-test"


def test_session_token_provider():
    provider = create_session_token_provider()
    assert provider.get_token() == "my_token"


def test_session_token_provider_cache():
    provider = create_session_token_provider()
    provider.get_token()
    assert provider.get_token() == "my_token"
    assert provider.login_requester.send_request.call_count == 1


def test_session_token_provider_cache_expiration():
    with freezegun.freeze_time("2001-05-21T12:00:00Z"):
        provider = create_session_token_provider()
        provider.get_token()

    provider.login_requester.send_request.return_value.content = json.dumps(
        {"nested": {"token": "updated_token"}}
    ).encode()

    with freezegun.freeze_time("2001-05-21T14:00:00Z"):
        assert provider.get_token() == "updated_token"

    assert provider.login_requester.send_request.call_count == 2


def test_session_token_provider_no_cache():
    provider = create_session_token_provider()
    provider.expiration_duration = None
    provider.get_token()
    assert provider.login_requester.send_request.call_count == 1
    provider.get_token()
    assert provider.login_requester.send_request.call_count == 2


def test_session_token_provider_ignored_response():
    provider = create_session_token_provider()
    provider.login_requester.send_request.return_value = None
    with pytest.raises(ReadException):
        provider.get_token()


@pytest.mark.parametrize(
    "api_token_template,expected_token",
    [
        pytest.param("Token {{ session_token }}", "Token my_token", id="token_prefix"),
        pytest.param("Bearer {{ session_token }}", "Bearer my_token", id="bearer_prefix"),
        pytest.param("{{ session_token }}", "my_token", id="just_session_token"),
        pytest.param("Custom-{{ session_token }}", "Custom-my_token", id="custom_prefix"),
        pytest.param(
            "realm=xyz, token={{ session_token }}",
            "realm=xyz, token=my_token",
            id="complex_format",
        ),
    ],
)
def test_interpolated_session_token_provider(api_token_template, expected_token):
    """Test that InterpolatedSessionTokenProvider correctly interpolates session token."""
    underlying_provider = create_session_token_provider()
    interpolated_provider = InterpolatedSessionTokenProvider(
        config={"some_config": "value"},
        api_token=api_token_template,
        session_token_provider=underlying_provider,
        parameters={},
    )
    assert interpolated_provider.get_token() == expected_token
