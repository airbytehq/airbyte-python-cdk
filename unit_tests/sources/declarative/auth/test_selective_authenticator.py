#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.auth.selective_authenticator import SelectiveAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


def test_authenticator_selected(mocker):
    authenticators = {"one": mocker.Mock(), "two": mocker.Mock()}
    auth = SelectiveAuthenticator(
        config={"auth": {"type": "one"}},
        authenticators=authenticators,
        authenticator_selection_path=["auth", "type"],
    )

    assert auth is authenticators["one"]


def test_selection_path_not_found(mocker):
    authenticators = {"one": mocker.Mock(), "two": mocker.Mock()}

    with pytest.raises(
        AirbyteTracedException,
        match='Required field "auth_type" is missing from connector configuration',
    ) as exc_info:
        _ = SelectiveAuthenticator(
            config={"auth": {"type": "one"}},
            authenticators=authenticators,
            authenticator_selection_path=["auth_type"],
        )

    assert exc_info.value.failure_type == FailureType.config_error


def test_selected_auth_not_found(mocker):
    authenticators = {"one": mocker.Mock(), "two": mocker.Mock()}

    with pytest.raises(
        AirbyteTracedException, match='contains unrecognized value "unknown"'
    ) as exc_info:
        _ = SelectiveAuthenticator(
            config={"auth": {"type": "unknown"}},
            authenticators=authenticators,
            authenticator_selection_path=["auth", "type"],
        )

    assert exc_info.value.failure_type == FailureType.config_error


def test_selection_path_not_found_includes_dotted_path(mocker):
    authenticators = {"one": mocker.Mock(), "two": mocker.Mock()}

    with pytest.raises(
        AirbyteTracedException, match='Required field "credentials.auth_type" is missing'
    ) as exc_info:
        _ = SelectiveAuthenticator(
            config={"credentials": {}},
            authenticators=authenticators,
            authenticator_selection_path=["credentials", "auth_type"],
        )

    assert exc_info.value.failure_type == FailureType.config_error


def test_selected_auth_not_found_lists_valid_options(mocker):
    authenticators = {"Client": mocker.Mock(), "Service": mocker.Mock()}

    with pytest.raises(AirbyteTracedException) as exc_info:
        _ = SelectiveAuthenticator(
            config={"credentials": {"auth_type": "BadValue"}},
            authenticators=authenticators,
            authenticator_selection_path=["credentials", "auth_type"],
        )

    assert exc_info.value.failure_type == FailureType.config_error
    assert "BadValue" in str(exc_info.value.message)
    assert "Client" in str(exc_info.value.internal_message)
    assert "Service" in str(exc_info.value.internal_message)
