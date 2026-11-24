#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import time

import pytest
from requests.exceptions import HTTPError

from airbyte_cdk.sources.declarative.auth.token import (
    LegacySessionTokenAuthenticator,
    get_new_session_token,
)

parameters = {"hello": "world"}
instance_api_url = "https://airbyte.metabaseapp.com/api/"
username = "username"
password = "password"
session_token = "session_token"
header = "X-App-Session"
session_token_response_key = "id"
login_url = "session"
validate_session_url = "user/current"

input_instance_api_url = "{{ config['instance_api_url'] }}"
input_username = "{{ config['username'] }}"
input_password = "{{ config['password'] }}"
input_session_token = "{{ config['session_token'] }}"

config = {
    "instance_api_url": instance_api_url,
    "username": username,
    "password": password,
    "session_token": session_token,
    "header": header,
    "session_token_response_key": session_token_response_key,
    "login_url": login_url,
    "validate_session_url": validate_session_url,
}

config_session_token = {
    "instance_api_url": instance_api_url,
    "username": "",
    "password": "",
    "session_token": session_token,
    "header": header,
    "session_token_response_key": session_token_response_key,
    "login_url": login_url,
    "validate_session_url": validate_session_url,
}

config_username_password = {
    "instance_api_url": instance_api_url,
    "username": username,
    "password": password,
    "session_token": "",
    "header": header,
    "session_token_response_key": session_token_response_key,
    "login_url": login_url,
    "validate_session_url": validate_session_url,
}


def test_auth_header():
    auth_header = LegacySessionTokenAuthenticator(
        config=config,
        parameters=parameters,
        api_url=input_instance_api_url,
        username=input_username,
        password=input_password,
        session_token=input_session_token,
        header=header,
        session_token_response_key=session_token_response_key,
        login_url=login_url,
        validate_session_url=validate_session_url,
    ).auth_header
    assert auth_header == "X-App-Session"


def test_get_token_valid_session(requests_mock):
    requests_mock.get(
        f"{config_session_token['instance_api_url']}user/current",
        json={"common_name": "common_name", "last_login": "last_login"},
    )

    token = LegacySessionTokenAuthenticator(
        config=config_session_token,
        parameters=parameters,
        api_url=input_instance_api_url,
        username=input_username,
        password=input_password,
        session_token=input_session_token,
        header=header,
        session_token_response_key=session_token_response_key,
        login_url=login_url,
        validate_session_url=validate_session_url,
    ).token
    assert token == "session_token"


def test_get_token_invalid_session_unauthorized():
    with pytest.raises(ConnectionError):
        _ = LegacySessionTokenAuthenticator(
            config=config_session_token,
            parameters=parameters,
            api_url=input_instance_api_url,
            username=input_username,
            password=input_password,
            session_token=input_session_token,
            header=header,
            session_token_response_key=session_token_response_key,
            login_url=login_url,
            validate_session_url=validate_session_url,
        ).token


def test_get_token_invalid_username_password_unauthorized():
    with pytest.raises(HTTPError):
        _ = LegacySessionTokenAuthenticator(
            config=config_username_password,
            parameters=parameters,
            api_url=input_instance_api_url,
            username=input_username,
            password=input_password,
            session_token=input_session_token,
            header=header,
            session_token_response_key=session_token_response_key,
            validate_session_url=validate_session_url,
            login_url=login_url,
        ).token


def test_get_token_username_password(requests_mock):
    requests_mock.post(f"{config['instance_api_url']}session", json={"id": "some session id"})

    token = LegacySessionTokenAuthenticator(
        config=config_username_password,
        parameters=parameters,
        api_url=input_instance_api_url,
        username=input_username,
        password=input_password,
        session_token=input_session_token,
        header=header,
        session_token_response_key=session_token_response_key,
        login_url=login_url,
        validate_session_url=validate_session_url,
    ).token
    assert token == "some session id"


def test_check_is_valid_session_token(requests_mock):
    requests_mock.get(
        f"{config['instance_api_url']}user/current",
        json={"common_name": "common_name", "last_login": "last_login"},
    )

    assert LegacySessionTokenAuthenticator(
        config=config,
        parameters=parameters,
        api_url=input_instance_api_url,
        username=input_username,
        password=input_password,
        session_token=input_session_token,
        header=header,
        session_token_response_key=session_token_response_key,
        validate_session_url=validate_session_url,
        login_url=login_url,
    ).is_valid_session_token()


def test_check_is_valid_session_token_unauthorized():
    assert not LegacySessionTokenAuthenticator(
        config=config,
        parameters=parameters,
        api_url=input_instance_api_url,
        username=input_username,
        password=input_password,
        session_token=input_session_token,
        header=header,
        session_token_response_key=session_token_response_key,
        login_url=login_url,
        validate_session_url=validate_session_url,
    ).is_valid_session_token()


def test_get_new_session_token(requests_mock):
    requests_mock.post(
        f"{config['instance_api_url']}session",
        headers={"Content-Type": "application/json"},
        json={"id": "some session id"},
    )

    session_token = get_new_session_token(
        f"{config['instance_api_url']}session",
        config["username"],
        config["password"],
        config["session_token_response_key"],
    )
    assert session_token == "some session id"


def _legacy_authenticator(**kwargs):
    return LegacySessionTokenAuthenticator(
        config=config_username_password,
        parameters=parameters,
        api_url=input_instance_api_url,
        username=input_username,
        password=input_password,
        session_token=input_session_token,
        header=header,
        session_token_response_key=session_token_response_key,
        login_url=login_url,
        validate_session_url=validate_session_url,
        **kwargs,
    )


def test_session_token_requests_are_bounded_by_the_request_deadline(requests_mock):
    get_new_session_token.cache_clear()
    requests_mock.post(f"{config['instance_api_url']}session", json={"id": "some session id"})

    token = _legacy_authenticator(request_deadline=time.monotonic() + 30).token

    assert token == "some session id"
    connect_timeout, read_timeout = requests_mock.request_history[0].timeout
    assert 0 < connect_timeout
    assert 0 < read_timeout
    assert connect_timeout + read_timeout <= 30


def test_session_token_request_is_not_sent_after_the_request_deadline(requests_mock):
    get_new_session_token.cache_clear()
    requests_mock.post(f"{config['instance_api_url']}session", json={"id": "some session id"})

    with pytest.raises(ConnectionError, match="ran out of time"):
        _ = _legacy_authenticator(request_deadline=time.monotonic() - 1).token

    assert requests_mock.call_count == 0


def test_session_token_cache_ignores_the_timeout(requests_mock):
    get_new_session_token.cache_clear()
    requests_mock.post(f"{config['instance_api_url']}session", json={"id": "some session id"})
    url = f"{config['instance_api_url']}session"

    get_new_session_token(url, username, password, session_token_response_key, timeout=(5, 5))
    get_new_session_token(url, username, password, session_token_response_key, timeout=(1, 1))

    assert requests_mock.call_count == 1
