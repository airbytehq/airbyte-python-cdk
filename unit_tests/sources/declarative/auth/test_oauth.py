#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import base64
import json
import logging
from datetime import timedelta, timezone
from unittest.mock import Mock

import freezegun
import pytest
import requests
from requests import Response

from airbyte_cdk.sources.declarative.auth import DeclarativeOauth2Authenticator
from airbyte_cdk.sources.declarative.auth.jwt import JwtAuthenticator
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils.airbyte_secrets_utils import filter_secrets
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_now, ab_datetime_parse

LOGGER = logging.getLogger(__name__)

resp = Response()

config = {
    "refresh_endpoint": "https://refresh_endpoint.com",
    "client_id": "some_client_id",
    "client_secret": "some_client_secret",
    "token_expiry_date": (ab_datetime_now() - timedelta(days=2)).isoformat(),
    "custom_field": "in_outbound_request",
    "another_field": "exists_in_body",
    "grant_type": "some_grant_type",
    "access_token": "some_access_token",
}
parameters = {"refresh_token": "some_refresh_token"}


class TestOauth2Authenticator:
    """
    Test class for OAuth2Authenticator.
    """

    def test_refresh_request_body(self):
        """
        Request body should match given configuration.
        """
        scopes = ["scope1", "scope2"]
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ parameters['refresh_token'] }}",
            config=config,
            scopes=["scope1", "scope2"],
            token_expiry_date="{{ config['token_expiry_date'] }}",
            refresh_request_body={
                "custom_field": "{{ config['custom_field'] }}",
                "another_field": "{{ config['another_field'] }}",
                "scopes": ["no_override"],
            },
            parameters=parameters,
            grant_type="{{ config['grant_type'] }}",
        )
        body = oauth.build_refresh_request_body()
        expected = {
            "grant_type": "some_grant_type",
            "client_id": "some_client_id",
            "client_secret": "some_client_secret",
            "refresh_token": "some_refresh_token",
            "scopes": scopes,
            "custom_field": "in_outbound_request",
            "another_field": "exists_in_body",
        }
        assert body == expected

    def test_refresh_request_headers(self):
        """
        Request headers should match given configuration.
        """
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ parameters['refresh_token'] }}",
            config=config,
            token_expiry_date="{{ config['token_expiry_date'] }}",
            refresh_request_headers={
                "Authorization": "Basic {{ [config['client_id'], config['client_secret']] | join(':') | base64encode }}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            parameters=parameters,
        )
        headers = oauth.build_refresh_request_headers()
        expected = {
            "Authorization": "Basic c29tZV9jbGllbnRfaWQ6c29tZV9jbGllbnRfc2VjcmV0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        assert headers == expected

        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ parameters['refresh_token'] }}",
            config=config,
            token_expiry_date="{{ config['token_expiry_date'] }}",
            parameters=parameters,
        )
        headers = oauth.build_refresh_request_headers()
        assert headers is None

    def test_refresh_with_encode_config_params(self):
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] | base64encode }}",
            client_secret="{{ config['client_secret'] | base64encode }}",
            config=config,
            parameters={},
            grant_type="client_credentials",
        )
        body = oauth.build_refresh_request_body()
        expected = {
            "grant_type": "client_credentials",
            "client_id": base64.b64encode(config["client_id"].encode("utf-8")).decode(),
            "client_secret": base64.b64encode(config["client_secret"].encode("utf-8")).decode(),
            "refresh_token": None,
        }
        assert body == expected

    def test_refresh_with_decode_config_params(self):
        updated_config_fields = {
            "client_id": base64.b64encode(config["client_id"].encode("utf-8")).decode(),
            "client_secret": base64.b64encode(config["client_secret"].encode("utf-8")).decode(),
        }
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] | base64decode }}",
            client_secret="{{ config['client_secret'] | base64decode }}",
            config=config | updated_config_fields,
            parameters={},
            grant_type="client_credentials",
        )
        body = oauth.build_refresh_request_body()
        expected = {
            "grant_type": "client_credentials",
            "client_id": "some_client_id",
            "client_secret": "some_client_secret",
            "refresh_token": None,
        }
        assert body == expected

    def test_refresh_without_refresh_token(self):
        """
        Should work fine for grant_type client_credentials.
        """
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            config=config,
            parameters={},
            grant_type="client_credentials",
        )
        body = oauth.build_refresh_request_body()
        expected = {
            "grant_type": "client_credentials",
            "client_id": "some_client_id",
            "client_secret": "some_client_secret",
            "refresh_token": None,
        }
        assert body == expected

    def test_get_auth_header_without_refresh_token_and_without_refresh_token_endpoint(self):
        """
        Coverred the case when the `access_token_value` is supplied,
        without `token_refresh_endpoint` or `refresh_token` provided.

        In this case, it's expected to have the `access_token_value` provided to return the permanent `auth header`,
        contains the authentication.
        """
        oauth = DeclarativeOauth2Authenticator(
            access_token_value="{{ config['access_token'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            config=config,
            parameters={},
            grant_type="client_credentials",
        )
        assert oauth.get_auth_header() == {"Authorization": "Bearer some_access_token"}

    def test_error_on_refresh_token_grant_without_refresh_token(self):
        """
        Should throw an error if grant_type refresh_token is configured without refresh_token.
        """
        with pytest.raises(ValueError):
            DeclarativeOauth2Authenticator(
                token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
                client_id="{{ config['client_id'] }}",
                client_secret="{{ config['client_secret'] }}",
                config=config,
                parameters={},
                grant_type="refresh_token",
            )

    def test_refresh_access_token(self, mocker):
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ config['refresh_token'] }}",
            config=config,
            scopes=["scope1", "scope2"],
            token_expiry_date="{{ config['token_expiry_date'] }}",
            refresh_request_body={
                "custom_field": "{{ config['custom_field'] }}",
                "another_field": "{{ config['another_field'] }}",
                "scopes": ["no_override"],
            },
            parameters={},
        )

        resp.status_code = 200
        mocker.patch.object(
            resp, "json", return_value={"access_token": "access_token", "expires_in": 1000}
        )
        mocker.patch.object(requests, "request", side_effect=mock_request, autospec=True)
        token = oauth.refresh_access_token()

        assert ("access_token", 1000) == token

        filtered = filter_secrets("access_token")
        assert filtered == "****"

    def test_refresh_access_token_when_headers_provided(self, mocker):
        expected_headers = {
            "Authorization": "Bearer some_access_token",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ config['refresh_token'] }}",
            config=config,
            scopes=["scope1", "scope2"],
            token_expiry_date="{{ config['token_expiry_date'] }}",
            refresh_request_headers=expected_headers,
            parameters={},
        )

        resp.status_code = 200
        mocker.patch.object(
            resp, "json", return_value={"access_token": "access_token", "expires_in": 1000}
        )
        mocked_request = mocker.patch.object(
            requests, "request", side_effect=mock_request, autospec=True
        )
        token = oauth.refresh_access_token()

        assert ("access_token", 1000) == token

        assert mocked_request.call_args.kwargs["headers"] == expected_headers

    def test_refresh_access_token_missing_access_token(self, mocker):
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ config['refresh_token'] }}",
            config=config,
            scopes=["scope1", "scope2"],
            token_expiry_date="{{ config['token_expiry_date'] }}",
            refresh_request_body={
                "custom_field": "{{ config['custom_field'] }}",
                "another_field": "{{ config['another_field'] }}",
                "scopes": ["no_override"],
            },
            parameters={},
        )

        resp.status_code = 200
        mocker.patch.object(resp, "json", return_value={"expires_in": 1000})
        mocker.patch.object(requests, "request", side_effect=mock_request, autospec=True)
        with pytest.raises(Exception):
            oauth.refresh_access_token()

    @pytest.mark.parametrize(
        "timestamp, expected_date",
        [
            (1640995200, "2022-01-01T00:00:00Z"),
            ("1650758400", "2022-04-24T00:00:00Z"),
        ],
        ids=["timestamp_as_integer", "timestamp_as_integer_inside_string"],
    )
    def test_initialize_declarative_oauth_with_token_expiry_date_as_timestamp(
        self, timestamp, expected_date
    ):
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            token_expiry_date=timestamp,
            access_token_value="some_access_token",
            refresh_token="some_refresh_token",
            config={
                "refresh_endpoint": "refresh_end",
                "client_id": "some_client_id",
                "client_secret": "some_client_secret",
            },
            parameters={},
            grant_type="client_credentials",
        )
        assert isinstance(oauth._token_expiry_date, AirbyteDateTime)
        assert oauth.get_token_expiry_date() == ab_datetime_parse(expected_date)

    def test_given_no_access_token_but_expiry_in_the_future_when_refresh_token_then_fetch_access_token(
        self,
    ) -> None:
        expiry_date = ab_datetime_now().add(timedelta(days=1))
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="https://refresh_endpoint.com/",
            client_id="some_client_id",
            client_secret="some_client_secret",
            token_expiry_date=expiry_date.isoformat(),
            refresh_token="some_refresh_token",
            config={},
            parameters={},
            grant_type="client",
        )

        with HttpMocker() as http_mocker:
            http_mocker.post(
                HttpRequest(
                    url="https://refresh_endpoint.com/",
                    body="grant_type=client&client_id=some_client_id&client_secret=some_client_secret&refresh_token=some_refresh_token",
                ),
                HttpResponse(body=json.dumps({"access_token": "new_access_token"})),
            )
            oauth.get_access_token()

        assert oauth.access_token == "new_access_token"
        assert oauth._token_expiry_date == expiry_date

    @pytest.mark.parametrize(
        "expires_in_response, token_expiry_date_format",
        [
            ("2020-01-02T00:00:00Z", "YYYY-MM-DDTHH:mm:ss[Z]"),
            ("2020-01-02T00:00:00.000000+00:00", "YYYY-MM-DDTHH:mm:ss.SSSSSSZ"),
            ("2020-01-02", "YYYY-MM-DD"),
        ],
        ids=["rfc3339", "iso8601", "simple_date"],
    )
    @freezegun.freeze_time("2020-01-01")
    def test_refresh_access_token_expire_format(
        self, mocker, expires_in_response, token_expiry_date_format
    ):
        next_day = "2020-01-02T00:00:00Z"
        config.update(
            {"token_expiry_date": (ab_datetime_parse(next_day) - timedelta(days=2)).isoformat()}
        )
        message_repository = Mock()
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ config['refresh_token'] }}",
            config=config,
            scopes=["scope1", "scope2"],
            token_expiry_date="{{ config['token_expiry_date'] }}",
            token_expiry_date_format=token_expiry_date_format,
            token_expiry_is_time_of_expiration=True,
            refresh_request_body={
                "custom_field": "{{ config['custom_field'] }}",
                "another_field": "{{ config['another_field'] }}",
                "scopes": ["no_override"],
            },
            message_repository=message_repository,
            parameters={},
        )

        resp.status_code = 200
        mocker.patch.object(
            resp,
            "json",
            return_value={"access_token": "access_token", "expires_in": expires_in_response},
        )
        mocker.patch.object(requests, "request", side_effect=mock_request, autospec=True)
        token = oauth.get_access_token()
        assert "access_token" == token
        assert oauth.get_token_expiry_date() == ab_datetime_parse(next_day)
        assert message_repository.log_message.call_count == 1

    @pytest.mark.parametrize(
        "expires_in_response, next_day, raises",
        [
            (86400, "2020-01-02T00:00:00Z", False),
            (86400.1, "2020-01-02T00:00:00Z", False),
            ("86400", "2020-01-02T00:00:00Z", False),
            ("86400.1", "2020-01-02T00:00:00Z", False),
            ("2020-01-02T00:00:00Z", "2020-01-02T00:00:00Z", True),
        ],
        ids=[
            "time_in_seconds",
            "time_in_seconds_float",
            "time_in_seconds_str",
            "time_in_seconds_str_float",
            "invalid",
        ],
    )
    @freezegun.freeze_time("2020-01-01")
    def test_set_token_expiry_date_no_format(self, mocker, expires_in_response, next_day, raises):
        config.update(
            {"token_expiry_date": (ab_datetime_parse(next_day) - timedelta(days=2)).isoformat()}
        )
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ config['refresh_token'] }}",
            config=config,
            scopes=["scope1", "scope2"],
            refresh_request_body={
                "custom_field": "{{ config['custom_field'] }}",
                "another_field": "{{ config['another_field'] }}",
                "scopes": ["no_override"],
            },
            parameters={},
        )

        resp.status_code = 200
        mocker.patch.object(
            resp,
            "json",
            return_value={"access_token": "access_token", "expires_in": expires_in_response},
        )
        mocker.patch.object(requests, "request", side_effect=mock_request, autospec=True)
        if raises:
            with pytest.raises(ValueError):
                oauth.get_access_token()
        else:
            token = oauth.get_access_token()
            assert "access_token" == token
            assert oauth.get_token_expiry_date() == ab_datetime_parse(next_day)

    def test_profile_assertion(self, mocker):
        with HttpMocker() as http_mocker:
            jwt = JwtAuthenticator(
                config={},
                parameters={},
                secret_key="test",
                algorithm="HS256",
                token_duration=1000,
                typ="JWT",
                iss="iss",
            )

            mocker.patch(
                "airbyte_cdk.sources.declarative.auth.jwt.JwtAuthenticator.token",
                new_callable=lambda: "token",
            )

            oauth = DeclarativeOauth2Authenticator(
                token_refresh_endpoint="https://refresh_endpoint.com/",
                config=config,
                parameters={},
                profile_assertion=jwt,
                use_profile_assertion=True,
            )
            http_mocker.post(
                HttpRequest(
                    url="https://refresh_endpoint.com/",
                    body="grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=token",
                ),
                HttpResponse(body=json.dumps({"access_token": "access_token", "expires_in": 1000})),
            )

            token = oauth.refresh_access_token()

        assert ("access_token", 1000) == token

        filtered = filter_secrets("access_token")
        assert filtered == "****"

    def test_error_handling(self, mocker):
        oauth = DeclarativeOauth2Authenticator(
            token_refresh_endpoint="{{ config['refresh_endpoint'] }}",
            client_id="{{ config['client_id'] }}",
            client_secret="{{ config['client_secret'] }}",
            refresh_token="{{ config['refresh_token'] }}",
            config=config,
            scopes=["scope1", "scope2"],
            refresh_request_body={
                "custom_field": "{{ config['custom_field'] }}",
                "another_field": "{{ config['another_field'] }}",
                "scopes": ["no_override"],
            },
            parameters={},
        )
        resp.status_code = 400
        mocker.patch.object(
            resp, "json", return_value={"access_token": "access_token", "expires_in": 123}
        )
        mocker.patch.object(requests, "request", side_effect=mock_request, autospec=True)
        with pytest.raises(requests.exceptions.HTTPError) as e:
            oauth.refresh_access_token()
            assert e.value.errno == 400


def mock_request(method, url, data, headers):
    if url == "https://refresh_endpoint.com":
        return resp
    raise Exception(
        f"Error while refreshing access token with request: {method}, {url}, {data}, {headers}"
    )
