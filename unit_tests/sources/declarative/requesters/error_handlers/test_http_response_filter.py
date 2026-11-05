#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json

import pytest
import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters.error_handlers import HttpResponseFilter
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    ErrorResolution,
    ResponseAction,
)


@pytest.mark.parametrize(
    "action, failure_type, http_codes, predicate, error_contains, error_message, response, expected_error_resolution",
    [
        pytest.param(
            ResponseAction.FAIL,
            None,
            {501, 503},
            "",
            "",
            "custom error message",
            {"status_code": 503},
            ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.transient_error,
                error_message="custom error message",
            ),
            id="test_http_code_matches",
        ),
        pytest.param(
            ResponseAction.IGNORE,
            None,
            {403},
            "",
            "",
            "",
            {"status_code": 403},
            ErrorResolution(
                response_action=ResponseAction.IGNORE,
                failure_type=FailureType.config_error,
                error_message="HTTP Status Code: 403. Error: Forbidden. You don't have permission to access this resource.",
            ),
            id="test_http_code_matches_ignore_action",
        ),
        pytest.param(
            ResponseAction.RETRY,
            None,
            {429},
            "",
            "",
            "",
            {"status_code": 429},
            ErrorResolution(
                response_action=ResponseAction.RETRY,
                failure_type=FailureType.transient_error,
                error_message="HTTP Status Code: 429. Error: Too many requests.",
            ),
            id="test_http_code_matches_retry_action",
        ),
        pytest.param(
            ResponseAction.FAIL,
            None,
            {},
            '{{ response.the_body == "do_i_match" }}',
            "",
            "error message was: {{ response.failure }}",
            {"status_code": 404, "json": {"the_body": "do_i_match", "failure": "i failed you"}},
            ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.system_error,
                error_message="error message was: i failed you",
            ),
            id="test_predicate_matches_json",
        ),
        pytest.param(
            ResponseAction.FAIL,
            None,
            {},
            '{{ headers.the_key == "header_match" }}',
            "",
            "error from header: {{ headers.warning }}",
            {"status_code": 404, "headers": {"the_key": "header_match", "warning": "this failed"}},
            ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.system_error,
                error_message="error from header: this failed",
            ),
            id="test_predicate_matches_headers",
        ),
        pytest.param(
            ResponseAction.FAIL,
            None,
            {},
            None,
            "DENIED",
            "",
            {"status_code": 403, "json": {"error": "REQUEST_DENIED"}},
            ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.config_error,
                error_message="HTTP Status Code: 403. Error: Forbidden. You don't have permission to access this resource.",
            ),
            id="test_predicate_matches_headers",
        ),
        pytest.param(
            ResponseAction.FAIL,
            None,
            {400, 404},
            '{{ headers.error == "invalid_input" or response.reason == "bad request"}}',
            "",
            "",
            {
                "status_code": 403,
                "headers": {"error": "authentication_error"},
                "json": {"reason": "permission denied"},
            },
            None,
            id="test_response_does_not_match_filter",
        ),
        pytest.param(
            ResponseAction.FAIL,
            FailureType.config_error,
            {403, 404},
            "",
            "",
            "check permissions",
            {"status_code": 403},
            ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.config_error,
                error_message="check permissions",
            ),
            id="test_http_code_matches_failure_type_config_error",
        ),
        pytest.param(
            ResponseAction.FAIL,
            FailureType.system_error,
            {403, 404},
            "",
            "",
            "check permissions",
            {"status_code": 403},
            ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.system_error,
                error_message="check permissions",
            ),
            id="test_http_code_matches_failure_type_system_error",
        ),
        pytest.param(
            ResponseAction.FAIL,
            FailureType.transient_error,
            {500},
            "",
            "",
            "rate limits",
            {"status_code": 500},
            ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.transient_error,
                error_message="rate limits",
            ),
            id="test_http_code_matches_failure_type_transient_error",
        ),
        pytest.param(
            ResponseAction.RETRY,
            FailureType.config_error,
            {500},
            "",
            "",
            "rate limits",
            {"status_code": 500},
            ErrorResolution(
                response_action=ResponseAction.RETRY,
                failure_type=FailureType.transient_error,
                error_message="rate limits",
            ),
            id="test_http_code_matches_failure_type_config_error_action_retry_uses_default_failure_type",
        ),
        pytest.param(
            ResponseAction.RATE_LIMITED,
            None,
            {500},
            "",
            "",
            "rate limits",
            {"status_code": 500},
            ErrorResolution(
                response_action=ResponseAction.RATE_LIMITED,
                failure_type=FailureType.transient_error,
                error_message="rate limits",
            ),
            id="test_http_code_matches_response_action_rate_limited",
        ),
    ],
)
def test_matches(
    requests_mock,
    action,
    failure_type,
    http_codes,
    predicate,
    error_contains,
    error_message,
    response,
    expected_error_resolution,
):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        text=response.get("json") and json.dumps(response.get("json")),
        headers=response.get("headers") or {},
        status_code=response.get("status_code"),
    )
    response = requests.get("https://airbyte.io/")
    response_filter = HttpResponseFilter(
        action=action,
        failure_type=failure_type,
        config={},
        parameters={},
        http_codes=http_codes,
        predicate=predicate,
        error_message_contains=error_contains,
        error_message=error_message,
    )

    actual_response_status = response_filter.matches(response)
    if expected_error_resolution:
        assert actual_response_status.response_action == expected_error_resolution.response_action
        assert actual_response_status.failure_type == expected_error_resolution.failure_type
        assert actual_response_status.error_message == expected_error_resolution.error_message
    else:
        assert actual_response_status is None


@pytest.mark.parametrize(
    "url, expected_request_url",
    [
        pytest.param(
            "https://api.github.com/repos/airbytehq/airbyte/stargazers?per_page=100&page=2",
            "https://api.github.com/repos/airbytehq/airbyte/stargazers",
            id="query_string_dropped",
        ),
        pytest.param(
            "https://user:secret@api.example.com/v1/items?api_key=secret",
            "https://api.example.com/v1/items",
            id="credentials_dropped",
        ),
        pytest.param(
            "https://user:pw@api.example.com:8443/v1/items?k=v#frag",
            "https://api.example.com:8443/v1/items",
            id="port_kept_fragment_dropped",
        ),
        pytest.param(
            "https://[::1]:8443/v1/items?k=v",
            "https://[::1]:8443/v1/items",
            id="ipv6_host_and_port_kept",
        ),
    ],
)
def test_error_message_interpolates_request_url(requests_mock, url, expected_request_url):
    requests_mock.get(url, status_code=404, json={"message": "Not Found"})
    response_filter = HttpResponseFilter(
        action=ResponseAction.IGNORE,
        config={},
        parameters={},
        http_codes={404},
        error_message="Skipping {{ request_url }}.",
    )

    resolution = response_filter.matches(requests.get(url))

    assert resolution.response_action == ResponseAction.IGNORE
    assert resolution.error_message == f"Skipping {expected_request_url}."


def test_predicate_interpolates_request_url(requests_mock):
    requests_mock.get(
        "https://api.github.com/repos/airbytehq/airbyte/stargazers",
        status_code=404,
        json={"message": "Not Found"},
    )
    requests_mock.get(
        "https://api.github.com/repos/airbytehq/airbyte/events",
        status_code=404,
        json={"message": "Not Found"},
    )
    requests_mock.get(
        "https://api.github.com/repos/airbytehq/airbyte-cdk/stargazers",
        status_code=200,
        json=[{"login": "octocat"}],
    )
    response_filter = HttpResponseFilter(
        action=ResponseAction.IGNORE,
        config={},
        parameters={},
        predicate="{{ response.get('message') == 'Not Found' and request_url.endswith('/stargazers') }}",
    )

    stargazers = response_filter.matches(
        requests.get("https://api.github.com/repos/airbytehq/airbyte/stargazers")
    )
    events = response_filter.matches(
        requests.get("https://api.github.com/repos/airbytehq/airbyte/events")
    )
    # The filter also runs on successful pages, so the predicate must not match on the URL alone.
    successful_page = response_filter.matches(
        requests.get("https://api.github.com/repos/airbytehq/airbyte-cdk/stargazers")
    )

    assert stargazers.response_action == ResponseAction.IGNORE
    assert events is None
    assert successful_page is None


def test_request_url_is_empty_when_response_has_no_url():
    response = requests.Response()
    response.status_code = 404
    message_filter = HttpResponseFilter(
        action=ResponseAction.IGNORE,
        config={},
        parameters={},
        http_codes={404},
        error_message="Skipping [{{ request_url }}].",
    )
    predicate_filter = HttpResponseFilter(
        action=ResponseAction.IGNORE,
        config={},
        parameters={},
        predicate="{{ request_url.endswith('/stargazers') }}",
    )

    assert message_filter.matches(response).error_message == "Skipping []."
    assert predicate_filter.matches(response) is None
