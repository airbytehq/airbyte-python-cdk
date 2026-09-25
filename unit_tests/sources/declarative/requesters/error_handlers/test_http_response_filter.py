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
from airbyte_cdk.sources.streams.http.streamed_response import mark_body_streamed


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


def _access_denied_filter(**kwargs) -> HttpResponseFilter:
    return HttpResponseFilter(
        action=ResponseAction.IGNORE,
        failure_type=None,
        config={},
        parameters={},
        http_codes=set(),
        predicate="",
        error_message_contains="You do not have access",
        error_message="",
        **kwargs,
    )


def _streamed_2xx_with_access_denied_body(requests_mock):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        text=json.dumps({"error": "You do not have access"}),
        status_code=200,
    )
    return requests.get("https://airbyte.io/", stream=True)


def test_error_message_contains_matches_403_despite_streamed_marker(requests_mock):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        text=json.dumps({"error": "You do not have access"}),
        status_code=403,
    )
    response = requests.get("https://airbyte.io/", stream=True)
    mark_body_streamed(response)
    resolution = _access_denied_filter().matches(response)
    assert resolution.response_action == ResponseAction.IGNORE


def test_streamed_2xx_skips_body_filters_and_keeps_body_unread(requests_mock):
    response = _streamed_2xx_with_access_denied_body(requests_mock)
    mark_body_streamed(response)
    assert _access_denied_filter().matches(response) is None
    assert response._content_consumed is False


def test_unmarked_2xx_still_matches_error_message_contains(requests_mock):
    response = _streamed_2xx_with_access_denied_body(requests_mock)
    resolution = _access_denied_filter().matches(response)
    assert resolution.response_action == ResponseAction.IGNORE


def test_streamed_2xx_skips_predicate_filter(requests_mock):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        text=json.dumps({"flag": "match me"}),
        status_code=200,
    )
    response = requests.get("https://airbyte.io/", stream=True)
    mark_body_streamed(response)
    response_filter = HttpResponseFilter(
        action=ResponseAction.IGNORE,
        failure_type=None,
        config={},
        parameters={},
        http_codes=set(),
        predicate='{{ response.flag == "match me" }}',
        error_message_contains="",
        error_message="",
    )
    assert response_filter.matches(response) is None
    assert response._content_consumed is False


def test_http_codes_filter_matches_marked_2xx_without_consuming_body(requests_mock):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        text=json.dumps({"error": "whatever"}),
        status_code=200,
    )
    response = requests.get("https://airbyte.io/", stream=True)
    mark_body_streamed(response)
    response_filter = HttpResponseFilter(
        action=ResponseAction.IGNORE,
        failure_type=None,
        config={},
        parameters={},
        http_codes={200},
        predicate="",
        error_message_contains="",
        error_message="matched: {{ response.get('error') }}",
    )
    resolution = response_filter.matches(response)
    assert resolution.response_action == ResponseAction.IGNORE
    # the error_message template interpolates `response` as {} for streamed 2xx bodies
    assert resolution.error_message == "matched: None"
    assert response._content_consumed is False
