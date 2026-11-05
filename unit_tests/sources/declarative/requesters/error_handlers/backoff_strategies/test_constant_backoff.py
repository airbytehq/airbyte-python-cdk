#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from unittest.mock import MagicMock

import pytest
import requests

from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    ConstantBackoffStrategy as ConstantBackoffStrategyModel,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.constant_backoff_strategy import (
    ConstantBackoffStrategy,
)

BACKOFF_TIME = 10
PARAMETERS_BACKOFF_TIME = 20
CONFIG_BACKOFF_TIME = 30

GSC_BACKOFF_EXPRESSION = "{{ 900 if 'load quota exceeded' in ((response.get('error') or {}).get('message') or '') | lower else 0 }}"
AMAZON_BACKOFF_EXPRESSION = "{{ (1 / (headers.get('x-amzn-RateLimit-Limit') | float)) if (headers.get('x-amzn-RateLimit-Limit') | float) > 0 else 60 }}"
PINTEREST_BACKOFF_EXPRESSION = "{{ (response.get('message', '') | regex_search('(?i)Retry after\\s+(\\d+)\\s+seconds')) or min(2 ** attempt_count, 120) if response is mapping else min(2 ** attempt_count, 120) }}"
LINKEDIN_BACKOFF_EXPRESSION = "{{ 330 if 'data request limit has been exceeded' in (response.get('message') or '') | lower and '45 million metric values' in (response.get('message') or '') | lower else 0 }}"


def _make_response(status_code=429, json_body=None, raw_body=None, headers=None):
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://airbyte.io"
    response.headers.update(headers or {})
    if raw_body is not None:
        response._content = raw_body
    elif json_body is not None:
        response._content = json.dumps(json_body).encode("utf-8")
    else:
        response._content = b"{}"
    return response


def _strategy(backoff_time_in_seconds, jitter_range=None, parameters=None, config=None):
    return ConstantBackoffStrategy(
        parameters=parameters or {},
        backoff_time_in_seconds=backoff_time_in_seconds,
        jitter_range_in_seconds=jitter_range,
        config=config or {},
    )


@pytest.mark.parametrize(
    "test_name, attempt_count, backofftime, jitter_range, expected_backoff_time",
    [
        ("test_constant_backoff_first_attempt", 1, BACKOFF_TIME, None, BACKOFF_TIME),
        ("test_constant_backoff_first_attempt_float", 1, 6.7, None, 6.7),
        ("test_constant_backoff_attempt_round_float", 1.0, 6.7, None, 6.7),
        ("test_constant_backoff_attempt_round_float", 1.5, 6.7, None, 6.7),
        ("test_constant_backoff_first_attempt_round_float", 1, 10.0, None, BACKOFF_TIME),
        ("test_constant_backoff_second_attempt_round_float", 2, 10.0, None, BACKOFF_TIME),
        ("test_constant_backoff_zero_jitter", 2, BACKOFF_TIME, 0, BACKOFF_TIME),
        (
            "test_constant_backoff_from_parameters",
            1,
            "{{ parameters['backoff'] }}",
            None,
            PARAMETERS_BACKOFF_TIME,
        ),
        (
            "test_constant_backoff_from_config",
            1,
            "{{ config['backoff'] }}",
            None,
            CONFIG_BACKOFF_TIME,
        ),
    ],
)
def test_constant_backoff(
    test_name, attempt_count, backofftime, jitter_range, expected_backoff_time
):
    response_mock = MagicMock()
    backoff_strategy = ConstantBackoffStrategy(
        parameters={"backoff": PARAMETERS_BACKOFF_TIME},
        backoff_time_in_seconds=backofftime,
        jitter_range_in_seconds=jitter_range,
        config={"backoff": CONFIG_BACKOFF_TIME, "jitter": 0},
    )
    backoff = backoff_strategy.backoff_time(response_mock, attempt_count=attempt_count)
    assert backoff == expected_backoff_time


@pytest.mark.parametrize(
    "backofftime, jitter_range, expected_lower_bound, expected_upper_bound",
    [
        pytest.param(60, 15, 60, 90, id="base_backoff_floor"),
        pytest.param(10, 30, 10, 70, id="large_jitter"),
    ],
)
def test_constant_backoff_with_jitter_bounds(
    backofftime, jitter_range, expected_lower_bound, expected_upper_bound
):
    response_mock = MagicMock()
    backoff_strategy = ConstantBackoffStrategy(
        parameters={},
        backoff_time_in_seconds=backofftime,
        jitter_range_in_seconds=jitter_range,
        config={},
    )

    backoff_times = [
        backoff_strategy.backoff_time(response_mock, attempt_count=1) for _ in range(2000)
    ]

    assert all(expected_lower_bound <= backoff <= expected_upper_bound for backoff in backoff_times)
    assert len(set(backoff_times)) > 1


@pytest.mark.parametrize(
    "json_body, expected_backoff_time",
    [
        pytest.param(
            {"error": {"message": "Search Analytics load quota exceeded."}},
            900,
            id="load_quota_exceeded",
        ),
        pytest.param(
            {"error": {"message": "Search Analytics QPS quota exceeded."}},
            None,
            id="qps_quota_exceeded",
        ),
    ],
)
def test_constant_backoff_response_body(json_body, expected_backoff_time):
    backoff_strategy = _strategy(GSC_BACKOFF_EXPRESSION)
    response = _make_response(json_body=json_body)
    assert backoff_strategy.backoff_time(response, attempt_count=1) == expected_backoff_time


@pytest.mark.parametrize(
    "headers, expected_backoff_time",
    [
        pytest.param({"x-amzn-RateLimit-Limit": "0.5"}, 2.0, id="low_rate_limit"),
        pytest.param({"x-amzn-RateLimit-Limit": "2.0"}, 0.5, id="higher_rate_limit"),
        pytest.param({}, 60, id="missing_rate_limit_header"),
        pytest.param({"x-amzn-RateLimit-Limit": "abc"}, 60, id="garbage_rate_limit_header"),
    ],
)
def test_constant_backoff_response_headers(headers, expected_backoff_time):
    backoff_strategy = _strategy(AMAZON_BACKOFF_EXPRESSION)
    response = _make_response(headers=headers)
    assert backoff_strategy.backoff_time(response, attempt_count=1) == expected_backoff_time


@pytest.mark.parametrize(
    "json_body, raw_body, attempt_count, expected_backoff_time",
    [
        pytest.param(
            {"message": "Rate limited. Retry after 30 seconds"},
            None,
            1,
            30,
            id="retry_after_in_body",
        ),
        pytest.param({"message": "rate limited"}, None, 2, 4, id="exponential_fallback"),
        pytest.param({"message": "rate limited"}, None, 10, 120, id="exponential_fallback_capped"),
        pytest.param([1, 2, 3], None, 2, 4, id="non_dict_json_body"),
        pytest.param(None, b"not json at all", 3, 8, id="non_json_body"),
    ],
)
def test_constant_backoff_response_body_and_attempt_count(
    json_body, raw_body, attempt_count, expected_backoff_time
):
    backoff_strategy = _strategy(PINTEREST_BACKOFF_EXPRESSION)
    response = _make_response(json_body=json_body, raw_body=raw_body)
    assert (
        backoff_strategy.backoff_time(response, attempt_count=attempt_count)
        == expected_backoff_time
    )


def test_constant_backoff_unguarded_get_on_list_body_falls_through():
    backoff_strategy = _strategy("{{ response.get('message') }}")
    response = _make_response(json_body=[1, 2, 3])
    assert backoff_strategy.backoff_time(response, attempt_count=1) is None


@pytest.mark.parametrize("jitter_range", [None, 15])
@pytest.mark.parametrize(
    "backofftime",
    [
        pytest.param("{{ '' }}", id="empty_string_render"),
        pytest.param("{{ None }}", id="none_render"),
        pytest.param("{{ 0 }}", id="zero_render"),
        pytest.param(0, id="plain_zero"),
    ],
)
def test_constant_backoff_falsy_render_falls_through(backofftime, jitter_range):
    backoff_strategy = _strategy(backofftime, jitter_range=jitter_range)
    response = _make_response()
    assert backoff_strategy.backoff_time(response, attempt_count=1) is None


def test_constant_backoff_non_numeric_render_falls_through(caplog):
    backoff_strategy = _strategy("{{ 'abc' }}")
    response = _make_response()
    assert backoff_strategy.backoff_time(response, attempt_count=1) is None


@pytest.mark.parametrize(
    "response_or_exception",
    [
        pytest.param(requests.exceptions.ConnectionError(), id="request_exception"),
        pytest.param(None, id="no_response"),
    ],
)
def test_constant_backoff_on_exception_or_none(response_or_exception):
    backoff_strategy = _strategy("{{ headers.get('x') or response.get('y') or 7 }}")
    assert backoff_strategy.backoff_time(response_or_exception, attempt_count=1) == 7


def test_constant_backoff_from_factory():
    manifest = {
        "type": "ConstantBackoffStrategy",
        "backoff_time_in_seconds": LINKEDIN_BACKOFF_EXPRESSION,
    }
    backoff_strategy = ModelToComponentFactory().create_component(
        ConstantBackoffStrategyModel, manifest, config={}
    )
    assert isinstance(backoff_strategy, ConstantBackoffStrategy)

    throttled_response = _make_response(
        json_body={
            "message": "Data request limit has been exceeded. Limit of 45 million metric values per day."
        }
    )
    assert backoff_strategy.backoff_time(throttled_response, attempt_count=1) == 330

    other_response = _make_response(json_body={"message": "some other error"})
    assert backoff_strategy.backoff_time(other_response, attempt_count=1) is None
