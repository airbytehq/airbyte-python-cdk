#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import threading
import time
from datetime import timedelta
from unittest.mock import patch

import pytest
import requests

from airbyte_cdk.sources.declarative.auth.rate_limited_multiple_token import (
    RateLimitedMultipleTokenAuthenticator,
    TokenQuota,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    RateLimitedMultipleTokenAuthenticator as RateLimitedMultipleTokenAuthenticatorModel,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.sources.streams.call_rate import HttpRequestRegexMatcher
from airbyte_cdk.utils import AirbyteTracedException

QUOTA_STATUS_URL = "https://api.example.com/rate_limit"


def _quota_status_body(rest_remaining=5000, graphql_remaining=5000, reset_in_seconds=3600):
    reset = int(time.time()) + reset_in_seconds
    return {
        "resources": {
            "core": {"remaining": rest_remaining, "reset": reset, "limit": 5000},
            "graphql": {"remaining": graphql_remaining, "reset": reset, "limit": 5000},
        }
    }


def _quotas():
    return [
        TokenQuota(
            name="rest",
            remaining_path=["resources", "core", "remaining"],
            reset_path=["resources", "core", "reset"],
            limit_path=["resources", "core", "limit"],
        ),
        TokenQuota(
            name="graphql",
            remaining_path=["resources", "graphql", "remaining"],
            reset_path=["resources", "graphql", "reset"],
            limit_path=["resources", "graphql", "limit"],
            matchers=[HttpRequestRegexMatcher(url_path_pattern="/graphql")],
        ),
    ]


def _authenticator(tokens=("token_1", "token_2"), **kwargs):
    return RateLimitedMultipleTokenAuthenticator(
        tokens=list(tokens),
        quotas=_quotas(),
        quota_status_url=QUOTA_STATUS_URL,
        auth_method="token",
        **kwargs,
    )


def _prepared_request(url="https://api.example.com/repos"):
    return requests.Request(method="GET", url=url).prepare()


def test_seeding_and_header_injection(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _authenticator()

    request = authenticator(_prepared_request())

    assert request.headers["Authorization"] == "token token_1"
    # one seeding call per token
    assert requests_mock.call_count == 2


@pytest.mark.parametrize(
    "url,expected_quota",
    [
        pytest.param("https://api.example.com/repos", "rest", id="rest_request"),
        pytest.param("https://api.example.com/graphql", "graphql", id="graphql_request"),
    ],
)
def test_quota_matching(requests_mock, url, expected_quota):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _authenticator(tokens=("token_1",))

    authenticator(_prepared_request(url))

    states = authenticator._states["token_1"]
    assert states[expected_quota].remaining == 4999
    other_quota = "graphql" if expected_quota == "rest" else "rest"
    assert states[other_quota].remaining == 5000


def test_rotation_when_active_token_exhausted(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _authenticator()
    authenticator._ensure_initialized()
    authenticator._states["token_1"]["rest"].remaining = 0

    request = authenticator(_prepared_request())

    assert request.headers["Authorization"] == "token token_2"
    assert authenticator._states["token_2"]["rest"].remaining == 4999


def test_raises_transient_error_when_reset_exceeds_max_wait_time(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body(rest_remaining=0))
    authenticator = _authenticator(max_wait_time=timedelta(seconds=10))

    with pytest.raises(AirbyteTracedException, match="Rate limit is exceeded"):
        authenticator(_prepared_request())


def test_waits_and_reseeds_when_all_tokens_exhausted(requests_mock):
    responses = iter(
        [
            _quota_status_body(rest_remaining=0, reset_in_seconds=5),
            _quota_status_body(rest_remaining=0, reset_in_seconds=5),
            _quota_status_body(rest_remaining=5000),
            _quota_status_body(rest_remaining=5000),
        ]
    )
    requests_mock.get(QUOTA_STATUS_URL, json=lambda request, context: next(responses))
    authenticator = _authenticator()

    with patch("time.sleep") as mock_sleep:
        request = authenticator(_prepared_request())

    assert mock_sleep.called
    assert request.headers["Authorization"] == "token token_1"
    assert authenticator._states["token_1"]["rest"].remaining == 4999
    # the exhaustion wait is floored so stale reset timestamps can't busy-loop the quota endpoint
    assert (
        mock_sleep.call_args_list[0][0][0]
        >= RateLimitedMultipleTokenAuthenticator.MIN_EXHAUSTION_WAIT
    )


def test_budget_throttling_delay_injected_when_all_tokens_below_reserve(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body(rest_remaining=100))
    authenticator = _authenticator()

    with patch("time.sleep") as mock_sleep:
        authenticator(_prepared_request())

    # reserve = max(50, 0.1 * 5000) = 500 > 100 remaining on both tokens -> throttled
    assert mock_sleep.called
    delay = mock_sleep.call_args[0][0]
    assert 0.1 <= delay <= RateLimitedMultipleTokenAuthenticator.MAX_BUDGET_DELAY


def test_no_budget_throttling_when_tokens_have_headroom(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _authenticator()

    with patch("time.sleep") as mock_sleep:
        authenticator(_prepared_request())

    assert not mock_sleep.called


def test_thread_safety_no_lost_decrements(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _authenticator(tokens=("token_1",))
    calls = 200

    def make_call():
        authenticator(_prepared_request())

    threads = [threading.Thread(target=make_call) for _ in range(calls)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert authenticator._states["token_1"]["rest"].remaining == 5000 - calls


def test_thread_safety_header_token_matches_decremented_token(requests_mock):
    """Under concurrent rotation, each request must be signed with the token whose counter was decremented."""
    seed = _quota_status_body()
    seed["resources"]["core"]["remaining"] = 100
    requests_mock.get(QUOTA_STATUS_URL, json=seed)
    authenticator = _authenticator(
        tokens=("token_1", "token_2"), budget_reserve_fraction=0, budget_min_reserve=0
    )
    calls = 150  # more than one token's quota, forcing rotation mid-flight
    signed_tokens = []
    signed_lock = threading.Lock()

    def make_call():
        request = authenticator(_prepared_request())
        with signed_lock:
            signed_tokens.append(request.headers["Authorization"].split()[1])

    threads = [threading.Thread(target=make_call) for _ in range(calls)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    for token in ("token_1", "token_2"):
        used = signed_tokens.count(token)
        assert authenticator._states[token]["rest"].remaining == 100 - used


def test_missing_path_in_quota_status_response_raises_config_error(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json={"unexpected": {}})

    authenticator = _authenticator()
    with pytest.raises(AirbyteTracedException, match="missing an expected field"):
        authenticator(_prepared_request())


def test_no_tokens_raises_config_error():
    with pytest.raises(AirbyteTracedException, match="tokens are missing"):
        RateLimitedMultipleTokenAuthenticator(
            tokens=[], quotas=_quotas(), quota_status_url=QUOTA_STATUS_URL
        )


def _model(tokens):
    return RateLimitedMultipleTokenAuthenticatorModel.parse_obj(
        {
            "type": "RateLimitedMultipleTokenAuthenticator",
            "tokens": tokens,
            "auth_method": "token",
            "quota_status_source": {
                "type": "QuotaStatusSource",
                "url": "{{ config.get('api_url', 'https://api.example.com') }}/rate_limit",
            },
            "quotas": [
                {
                    "type": "TokenQuota",
                    "name": "rest",
                    "remaining_path": ["resources", "core", "remaining"],
                    "reset_path": ["resources", "core", "reset"],
                },
                {
                    "type": "TokenQuota",
                    "name": "graphql",
                    "remaining_path": ["resources", "graphql", "remaining"],
                    "reset_path": ["resources", "graphql", "reset"],
                    "matchers": [
                        {"type": "HttpRequestRegexMatcher", "url_path_pattern": "/graphql"}
                    ],
                },
            ],
        }
    )


@pytest.mark.parametrize(
    "tokens,config,expected_tokens",
    [
        pytest.param(
            "{{ config['pat'] }}",
            {"pat": "token_1,token_2, token_3"},
            ["token_1", "token_2", "token_3"],
            id="delimiter_separated_string",
        ),
        pytest.param(
            ["{{ config['token_a'] }}", "{{ config['token_b'] }}"],
            {"token_a": "token_1", "token_b": "token_2"},
            ["token_1", "token_2"],
            id="explicit_list",
        ),
    ],
)
def test_factory_token_parsing(tokens, config, expected_tokens):
    factory = ModelToComponentFactory()

    authenticator = factory.create_rate_limited_multiple_token_authenticator(_model(tokens), config)

    assert authenticator._tokens == expected_tokens
    assert authenticator._quota_status_url == "https://api.example.com/rate_limit"
    assert [quota.name for quota in authenticator._quotas] == ["rest", "graphql"]


def test_factory_returns_shared_instance_for_identical_definitions():
    factory = ModelToComponentFactory()
    config = {"pat": "token_1,token_2"}

    first = factory.create_rate_limited_multiple_token_authenticator(
        _model("{{ config['pat'] }}"), config
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _model("{{ config['pat'] }}"), config
    )

    assert first is second


def test_factory_interpolates_max_wait_time():
    factory = ModelToComponentFactory()
    model = _model("{{ config['pat'] }}")
    model.max_wait_time = "PT{{ config.get('max_waiting_time', 120) }}M"

    authenticator = factory.create_rate_limited_multiple_token_authenticator(
        model, {"pat": "token_1", "max_waiting_time": 30}
    )

    assert authenticator._max_wait_time == timedelta(minutes=30)
