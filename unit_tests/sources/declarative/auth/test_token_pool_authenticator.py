#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import time
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.auth.token_pool_authenticator import TokenPoolAuthenticator
from airbyte_cdk.sources.declarative.auth.token_rotation_strategies import (
    RateLimitAwareRotation,
    RoundRobinRotation,
)


@pytest.mark.parametrize(
    "tokens_str,separator,auth_method,header,expected_first,expected_second",
    [
        pytest.param(
            "token1,token2,token3",
            ",",
            "Bearer",
            "Authorization",
            "Bearer token1",
            "Bearer token2",
            id="default_bearer_comma_separated",
        ),
        pytest.param(
            "tok_a|tok_b",
            "|",
            "token",
            "Authorization",
            "token tok_a",
            "token tok_b",
            id="pipe_separator_with_token_prefix",
        ),
        pytest.param(
            "single_token",
            ",",
            "",
            "X-Api-Key",
            "single_token",
            "single_token",
            id="single_token_no_prefix",
        ),
    ],
)
def test_token_pool_authenticator_basic(
    tokens_str, separator, auth_method, header, expected_first, expected_second
):
    auth = TokenPoolAuthenticator(
        tokens=tokens_str,
        config={},
        parameters={},
        token_separator=separator,
        auth_method=auth_method,
        header=header,
    )
    assert auth.auth_header == header
    assert auth.token == expected_first
    assert auth.token == expected_second


def test_token_pool_authenticator_round_robin_cycles():
    auth = TokenPoolAuthenticator(
        tokens="a,b,c",
        config={},
        parameters={},
    )
    results = [auth.token for _ in range(6)]
    assert results == [
        "Bearer a",
        "Bearer b",
        "Bearer c",
        "Bearer a",
        "Bearer b",
        "Bearer c",
    ]


def test_token_pool_authenticator_interpolated_config():
    config = {"api_tokens": "key1,key2"}
    auth = TokenPoolAuthenticator(
        tokens="{{ config['api_tokens'] }}",
        config=config,
        parameters={},
    )
    assert auth.token == "Bearer key1"
    assert auth.token == "Bearer key2"


def test_token_pool_authenticator_empty_tokens_raises():
    with pytest.raises(ValueError, match="at least one token"):
        TokenPoolAuthenticator(
            tokens="",
            config={},
            parameters={},
        )


def test_round_robin_rotation():
    strategy = RoundRobinRotation(tokens=["x", "y", "z"], parameters={})
    results = [strategy.get_active_token() for _ in range(6)]
    assert results == ["x", "y", "z", "x", "y", "z"]


def test_rate_limit_aware_rotation_rotates_on_exhaustion():
    strategy = RateLimitAwareRotation(
        tokens=["tok1", "tok2"],
        parameters={},
    )
    # Initially tok1
    assert strategy.get_active_token() == "tok1"

    # Simulate tok1 exhausted
    response = MagicMock()
    response.headers = {
        "x-ratelimit-remaining": "0",
        "x-ratelimit-reset": str(int(time.time()) + 3600),
    }
    strategy.update_from_response(response)

    # Should now return tok2
    assert strategy.get_active_token() == "tok2"


def test_rate_limit_aware_rotation_uses_token_until_exhausted():
    strategy = RateLimitAwareRotation(
        tokens=["tok1", "tok2"],
        parameters={},
        budget_min_reserve=0,
        budget_reserve_fraction=0.0,
    )

    response = MagicMock()
    response.headers = {
        "x-ratelimit-remaining": "100",
        "x-ratelimit-reset": str(int(time.time()) + 3600),
    }
    strategy.update_from_response(response)

    # Still on tok1 since remaining > 0
    assert strategy.get_active_token() == "tok1"


def test_rate_limit_aware_rotation_raises_when_all_exhausted_and_max_wait_exceeded():
    strategy = RateLimitAwareRotation(
        tokens=["tok1", "tok2"],
        parameters={},
        max_wait_seconds=1,
    )

    # Exhaust tok1 (active_index=0)
    response1 = MagicMock()
    response1.headers = {
        "x-ratelimit-remaining": "0",
        "x-ratelimit-reset": str(int(time.time()) + 9999),
    }
    strategy.update_from_response(response1)
    # tok1 is now at remaining=0, _active_index rotated to 1

    # Exhaust tok2 (active_index=1)
    response2 = MagicMock()
    response2.headers = {
        "x-ratelimit-remaining": "0",
        "x-ratelimit-reset": str(int(time.time()) + 9999),
    }
    strategy.update_from_response(response2)

    # All tokens exhausted, reset time exceeds max_wait_seconds
    with pytest.raises(RuntimeError, match="exceeds max_wait_seconds"):
        strategy.get_active_token()


def test_on_http_response_called_on_authenticator():
    auth = TokenPoolAuthenticator(
        tokens="tok1,tok2",
        config={},
        parameters={},
        rotation_strategy=RateLimitAwareRotation(
            tokens=["tok1", "tok2"],
            parameters={},
        ),
    )

    response = MagicMock()
    response.headers = {
        "x-ratelimit-remaining": "0",
        "x-ratelimit-reset": str(int(time.time()) + 3600),
    }
    auth.on_http_response(response)

    # After exhaustion of tok1, should get tok2
    assert auth.token == "Bearer tok2"
