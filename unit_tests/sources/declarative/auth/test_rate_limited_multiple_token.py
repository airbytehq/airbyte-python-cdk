#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import threading
import time
from datetime import timedelta
from unittest.mock import patch

import pytest
import requests
from pydantic.v1 import ValidationError

from airbyte_cdk.sources.declarative.auth.rate_limited_multiple_token import (
    RateLimitedMultipleTokenAuthenticator,
    TokenQuota,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    RateLimitedMultipleTokenAuthenticator as RateLimitedMultipleTokenAuthenticatorModel,
)
from airbyte_cdk.sources.declarative.parsers.manifest_component_transformer import (
    ManifestComponentTransformer,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.sources.streams.call_rate import HttpRequestRegexMatcher
from airbyte_cdk.sources.streams.http.requests_native_auth import (
    ResponseAwareAuthenticator,
    TokenRotatingAuthenticator,
)
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


def test_elapsed_window_reseeds_from_the_server_rather_than_refilling_locally(requests_mock):
    """An exhausted pool whose reset has passed must be refreshed from the quota endpoint.

    Handing the allowance back locally instead would let the connector invent quota it has no
    evidence for: the reset timestamp says a new window is due, but only the server knows what
    is actually left in it. A pool refilled from a stale timestamp never reseeds and never
    waits, so it would serve unlimited calls against a quota it stopped tracking.
    """
    body = {"resources": {"core": {"remaining": 3, "reset": int(time.time()) - 10, "limit": 3}}}
    quota_status = requests_mock.get(QUOTA_STATUS_URL, json=body)
    authenticator = RateLimitedMultipleTokenAuthenticator(
        tokens=["token_1"],
        quotas=[
            TokenQuota(
                name="rest",
                remaining_path=["resources", "core", "remaining"],
                reset_path=["resources", "core", "reset"],
                limit_path=["resources", "core", "limit"],
            )
        ],
        quota_status_url=QUOTA_STATUS_URL,
        auth_method="token",
    )

    sleeps = []
    with patch("time.sleep", side_effect=lambda seconds: sleeps.append(seconds)):
        for _ in range(12):
            authenticator(_prepared_request())

    # 3 calls per window: the initial seeding plus one reseed for each exhausted window.
    assert quota_status.call_count == 4
    assert sleeps == [RateLimitedMultipleTokenAuthenticator.MIN_EXHAUSTION_WAIT] * 3


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
    worker_exceptions = []
    signed_lock = threading.Lock()

    def make_call():
        try:
            request = authenticator(_prepared_request())
            with signed_lock:
                signed_tokens.append(request.headers["Authorization"].split()[1])
        except Exception as exc:
            with signed_lock:
                worker_exceptions.append(exc)

    threads = [threading.Thread(target=make_call) for _ in range(calls)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert worker_exceptions == []
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


def test_no_quotas_raises_config_error():
    with pytest.raises(AirbyteTracedException, match="Quota pool configuration is missing"):
        RateLimitedMultipleTokenAuthenticator(
            tokens=["token_1"], quotas=[], quota_status_url=QUOTA_STATUS_URL
        )


def test_model_rejects_empty_quotas():
    with pytest.raises(ValidationError, match="quotas"):
        RateLimitedMultipleTokenAuthenticatorModel.parse_obj(
            {
                "type": "RateLimitedMultipleTokenAuthenticator",
                "tokens": ["token_1"],
                "quota_status_source": {
                    "type": "QuotaStatusSource",
                    "url": "https://api.example.com/rate_limit",
                },
                "quotas": [],
            }
        )


def test_raises_after_cumulative_max_wait_time(requests_mock):
    """A quota status that keeps reporting zero remaining with a stale reset must not loop forever."""
    requests_mock.get(
        QUOTA_STATUS_URL,
        json=lambda request, context: _quota_status_body(rest_remaining=0, reset_in_seconds=-10),
    )
    authenticator = _authenticator(max_wait_time=timedelta(seconds=30))
    clock = {"now": 0.0}

    def fake_sleep(seconds):
        clock["now"] += seconds

    with (
        patch("time.sleep", side_effect=fake_sleep),
        patch("time.monotonic", side_effect=lambda: clock["now"]),
    ):
        with pytest.raises(AirbyteTracedException, match="Rate limit is exceeded"):
            authenticator(_prepared_request())

    # total time waited never exceeds the configured max_wait_time
    assert clock["now"] <= 30


# --- reconciling quota state against response headers ---------------------------------------


def _response_aware_quotas(exhaustion_status_codes=(429,)):
    return [
        TokenQuota(
            name="rest",
            remaining_path=["resources", "core", "remaining"],
            reset_path=["resources", "core", "reset"],
            limit_path=["resources", "core", "limit"],
            remaining_header="X-RateLimit-Remaining",
            reset_header="X-RateLimit-Reset",
            limit_header="X-RateLimit-Limit",
            exhaustion_status_codes=list(exhaustion_status_codes),
        ),
        TokenQuota(
            name="graphql",
            remaining_path=["resources", "graphql", "remaining"],
            reset_path=["resources", "graphql", "reset"],
            limit_path=["resources", "graphql", "limit"],
            remaining_header="X-RateLimit-Remaining",
            reset_header="X-RateLimit-Reset",
            matchers=[HttpRequestRegexMatcher(url_path_pattern="/graphql")],
        ),
    ]


def _response_aware_authenticator(
    tokens=("token_1", "token_2"), exhaustion_status_codes=(429,), **kwargs
):
    """`**kwargs` reach the authenticator, matching `_authenticator` above; quota-shaping
    arguments are named explicitly so a caller passing e.g. `max_wait_time` doesn't hit a
    TypeError from the quota builder."""
    return RateLimitedMultipleTokenAuthenticator(
        tokens=list(tokens),
        quotas=_response_aware_quotas(exhaustion_status_codes=exhaustion_status_codes),
        quota_status_url=QUOTA_STATUS_URL,
        auth_method="token",
        **kwargs,
    )


def _response(status_code=200, headers=None, from_cache=False):
    response = requests.Response()
    response.status_code = status_code
    response.headers.update(headers or {})
    if from_cache:
        response.from_cache = True  # type: ignore[attr-defined] # mirrors requests_cache
    return response


def test_response_header_corrects_counter_downward(requests_mock):
    """The server is the source of truth: a lower remaining count wins over the local estimate."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    assert authenticator._states["token_1"]["rest"].remaining == 4999

    authenticator.update_from_response(request, _response(headers={"X-RateLimit-Remaining": "10"}))

    assert authenticator._states["token_1"]["rest"].remaining == 10


def test_response_header_does_not_ratchet_counter_up_within_a_window(requests_mock):
    """A slow response carries a stale, higher count. Handing those calls back would let concurrent
    requests overspend, so within one window the estimate may only tighten."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    reset_at = authenticator._states["token_1"]["rest"].reset_at
    authenticator._states["token_1"]["rest"].remaining = 10

    authenticator.update_from_response(
        request,
        _response(
            headers={
                "X-RateLimit-Remaining": "4000",
                "X-RateLimit-Reset": str(int(reset_at.timestamp())),
            }
        ),
    )

    assert authenticator._states["token_1"]["rest"].remaining == 10


def test_later_reset_in_response_starts_a_new_window(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    state = authenticator._states["token_1"]["rest"]
    state.remaining = 3
    next_window = int(state.reset_at.timestamp()) + 3600

    authenticator.update_from_response(
        request,
        _response(headers={"X-RateLimit-Remaining": "5000", "X-RateLimit-Reset": str(next_window)}),
    )

    assert state.remaining == 5000
    assert int(state.reset_at.timestamp()) == next_window


def test_slightly_earlier_reset_still_counts_as_the_current_window(requests_mock):
    """The quota endpoint and the response headers can disagree by a little, so a reset just
    behind the one we hold is the same window and its count still clamps."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    state = authenticator._states["token_1"]["rest"]
    state.remaining = 100
    current_window = int(state.reset_at.timestamp())

    authenticator.update_from_response(
        request,
        _response(
            headers={"X-RateLimit-Remaining": "7", "X-RateLimit-Reset": str(current_window - 5)}
        ),
    )

    assert state.remaining == 7
    assert int(state.reset_at.timestamp()) == current_window  # never moved backwards


def test_stale_zero_from_a_successful_response_does_not_park_a_refilled_pool(requests_mock):
    """A zero on a 200 is just the last call of a window, not a rate-limit rejection.

    The exhaustion exception to the window check exists so a rate limit whose reset header
    trails cannot stop rotation; honouring a zero from *any* response would let a dead
    window's final count park a pool that has already refilled -- the F9 bug via the escape
    hatch. The status is what tells the two apart.
    """
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    state = authenticator._states["token_1"]["rest"]
    previous_window = int(state.reset_at.timestamp())
    authenticator.update_from_response(
        request,
        _response(
            headers={
                "X-RateLimit-Remaining": "5000",
                "X-RateLimit-Reset": str(previous_window + 3600),
            }
        ),
    )
    assert state.remaining == 5000

    stale = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(previous_window)}
    authenticator.update_from_response(request, _response(status_code=200, headers=stale))
    assert state.remaining == 5000, "a zero from a successful dead-window response must be ignored"

    # ...but the same zero on a rate-limited response is an exhaustion signal and still lands
    authenticator.update_from_response(request, _response(status_code=429, headers=stale))
    assert state.remaining == 0


def test_count_from_a_rolled_over_window_does_not_clamp_the_fresh_pool(requests_mock):
    """A slow response that lands after the window turned describes a window that no longer
    exists. `min` would pin the refilled pool to that dead count for the rest of the hour, and
    nothing short of the next rollover would undo it."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    state = authenticator._states["token_1"]["rest"]
    previous_window = int(state.reset_at.timestamp())

    # the window turns over
    authenticator.update_from_response(
        request,
        _response(
            headers={
                "X-RateLimit-Remaining": "5000",
                "X-RateLimit-Reset": str(previous_window + 3600),
            }
        ),
    )
    assert state.remaining == 5000

    # ...and only now does an in-flight response from the old window arrive
    authenticator.update_from_response(
        request,
        _response(
            headers={"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": str(previous_window)}
        ),
    )

    assert state.remaining == 5000
    assert int(state.reset_at.timestamp()) == previous_window + 3600


def test_limit_from_an_older_window_is_not_applied(requests_mock):
    """The limit is accepted on the same terms as the reset it arrived with.

    Taking a previous window's limit would skew the throttling reserve and, on a later
    reset-only response, refill the pool to the wrong capacity.
    """
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    state = authenticator._states["token_1"]["rest"]
    current_window = int(state.reset_at.timestamp())

    authenticator.update_from_response(
        request,
        _response(
            headers={
                "X-RateLimit-Limit": "1000",
                "X-RateLimit-Reset": str(current_window - 3600),
            }
        ),
    )

    assert state.limit == 5000


def test_limit_header_updates_the_throttling_reserve(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())

    authenticator.update_from_response(request, _response(headers={"X-RateLimit-Limit": "15000"}))

    assert authenticator._states["token_1"]["rest"].limit == 15000


def test_exhaustion_status_code_zeroes_the_pool_and_next_request_rotates(requests_mock):
    """Regression test for the core defect.

    GitHub rejects a request for rate-limit reasons while the local counter still reads healthy.
    Before response awareness the authenticator kept handing out the same token and the error
    handler waited out the whole reset window with other tokens sitting idle.
    """
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator()
    request = authenticator(_prepared_request())
    assert request.headers["Authorization"] == "token token_1"
    assert authenticator._states["token_1"]["rest"].remaining == 4999  # locally still healthy

    authenticator.update_from_response(request, _response(status_code=429))

    assert authenticator._states["token_1"]["rest"].remaining == 0
    retry = authenticator(_prepared_request())
    assert retry.headers["Authorization"] == "token token_2"


def test_exhaustion_is_not_masked_by_an_earlier_reset_header(requests_mock):
    """A rate-limited response must zero the pool even when its reset header is older than the
    one we hold. Dropping the whole update in that case would silently disable rotation, since
    the seeding read and the response headers routinely disagree by a few seconds."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body(reset_in_seconds=3600))
    authenticator = _response_aware_authenticator()
    request = authenticator(_prepared_request())
    earlier = int(authenticator._states["token_1"]["rest"].reset_at.timestamp()) - 1800

    authenticator.update_from_response(
        request,
        _response(
            status_code=429,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(earlier)},
        ),
    )

    assert authenticator._states["token_1"]["rest"].remaining == 0
    # the window itself is never moved backwards
    assert int(authenticator._states["token_1"]["rest"].reset_at.timestamp()) == earlier + 1800
    assert authenticator(_prepared_request()).headers["Authorization"] == "token token_2"


def test_reset_only_response_restores_the_pool_on_a_new_window(requests_mock):
    """A response that proves the window rolled over is actionable on its own: the previous
    count belongs to a window that no longer exists."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    state = authenticator._states["token_1"]["rest"]
    state.remaining = 0
    next_window = int(state.reset_at.timestamp()) + 3600

    authenticator.update_from_response(
        request, _response(headers={"X-RateLimit-Reset": str(next_window)})
    )

    assert state.remaining == 5000  # the pool's full limit, for the window just opened
    assert int(state.reset_at.timestamp()) == next_window


def test_rate_limited_response_is_ignored_when_no_headers_are_configured(requests_mock):
    """Pools configured only with paths keep their previous behaviour."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _authenticator(tokens=("token_1", "token_2"))
    request = authenticator(_prepared_request())

    authenticator.update_from_response(
        request, _response(status_code=429, headers={"X-RateLimit-Remaining": "0"})
    )

    assert authenticator._states["token_1"]["rest"].remaining == 4999
    assert authenticator(_prepared_request()).headers["Authorization"] == "token token_1"


def test_response_is_attributed_to_the_token_that_sent_it(requests_mock):
    """Under concurrency the active token can move on before a response lands, so the update has
    to follow the request's own auth header rather than whichever token is active now."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator()
    request = authenticator(_prepared_request())
    assert request.headers["Authorization"] == "token token_1"
    authenticator._active_token = (
        "token_2"  # the authenticator rotated while the call was in flight
    )

    authenticator.update_from_response(request, _response(headers={"X-RateLimit-Remaining": "12"}))

    assert authenticator._states["token_1"]["rest"].remaining == 12
    assert authenticator._states["token_2"]["rest"].remaining == 5000


def test_cached_response_does_not_update_quota_state(requests_mock):
    """A replayed cached response carries stale headers and consumed no quota. `HttpClient` filters
    those out; this pins the behaviour if anything ever calls the method directly."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())

    from airbyte_cdk.sources.streams.http import HttpClient

    client = HttpClient(name="test", logger=__import__("logging").getLogger("test"))
    client._session.auth = authenticator
    client._update_authenticator_from_response(
        request, _response(headers={"X-RateLimit-Remaining": "1"}, from_cache=True)
    )

    assert authenticator._states["token_1"]["rest"].remaining == 4999


def test_graphql_response_updates_only_the_graphql_pool(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request("https://api.example.com/graphql"))

    authenticator.update_from_response(request, _response(headers={"X-RateLimit-Remaining": "42"}))

    assert authenticator._states["token_1"]["graphql"].remaining == 42
    assert authenticator._states["token_1"]["rest"].remaining == 5000


def test_satisfies_both_authenticator_capability_protocols(requests_mock):
    """`HttpClient` dispatches structurally, but the protocols are the declared contract -- an
    implementation drifting from either one should fail here rather than silently stop being
    consulted at runtime."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator()

    assert isinstance(authenticator, ResponseAwareAuthenticator)
    assert isinstance(authenticator, TokenRotatingAuthenticator)


def test_has_alternative_token_when_sender_is_spent_and_another_is_not(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator()
    request = authenticator(_prepared_request())
    authenticator.update_from_response(request, _response(status_code=429))

    assert authenticator.has_alternative_token(request) is True


def test_no_alternative_token_while_the_sending_token_still_has_calls(requests_mock):
    """A rejection that did not exhaust the sending token is not something rotation fixes --
    a secondary limit is typically per-account and would reject every token alike, so the
    computed wait must stand."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator()
    request = authenticator(_prepared_request())

    assert authenticator.has_alternative_token(request) is False


def test_no_alternative_token_when_every_token_is_spent(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator()
    request = authenticator(_prepared_request())
    authenticator._ensure_initialized()
    for token in authenticator._tokens:
        authenticator._states[token]["rest"].remaining = 0

    assert authenticator.has_alternative_token(request) is False


def test_no_alternative_token_with_a_single_token(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())
    authenticator.update_from_response(request, _response(status_code=429))

    assert authenticator.has_alternative_token(request) is False


def test_alternative_token_is_scoped_to_the_matched_quota_pool(requests_mock):
    """Availability is answered per pool: exhausting graphql says nothing about rest.

    The graphql pool here declares `remaining_header` but no `exhaustion_status_codes`, so it
    learns it is spent from the header rather than from the status code.
    """
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator()
    graphql_request = authenticator(_prepared_request("https://api.example.com/graphql"))
    rest_request = authenticator(_prepared_request())
    authenticator.update_from_response(
        graphql_request, _response(status_code=429, headers={"X-RateLimit-Remaining": "0"})
    )

    assert authenticator._states["token_1"]["graphql"].remaining == 0
    assert authenticator.has_alternative_token(graphql_request) is True
    # the rest pool is untouched, so nothing about it is "spent" and no rotation is implied
    assert authenticator._states["token_1"]["rest"].remaining == 4999
    assert authenticator.has_alternative_token(rest_request) is False


def test_response_signed_by_something_else_is_not_attributed_to_a_token(requests_mock):
    """The auth header may have been written by another component. Slicing off the configured
    prefix without checking it would leave `_states` membership as the only guard against
    charging the wrong token."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    authenticator._ensure_initialized()
    request = _prepared_request()
    request.headers["Authorization"] = "Bearer token_1"  # different scheme

    authenticator.update_from_response(request, _response(headers={"X-RateLimit-Remaining": "1"}))

    assert authenticator._states["token_1"]["rest"].remaining == 5000


def test_malformed_headers_are_ignored(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    request = authenticator(_prepared_request())

    authenticator.update_from_response(
        request,
        _response(headers={"X-RateLimit-Remaining": "not-a-number", "X-RateLimit-Reset": "soon"}),
    )

    assert authenticator._states["token_1"]["rest"].remaining == 4999


def test_concurrent_response_updates_do_not_lose_decrements(requests_mock):
    """Clamp once, then charge a known number of calls with no further updates, so the assertion
    fails if a single decrement is lost rather than only if the clamp itself is."""
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = _response_aware_authenticator(tokens=("token_1",))
    authenticator._ensure_initialized()
    reset_at = authenticator._states["token_1"]["rest"].reset_at
    seed_request = authenticator(_prepared_request())
    authenticator.update_from_response(
        seed_request,
        _response(
            headers={
                "X-RateLimit-Remaining": "4000",
                "X-RateLimit-Reset": str(int(reset_at.timestamp())),
            }
        ),
    )
    assert authenticator._states["token_1"]["rest"].remaining == 4000

    calls_per_thread, thread_count = 50, 4

    def worker():
        for _ in range(calls_per_thread):
            authenticator(_prepared_request())

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert (
        authenticator._states["token_1"]["rest"].remaining == 4000 - calls_per_thread * thread_count
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


def test_factory_returns_shared_instance_when_only_parameters_differ():
    factory = ModelToComponentFactory()
    config = {"pat": "token_1,token_2"}

    first_model = _model("{{ config['pat'] }}")
    first_model.parameters = {"name": "stream_a"}
    second_model = _model("{{ config['pat'] }}")
    second_model.parameters = {"name": "stream_b"}

    first = factory.create_rate_limited_multiple_token_authenticator(first_model, config)
    second = factory.create_rate_limited_multiple_token_authenticator(second_model, config)

    assert first is second


def test_factory_returns_shared_instance_when_propagated_parameters_differ():
    """Streams with identical authenticator definitions but different propagated `$parameters` must share one instance."""
    factory = ModelToComponentFactory()
    config = {"pat": "token_1,token_2"}
    transformer = ManifestComponentTransformer()

    def _propagated_model(stream_name):
        propagated = transformer.propagate_types_and_parameters(
            "authenticator",
            _model("{{ config['pat'] }}").dict(exclude_none=True, by_alias=True),
            {"name": stream_name, "primary_key": "id"},
        )
        return RateLimitedMultipleTokenAuthenticatorModel.parse_obj(propagated)

    first_model = _propagated_model("stream_a")
    second_model = _propagated_model("stream_b")
    assert first_model.parameters != second_model.parameters

    first = factory.create_rate_limited_multiple_token_authenticator(first_model, config)
    second = factory.create_rate_limited_multiple_token_authenticator(second_model, config)

    assert first is second


def test_factory_filters_empty_list_tokens():
    factory = ModelToComponentFactory()
    model = _model(["{{ config['token_a'] }}", "{{ config['token_b'] }}", " "])

    authenticator = factory.create_rate_limited_multiple_token_authenticator(
        model, {"token_a": "token_1", "token_b": ""}
    )

    assert authenticator._tokens == ["token_1"]


def test_factory_rejects_calendar_unit_max_wait_time():
    factory = ModelToComponentFactory()
    model = _model("{{ config['pat'] }}")
    model.max_wait_time = "P1M"

    with pytest.raises(ValueError, match="calendar-unit"):
        factory.create_rate_limited_multiple_token_authenticator(model, {"pat": "token_1"})


def test_factory_interpolates_max_wait_time():
    factory = ModelToComponentFactory()
    model = _model("{{ config['pat'] }}")
    model.max_wait_time = "PT{{ config.get('max_waiting_time', 120) }}M"

    authenticator = factory.create_rate_limited_multiple_token_authenticator(
        model, {"pat": "token_1", "max_waiting_time": 30}
    )

    assert authenticator._max_wait_time == timedelta(minutes=30)


def test_factory_passes_response_header_fields_through():
    factory = ModelToComponentFactory()
    model = _model("{{ config['pat'] }}")
    model.quotas[0].remaining_header = "X-RateLimit-Remaining"
    model.quotas[0].reset_header = "X-RateLimit-Reset"
    model.quotas[0].limit_header = "X-RateLimit-Limit"
    model.quotas[0].exhaustion_status_codes = [429]

    authenticator = factory.create_rate_limited_multiple_token_authenticator(
        model, {"pat": "token_1"}
    )

    rest, graphql = authenticator._quotas
    assert rest.remaining_header == "X-RateLimit-Remaining"
    assert rest.reset_header == "X-RateLimit-Reset"
    assert rest.limit_header == "X-RateLimit-Limit"
    assert rest.exhaustion_status_codes == [429]
    assert rest.is_response_aware
    assert not graphql.is_response_aware


def test_factory_shares_instances_when_exhaustion_codes_are_omitted_versus_empty():
    """An omitted `exhaustion_status_codes` and an explicit `[]` behave identically at runtime,
    so they must not key differently -- splitting the counters is the very failure this
    component exists to avoid."""
    factory = ModelToComponentFactory()
    config = {"pat": "token_1,token_2"}

    omitted = _model("{{ config['pat'] }}")
    explicit_empty = _model("{{ config['pat'] }}")
    explicit_empty.quotas[0].exhaustion_status_codes = []

    assert factory.create_rate_limited_multiple_token_authenticator(
        omitted, config
    ) is factory.create_rate_limited_multiple_token_authenticator(explicit_empty, config)


def test_factory_does_not_share_instances_across_differing_header_config():
    """Header config is part of the authenticator's behaviour, so it must take part in the
    instance cache key -- otherwise two differently configured pools would silently share state."""
    factory = ModelToComponentFactory()
    config = {"pat": "token_1,token_2"}

    plain = factory.create_rate_limited_multiple_token_authenticator(
        _model("{{ config['pat'] }}"), config
    )
    response_aware_model = _model("{{ config['pat'] }}")
    response_aware_model.quotas[0].remaining_header = "X-RateLimit-Remaining"
    response_aware = factory.create_rate_limited_multiple_token_authenticator(
        response_aware_model, config
    )

    assert plain is not response_aware
