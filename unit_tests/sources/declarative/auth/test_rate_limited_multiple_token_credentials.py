#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
"""`RateLimitedMultipleTokenAuthenticator` with `credentials`: rotation across credentials that
log in and refresh their own tokens, with quota state kept per credential key."""

import json
import threading
import time
from typing import Any, List, Mapping, Optional

import freezegun
import pytest
import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.auth.rate_limited_multiple_token import (
    RateLimitedMultipleTokenAuthenticator,
    RotatingCredential,
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
from airbyte_cdk.utils import AirbyteTracedException

API_URL = "https://api.example.com"
QUOTA_STATUS_URL = f"{API_URL}/rate_limit"


def _quota_status_body(remaining: int = 5000) -> Mapping[str, Any]:
    return {"core": {"remaining": remaining, "reset": int(time.time()) + 3600, "limit": 5000}}


def _quotas() -> List[TokenQuota]:
    return [
        TokenQuota(
            name="rest",
            remaining_path=["core", "remaining"],
            reset_path=["core", "reset"],
            limit_path=["core", "limit"],
            remaining_header="X-RateLimit-Remaining",
            reset_header="X-RateLimit-Reset",
            exhaustion_status_codes=[429],
        )
    ]


class _RefreshingCredential(DeclarativeAuthenticator):
    """Stands in for a login-based authenticator: its token changes whenever it is refreshed."""

    def __init__(self, name: str, header: str = "Authorization") -> None:
        self.name = name
        self.generation = 1
        self._header = header

    def refresh(self) -> None:
        self.generation += 1

    @property
    def auth_header(self) -> str:
        return self._header

    @property
    def token(self) -> str:
        return f"Bearer {self.name}-v{self.generation}"


def _authenticator(
    credentials: List[RotatingCredential], credentials_error: Optional[str] = None
) -> RateLimitedMultipleTokenAuthenticator:
    return RateLimitedMultipleTokenAuthenticator(
        tokens=[],
        quotas=_quotas(),
        quota_status_url=QUOTA_STATUS_URL,
        credentials=credentials,
        credentials_error=credentials_error,
    )


def _two_credentials():
    first, second = _RefreshingCredential("a"), _RefreshingCredential("b")
    return first, second, [RotatingCredential("a", first), RotatingCredential("b", second)]


def _request() -> requests.PreparedRequest:
    return requests.Request(method="GET", url=f"{API_URL}/items").prepare()


def _response(status_code: int = 200, headers: Optional[Mapping[str, str]] = None):
    response = requests.Response()
    response.status_code = status_code
    response.headers.update(headers or {})
    return response


def test_requests_are_signed_with_the_credentials_live_header(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    _, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials)

    request = authenticator(_request())

    assert request.headers["Authorization"] == "Bearer a-v1"
    # each credential's quota is seeded with that credential's own token
    assert sorted(r.headers["Authorization"] for r in requests_mock.request_history) == [
        "Bearer a-v1",
        "Bearer b-v1",
    ]
    assert sorted(authenticator._states) == ["a", "b"]


def test_a_refreshed_token_keeps_the_credentials_quota_state(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    first, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials)
    authenticator(_request())

    first.refresh()
    request = authenticator(_request())

    assert request.headers["Authorization"] == "Bearer a-v2"
    assert authenticator._states["a"]["rest"].remaining == 4998


def test_a_response_signed_before_a_refresh_is_attributed_to_its_credential(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    first, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials)
    in_flight = authenticator(_request())
    first.refresh()
    authenticator(_request())

    authenticator.update_from_response(in_flight, _response(headers={"X-RateLimit-Remaining": "7"}))

    assert in_flight.headers["Authorization"] == "Bearer a-v1"
    assert authenticator._states["a"]["rest"].remaining == 7
    assert authenticator._states["b"]["rest"].remaining == 5000


def test_header_values_are_forgotten_after_enough_refreshes(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    first, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials)
    oldest = authenticator(_request())
    for _ in range(RateLimitedMultipleTokenAuthenticator.ISSUED_HEADER_VALUES_PER_CREDENTIAL):
        first.refresh()
        authenticator(_request())

    assert authenticator._token_from_request(oldest) is None
    assert len(authenticator._issued_header_values) == (
        RateLimitedMultipleTokenAuthenticator.ISSUED_HEADER_VALUES_PER_CREDENTIAL
    )


def test_a_rejected_credential_rotates_onto_the_next_one(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    _, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials)
    request = authenticator(_request())

    authenticator.update_from_response(request, _response(status_code=429))

    assert authenticator.has_alternative_token(request)
    assert authenticator(_request()).headers["Authorization"] == "Bearer b-v1"


def test_a_header_value_the_authenticator_never_issued_is_not_attributed(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    _, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials)
    authenticator(_request())
    foreign = _request()
    foreign.headers["Authorization"] = "Bearer someone-else"

    authenticator.update_from_response(foreign, _response(headers={"X-RateLimit-Remaining": "1"}))

    assert authenticator._states["a"]["rest"].remaining == 4999


def test_an_empty_credential_list_fails_only_on_first_use(requests_mock):
    authenticator = _authenticator([])

    with pytest.raises(AirbyteTracedException) as error:
        authenticator(_request())

    assert error.value.failure_type == FailureType.config_error
    assert "missing" in error.value.message
    assert not requests_mock.called


def test_a_credentials_error_is_reported_on_first_use(requests_mock):
    authenticator = _authenticator(
        [], credentials_error="The credentials must be a list of objects."
    )

    with pytest.raises(AirbyteTracedException, match="must be a list of objects") as error:
        authenticator(_request())

    assert error.value.failure_type == FailureType.config_error


def test_duplicate_credential_keys_fail_on_first_use(requests_mock):
    authenticator = _authenticator(
        [
            RotatingCredential("same", _RefreshingCredential("a")),
            RotatingCredential("same", _RefreshingCredential("b")),
        ]
    )

    with pytest.raises(AirbyteTracedException, match="key 'same'"):
        authenticator(_request())
    assert not requests_mock.called


def test_tokens_and_credentials_together_are_rejected():
    with pytest.raises(ValueError, match="not both"):
        RateLimitedMultipleTokenAuthenticator(
            tokens=["token_1"],
            quotas=_quotas(),
            quota_status_url=QUOTA_STATUS_URL,
            credentials=[RotatingCredential("a", _RefreshingCredential("a"))],
        )


def test_credentials_injecting_into_different_headers_are_rejected_on_first_use():
    authenticator = _authenticator(
        [
            RotatingCredential("a", _RefreshingCredential("a")),
            RotatingCredential("b", _RefreshingCredential("b", header="X-Api-Key")),
        ]
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator(_request())

    assert "same request header" in error.value.internal_message


@pytest.mark.parametrize(
    "use",
    [
        pytest.param(lambda authenticator: authenticator(_request()), id="call"),
        pytest.param(lambda authenticator: authenticator.token, id="token"),
        pytest.param(lambda authenticator: authenticator.get_auth_header(), id="get_auth_header"),
    ],
)
@pytest.mark.parametrize(
    "credentials, credentials_error, expected_message",
    [
        pytest.param([], None, "missing", id="empty"),
        pytest.param(
            [RotatingCredential("a", _RefreshingCredential("a"))],
            "The credentials must be a list of objects.",
            "list of objects",
            id="malformed",
        ),
        pytest.param(
            [
                RotatingCredential("a", _RefreshingCredential("a")),
                RotatingCredential("a", _RefreshingCredential("b")),
            ],
            None,
            "own key",
            id="duplicate_key",
        ),
    ],
)
def test_an_unusable_credential_list_is_a_config_error_however_it_is_first_used(
    requests_mock, use, credentials, credentials_error, expected_message
):
    authenticator = _authenticator(credentials, credentials_error=credentials_error)

    with pytest.raises(AirbyteTracedException, match=expected_message) as error:
        use(authenticator)

    assert error.value.failure_type == FailureType.config_error
    assert not requests_mock.called


def test_an_unchanged_token_stays_attributed_over_many_requests(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    _, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials[:1])

    requests = [
        authenticator(_request())
        for _ in range(authenticator.ISSUED_HEADER_VALUES_PER_CREDENTIAL + 2)
    ]

    assert all(authenticator._token_from_request(request) == "a" for request in requests)


def test_concurrent_requests_are_all_attributed_to_their_credential(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    _, _, credentials = _two_credentials()
    authenticator = _authenticator(credentials)
    signed: List[requests.PreparedRequest] = []
    signed_lock = threading.Lock()

    def _send() -> None:
        for _ in range(20):
            request = authenticator(_request())
            with signed_lock:
                signed.append(request)

    threads = [threading.Thread(target=_send) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(signed) == 160
    assert {authenticator._token_from_request(request) for request in signed} <= {"a", "b"}
    assert all(authenticator._token_from_request(request) is not None for request in signed)


# --- factory ------------------------------------------------------------------------------


def _session_authenticator(
    login_requester_updates: Optional[Mapping[str, Any]] = None, **updates: Any
) -> Mapping[str, Any]:
    authenticator: Mapping[str, Any] = {
        "type": "SessionTokenAuthenticator",
        "login_requester": {
            "type": "HttpRequester",
            "url_base": API_URL,
            "path": "/login/{{ config['credential']['id'] }}",
            "http_method": "POST",
            "request_body_json": {"secret": "{{ config['credential']['secret'] }}"},
            **(login_requester_updates or {}),
        },
        "session_token_path": ["token"],
        "expiration_duration": "PT55M",
        "request_authentication": {"type": "Bearer"},
        **updates,
    }
    return {key: value for key, value in authenticator.items() if value is not None}


def _model(
    items: Any = "{{ config['accounts'] }}",
    key: Optional[str] = "{{ config['credential']['id'] }}",
    tokens: Any = None,
    authenticator: Optional[Mapping[str, Any]] = None,
    **definition_updates: Any,
) -> RateLimitedMultipleTokenAuthenticatorModel:
    credentials: Mapping[str, Any] = {
        "type": "CredentialSet",
        "items": items,
        "authenticator": authenticator or _session_authenticator(),
    }
    if key is not None:
        credentials = {**credentials, "key": key}
    definition: Mapping[str, Any] = {
        "type": "RateLimitedMultipleTokenAuthenticator",
        "credentials": credentials,
        "quota_status_source": {"type": "QuotaStatusSource", "url": QUOTA_STATUS_URL},
        "quotas": [
            {
                "type": "TokenQuota",
                "name": "rest",
                "remaining_path": ["core", "remaining"],
                "reset_path": ["core", "reset"],
            }
        ],
    }
    if tokens is not None:
        definition = {**definition, "tokens": tokens}
    return RateLimitedMultipleTokenAuthenticatorModel.parse_obj(
        {**definition, **definition_updates}
    )


def _propagated(
    model: RateLimitedMultipleTokenAuthenticatorModel, parameters: Mapping[str, Any]
) -> RateLimitedMultipleTokenAuthenticatorModel:
    """The model as a stream with these `$parameters` would see it after propagation."""
    return RateLimitedMultipleTokenAuthenticatorModel.parse_obj(
        ManifestComponentTransformer().propagate_types_and_parameters(
            "authenticator", model.dict(exclude_none=True, by_alias=True), parameters
        )
    )


ACCOUNTS = [{"id": "111", "secret": "s1"}, {"id": "222", "secret": "s2"}]


def _mock_logins(requests_mock):
    for account in ACCOUNTS:
        requests_mock.post(
            f"{API_URL}/login/{account['id']}", json={"token": f"session-{account['id']}"}
        )
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())


def test_factory_builds_one_credential_per_item_and_logs_in_only_on_first_use(requests_mock):
    _mock_logins(requests_mock)

    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": ACCOUNTS}, name="stream"
    )

    assert not requests_mock.called
    assert authenticator._tokens == ["111", "222"]
    request = authenticator(_request())
    assert request.headers["Authorization"] == "Bearer session-111"
    login_bodies = [json.loads(r.body) for r in requests_mock.request_history if r.method == "POST"]
    assert login_bodies == [{"secret": "s1"}, {"secret": "s2"}]


def test_factory_numeric_looking_keys_stay_strings(requests_mock):
    _mock_logins(requests_mock)

    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": ACCOUNTS}, name="stream"
    )

    assert all(isinstance(key, str) for key in authenticator._tokens)


def test_factory_defaults_keys_to_positions():
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(key=None), {"accounts": ACCOUNTS}, name="stream"
    )

    assert authenticator._tokens == ["0", "1"]


@pytest.mark.parametrize(
    "authenticator",
    [
        pytest.param(None, id="bearer"),
        pytest.param(
            _session_authenticator(
                request_authentication={
                    "type": "ApiKey",
                    "inject_into": {
                        "type": "RequestOption",
                        "inject_into": "header",
                        "field_name": "X-Api-Key",
                    },
                }
            ),
            id="api_key",
        ),
    ],
)
def test_factory_shares_one_instance_across_streams_whose_parameters_differ(authenticator):
    factory = ModelToComponentFactory()

    def _stream_model(stream_name, primary_key, data_field):
        # The usual stream `$parameters`: propagation copies the keys the login requester does
        # not declare onto it as extras, which must not split the instance.
        return _propagated(
            _model(authenticator=authenticator),
            {"name": stream_name, "primary_key": primary_key, "data_field": data_field},
        )

    first_model = _stream_model("stream_a", "id", "users")
    second_model = _stream_model("stream_b", "team_id", "teams")
    assert (
        first_model.credentials.authenticator.parameters
        != second_model.credentials.authenticator.parameters
    )

    first = factory.create_rate_limited_multiple_token_authenticator(
        first_model, {"accounts": ACCOUNTS}, name="stream_a"
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        second_model, {"accounts": ACCOUNTS}, name="stream_b"
    )

    assert first is second


def test_factory_does_not_share_instances_across_different_credentials():
    factory = ModelToComponentFactory()

    first = factory.create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": ACCOUNTS}
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": [{"id": "111", "secret": "rotated"}]}
    )

    assert first is not second


def test_factory_cache_key_never_contains_credential_values():
    factory = ModelToComponentFactory()

    factory.create_rate_limited_multiple_token_authenticator(_model(), {"accounts": ACCOUNTS})

    (cache_key,) = factory._rate_limited_authenticators
    assert "s1" not in cache_key and "s2" not in cache_key


def test_factory_tokens_definition_keys_exactly_as_before():
    factory = ModelToComponentFactory()
    model = RateLimitedMultipleTokenAuthenticatorModel.parse_obj(
        {
            "type": "RateLimitedMultipleTokenAuthenticator",
            "tokens": "{{ config['pat'] }}",
            "quota_status_source": {"type": "QuotaStatusSource", "url": QUOTA_STATUS_URL},
            "quotas": [
                {
                    "type": "TokenQuota",
                    "name": "rest",
                    "remaining_path": ["core", "remaining"],
                    "reset_path": ["core", "reset"],
                }
            ],
        }
    )

    factory.create_rate_limited_multiple_token_authenticator(model, {"pat": "token_1"})

    # The key the base commit (before `credentials` existed) produces for this definition.
    (cache_key,) = factory._rate_limited_authenticators
    assert json.loads(cache_key) == {
        "auth_method": "Bearer",
        "budget_min_reserve": 50,
        "budget_reserve_fraction": 0.1,
        "header": "Authorization",
        "max_wait_time": 7200.0,
        "quota_status_headers": {},
        "quota_status_http_method": "GET",
        "quota_status_unavailable_status_codes": [],
        "quota_status_url": QUOTA_STATUS_URL,
        "quotas": [
            {
                "exhaustion_status_codes": [],
                "limit_header": None,
                "limit_path": None,
                "matchers": [],
                "name": "rest",
                "remaining_header": None,
                "remaining_path": ["core", "remaining"],
                "reset_header": None,
                "reset_path": ["core", "reset"],
            }
        ],
        "tokens": ["token_1"],
    }


def test_factory_does_not_share_instances_across_credentials_that_differ_only_in_secrets():
    factory = ModelToComponentFactory()
    rotated = [{**account, "secret": f"rotated-{account['secret']}"} for account in ACCOUNTS]

    first = factory.create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": ACCOUNTS}
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": rotated}
    )

    assert first is not second


def test_factory_does_not_share_instances_across_different_login_definitions():
    factory = ModelToComponentFactory()

    first = factory.create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": ACCOUNTS}
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _model(
            authenticator=_session_authenticator(
                {"path": "/v2/login/{{ config['credential']['id'] }}"}
            )
        ),
        {"accounts": ACCOUNTS},
    )

    assert first is not second


def test_factory_keys_a_login_that_reads_parameters_by_those_parameters(requests_mock):
    # `login_prefix` reaches the login requester only as an undeclared extra, which the cache key
    # ignores, so only the `parameters` reference in `path` can tell the two streams apart.
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    for prefix in ("login", "alt"):
        requests_mock.post(f"{API_URL}/{prefix}/111", json={"token": f"{prefix}-111"})
    model = _model(
        authenticator=_session_authenticator(
            {"path": "/{{ parameters['login_prefix'] }}/{{ config['credential']['id'] }}"}
        )
    )
    factory = ModelToComponentFactory()
    config = {"accounts": ACCOUNTS[:1]}

    first = factory.create_rate_limited_multiple_token_authenticator(
        _propagated(model, {"name": "stream_a", "login_prefix": "login"}), config, name="stream_a"
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _propagated(model, {"name": "stream_b", "login_prefix": "alt"}), config, name="stream_b"
    )

    assert first is not second
    assert first(_request()).headers["Authorization"] == "Bearer login-111"
    assert second(_request()).headers["Authorization"] == "Bearer alt-111"


def test_factory_keeps_a_field_that_happens_to_equal_a_parameter():
    factory = ModelToComponentFactory()
    config = {"accounts": ACCOUNTS}

    first = factory.create_rate_limited_multiple_token_authenticator(
        _propagated(
            _model(authenticator=_session_authenticator({"url_base": "https://a.example.com"})),
            {"name": "stream_a", "url_base": "https://a.example.com"},
        ),
        config,
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _propagated(
            _model(authenticator=_session_authenticator({"url_base": "https://b.example.com"})),
            {"name": "stream_b", "url_base": "https://b.example.com"},
        ),
        config,
    )

    assert first is not second


def test_factory_api_key_header_credentials_log_in_only_on_first_use(requests_mock):
    _mock_logins(requests_mock)
    model = _model(
        authenticator=_session_authenticator(
            request_authentication={
                "type": "ApiKey",
                "inject_into": {
                    "type": "RequestOption",
                    "inject_into": "header",
                    "field_name": "X-Api-Key",
                },
            }
        )
    )

    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        model, {"accounts": ACCOUNTS}, name="stream"
    )

    assert not requests_mock.called
    request = authenticator(_request())
    assert request.headers["X-Api-Key"] == "session-111"
    assert "Authorization" not in request.headers


def test_factory_rejects_credentials_injected_outside_a_header(requests_mock):
    model = _model(
        authenticator=_session_authenticator(
            request_authentication={
                "type": "ApiKey",
                "inject_into": {
                    "type": "RequestOption",
                    "inject_into": "request_parameter",
                    "field_name": "api_key",
                },
            }
        )
    )

    with pytest.raises(ValueError, match="request header"):
        ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
            model, {"accounts": ACCOUNTS}
        )

    assert not requests_mock.called


def test_factory_rejects_credentials_without_an_expiration_duration():
    model = _model(authenticator=_session_authenticator(expiration_duration=None))

    with pytest.raises(ValueError, match="expiration_duration"):
        ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
            model, {"accounts": ACCOUNTS}
        )


@pytest.mark.parametrize(
    "field, value",
    [
        pytest.param("header", "X-Api-Key", id="header"),
        pytest.param("auth_method", "token", id="auth_method"),
        pytest.param("token_delimiter", ";", id="token_delimiter"),
    ],
)
def test_factory_rejects_static_token_settings_together_with_credentials(field, value):
    with pytest.raises(ValueError, match=field):
        ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
            _model(**{field: value}), {"accounts": ACCOUNTS}
        )


def test_a_failed_login_names_the_credential(requests_mock):
    requests_mock.post(f"{API_URL}/login/111", status_code=401, json={"message": "bad secret"})
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": ACCOUNTS[:1]}
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator(_request())

    assert "'111'" in error.value.message


def test_a_session_refresh_keeps_the_credentials_quota_state(requests_mock):
    logins = iter(["session-1", "session-2"])
    requests_mock.post(
        f"{API_URL}/login/111", json=lambda request, context: {"token": next(logins)}
    )
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body(remaining=4000))

    with freezegun.freeze_time("2026-01-01T00:00:00Z") as frozen:
        authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
            _model(), {"accounts": ACCOUNTS[:1]}
        )
        before = authenticator(_request())
        states = authenticator._states["111"]
        assert states["rest"].remaining == 3999

        frozen.tick(delta=3600)
        after = authenticator(_request())

    assert before.headers["Authorization"] == "Bearer session-1"
    assert after.headers["Authorization"] == "Bearer session-2"
    assert authenticator._states["111"] is states
    assert states["rest"].remaining == 3998  # the refreshed token spends from the same counter
    assert authenticator._token_from_request(before) == "111"
    assert authenticator._token_from_request(after) == "111"


@pytest.mark.parametrize(
    "config, expected_message",
    [
        pytest.param({}, "missing", id="field_absent"),
        pytest.param({"accounts": []}, "missing", id="empty_list"),
        pytest.param({"accounts": "not-a-list"}, "must be a list of objects", id="not_a_list"),
        pytest.param(
            {"accounts": [{"secret": "s1"}]}, "position 1 has no identifier", id="missing_key"
        ),
    ],
)
def test_factory_builds_unusable_credentials_without_failing_and_reports_them_on_use(
    requests_mock, config, expected_message
):
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(), config
    )

    with pytest.raises(AirbyteTracedException, match=expected_message) as error:
        authenticator(_request())

    assert error.value.failure_type == FailureType.config_error
    assert not requests_mock.called


@pytest.mark.parametrize(
    "tokens, items",
    [
        pytest.param("{{ config['pat'] }}", "{{ config['accounts'] }}", id="both"),
    ],
)
def test_factory_rejects_tokens_together_with_credentials(tokens, items):
    with pytest.raises(ValueError, match="exactly one of"):
        ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
            _model(items=items, tokens=tokens), {"pat": "token_1", "accounts": ACCOUNTS}
        )


def test_factory_rejects_a_definition_with_neither_tokens_nor_credentials():
    model = _model()
    model.credentials = None

    with pytest.raises(ValueError, match="exactly one of"):
        ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(model, {})


# --- end to end through a manifest ------------------------------------------------------


def _manifest() -> Mapping[str, Any]:
    quota_settings = {
        "quota_status_source": {"type": "QuotaStatusSource", "url": QUOTA_STATUS_URL},
        "quotas": [
            {
                "type": "TokenQuota",
                "name": "rest",
                "remaining_path": ["core", "remaining"],
                "reset_path": ["core", "reset"],
                "remaining_header": "X-RateLimit-Remaining",
                "reset_header": "X-RateLimit-Reset",
            }
        ],
    }
    requester = {
        "type": "HttpRequester",
        "url_base": API_URL,
        "path": "/{{ parameters['name'] }}",
        "http_method": "GET",
        "authenticator": {
            "type": "SelectiveAuthenticator",
            "authenticator_selection_path": ["auth_mode"],
            "authenticators": {
                "tokens": {
                    "type": "RateLimitedMultipleTokenAuthenticator",
                    "tokens": "{{ config.get('pat', 'placeholder') }}",
                    **quota_settings,
                },
                "accounts": {
                    "type": "RateLimitedMultipleTokenAuthenticator",
                    "credentials": {
                        "type": "CredentialSet",
                        "items": "{{ config.get('accounts') }}",
                        "key": "{{ config['credential']['id'] }}",
                        "authenticator": {
                            "type": "SessionTokenAuthenticator",
                            "login_requester": {
                                "type": "HttpRequester",
                                "url_base": API_URL,
                                "path": "/login/{{ config['credential']['id'] }}",
                                "http_method": "POST",
                            },
                            "session_token_path": ["token"],
                            "expiration_duration": "PT55M",
                            "request_authentication": {"type": "Bearer"},
                        },
                    },
                    **quota_settings,
                },
            },
        },
    }

    def _stream(name: str) -> Mapping[str, Any]:
        return {
            "type": "DeclarativeStream",
            "name": name,
            "primary_key": [],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": requester,
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
            },
            "$parameters": {"name": name, "primary_key": "id", "data_field": name},
        }

    return {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["users"]},
        "streams": [_stream("users"), _stream("teams")],
        "spec": {
            "type": "Spec",
            "connection_specification": {
                "type": "object",
                "properties": {},
                "additionalProperties": True,
            },
        },
    }


def _authenticators_of(source, config):
    return [
        stream._stream_partition_generator._partition_factory._retriever.requester.authenticator
        for stream in source.streams(config)
    ]


def test_streams_share_one_authenticator_and_log_in_once_per_credential(requests_mock):
    from airbyte_cdk.models import SyncMode
    from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
        ConcurrentDeclarativeSource,
    )
    from airbyte_cdk.test.catalog_builder import CatalogBuilder
    from airbyte_cdk.test.entrypoint_wrapper import read

    config = {"auth_mode": "accounts", "accounts": ACCOUNTS}
    logins = {
        account["id"]: requests_mock.post(
            f"{API_URL}/login/{account['id']}", json={"token": f"session-{account['id']}"}
        )
        for account in ACCOUNTS
    }
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    requests_mock.get(f"{API_URL}/users", json=[{"id": 1}])
    requests_mock.get(f"{API_URL}/teams", json=[{"id": 2}])
    catalog = (
        CatalogBuilder()
        .with_stream("users", SyncMode.full_refresh)
        .with_stream("teams", SyncMode.full_refresh)
        .build()
    )
    source = ConcurrentDeclarativeSource(
        source_config=_manifest(), config=config, catalog=catalog, state=None
    )

    authenticators = _authenticators_of(source, config)
    output = read(source, config, catalog)

    assert len({id(authenticator) for authenticator in authenticators}) == 1
    assert len(output.records) == 2
    assert {login.call_count for login in logins.values()} == {1}
    data_requests = [r for r in requests_mock.request_history if r.path in ("/users", "/teams")]
    assert {r.headers["Authorization"] for r in data_requests} == {"Bearer session-111"}


def test_a_config_selecting_static_tokens_is_unaffected_by_the_credentials_branch(requests_mock):
    from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
        ConcurrentDeclarativeSource,
    )

    config = {"auth_mode": "tokens", "pat": "token_1"}
    source = ConcurrentDeclarativeSource(
        source_config=_manifest(), config=config, catalog=None, state=None
    )

    authenticators = _authenticators_of(source, config)

    assert len({id(authenticator) for authenticator in authenticators}) == 1
    assert authenticators[0]._tokens == ["token_1"]
    assert not requests_mock.called


def test_factory_keeps_credential_ids_that_look_like_numbers_distinct():
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(), {"accounts": [{"id": "1_000", "secret": "a"}, {"id": "1000", "secret": "b"}]}
    )

    assert authenticator._tokens == ["1_000", "1000"]
    assert authenticator._credentials_error is None


def test_factory_does_not_share_logins_whose_body_differs_in_a_field_named_parameters():
    factory = ModelToComponentFactory()

    def _with_scope(scope: str) -> RateLimitedMultipleTokenAuthenticatorModel:
        return _model(
            authenticator=_session_authenticator(
                {"request_body_json": {"parameters": {"scope": scope}}}
            )
        )

    first = factory.create_rate_limited_multiple_token_authenticator(
        _with_scope("read"), {"accounts": ACCOUNTS}
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _with_scope("write"), {"accounts": ACCOUNTS}
    )

    assert first is not second


def _api_key_model() -> RateLimitedMultipleTokenAuthenticatorModel:
    return _model(
        authenticator=_session_authenticator(
            request_authentication={
                "type": "ApiKey",
                "inject_into": {
                    "type": "RequestOption",
                    "inject_into": "header",
                    "field_name": "X-Api-Key",
                },
            }
        )
    )


@pytest.mark.parametrize(
    "model_factory, login_response, failure_type",
    [
        # A response without a token is the API misbehaving, not the user's config.
        pytest.param(
            _model,
            {"status_code": 200, "json": {}},
            FailureType.system_error,
            id="bearer_no_token",
        ),
        # A rejected login keeps the failure type of the error it arrived as.
        pytest.param(
            _api_key_model,
            {"status_code": 401, "json": {"message": "no"}},
            FailureType.config_error,
            id="api_key_401",
        ),
        pytest.param(
            _api_key_model,
            {"status_code": 200, "json": {}},
            FailureType.system_error,
            id="api_key_no_token",
        ),
    ],
)
def test_every_kind_of_failed_login_names_the_credential(
    requests_mock, model_factory, login_response, failure_type
):
    requests_mock.post(f"{API_URL}/login/111", json={"token": "session-111"})
    requests_mock.post(f"{API_URL}/login/222", **login_response)
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        model_factory(), {"accounts": ACCOUNTS}
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator(_request())

    assert "'222'" in error.value.message
    assert error.value.failure_type == failure_type


def _custom_login_authenticator(scope: str) -> Mapping[str, Any]:
    return _session_authenticator(
        {
            "authenticator": {
                "type": "CustomAuthenticator",
                "class_name": "components.LoginSigner",
                "scope": scope,
            }
        }
    )


def test_cache_key_keeps_the_settings_of_a_custom_component_in_the_login():
    def _key(scope: str) -> Any:
        return ModelToComponentFactory._credential_authenticator_cache_key(
            _model(authenticator=_custom_login_authenticator(scope)).credentials.authenticator
        )

    assert _key("read") != _key("admin")
    assert _key("read") == _key("read")


def test_cache_key_keys_a_custom_component_with_the_parameters_it_receives():
    def _key(stream_name: str) -> Any:
        model = _propagated(
            _model(authenticator=_custom_login_authenticator("read")), {"name": stream_name}
        )
        return ModelToComponentFactory._credential_authenticator_cache_key(
            model.credentials.authenticator
        )

    # A custom component receives the stream's parameters, so it is not shared across streams.
    assert _key("stream_a") != _key("stream_b")


def test_factory_does_not_share_instances_across_configs_the_login_reads():
    factory = ModelToComponentFactory()
    model = _model(authenticator=_session_authenticator({"url_base": "{{ config['api_url'] }}"}))

    first = factory.create_rate_limited_multiple_token_authenticator(
        model, {"accounts": ACCOUNTS, "api_url": "https://eu.example.com"}
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        model, {"accounts": ACCOUNTS, "api_url": "https://us.example.com"}
    )

    assert first is not second


# --- stream `$parameters` and the shared login ---------------------------------------------


def test_stream_parameters_do_not_fill_the_login_so_streams_still_share_it():
    factory = ModelToComponentFactory()
    # The login sets `url_base` and `path` and leaves `url` empty, which a stream's `url` would
    # otherwise fill: the login request is the same, but the definition would differ per stream.
    streams = [
        _propagated(_model(), {"name": name, "url": f"{API_URL}/{name}", "use_cache": True})
        for name in ("users", "teams")
    ]

    first, second = (
        factory.create_rate_limited_multiple_token_authenticator(model, {"accounts": ACCOUNTS})
        for model in streams
    )

    assert first is second
    login_requester = streams[0].credentials.authenticator.login_requester
    assert login_requester.url is None
    assert not login_requester.use_cache
    # The stream's parameters are still there to be read as `parameters`.
    assert login_requester.parameters["url"] == f"{API_URL}/users"


def test_parameters_declared_inside_the_login_still_fill_its_fields(requests_mock):
    requests_mock.post(f"{API_URL}/sessions", json={"token": "session"})
    requests_mock.get(QUOTA_STATUS_URL, json=_quota_status_body())
    login = _session_authenticator({"path": None, "$parameters": {"path": "/sessions"}})
    login["login_requester"] = {
        key: value for key, value in login["login_requester"].items() if value is not None
    }
    model = _propagated(_model(authenticator=login), {"name": "users", "path": "/users"})

    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        model, {"accounts": ACCOUNTS[:1]}
    )
    authenticator(_request())

    assert model.credentials.authenticator.login_requester.path == "/sessions"
    assert requests_mock.request_history[0].path == "/sessions"


def test_components_outside_the_login_are_still_filled_with_stream_parameters():
    propagated = ManifestComponentTransformer().propagate_types_and_parameters(
        "",
        {
            "type": "DeclarativeStream",
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {"type": "HttpRequester", "url_base": API_URL},
            },
            "$parameters": {"name": "users", "path": "/users"},
        },
        {},
    )

    assert propagated["retriever"]["requester"]["path"] == "/users"


def test_a_second_instance_for_the_same_credentials_is_warned_about_once(caplog):
    factory = ModelToComponentFactory()
    model = _model(
        authenticator=_session_authenticator({"path": "/login/{{ parameters['name'] }}"})
    )

    with caplog.at_level("WARNING", logger="airbyte.model_to_component_factory"):
        for stream_name in ("users", "teams", "projects"):
            factory.create_rate_limited_multiple_token_authenticator(
                _propagated(model, {"name": stream_name}), {"accounts": ACCOUNTS}
            )

    warnings = [r for r in caplog.records if "more than one instance" in r.getMessage()]
    assert len(warnings) == 1
    assert "['111', '222']" in warnings[0].getMessage()


def test_streams_sharing_a_login_are_not_warned_about(caplog):
    factory = ModelToComponentFactory()

    with caplog.at_level("WARNING", logger="airbyte.model_to_component_factory"):
        for stream_name in ("users", "teams"):
            factory.create_rate_limited_multiple_token_authenticator(
                _propagated(_model(), {"name": stream_name}), {"accounts": ACCOUNTS}
            )

    assert not [r for r in caplog.records if "more than one instance" in r.getMessage()]


# --- deferred errors from interpolation -----------------------------------------------------


def test_a_missing_parent_object_in_the_config_is_deferred_to_first_use(requests_mock):
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(items="{{ config['credentials']['accounts'] }}"), {"auth_mode": "tokens"}
    )

    with pytest.raises(AirbyteTracedException, match="missing") as error:
        authenticator(_request())

    assert error.value.failure_type == FailureType.config_error
    assert not requests_mock.called


def test_a_config_selecting_static_tokens_builds_with_a_nested_items_path(requests_mock):
    from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
        ConcurrentDeclarativeSource,
    )

    manifest = _manifest()
    for stream in manifest["streams"]:
        accounts = stream["retriever"]["requester"]["authenticator"]["authenticators"]["accounts"]
        accounts["credentials"]["items"] = "{{ config['credentials']['accounts'] }}"
    config = {"auth_mode": "tokens", "pat": "token_1"}

    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=config, catalog=None, state=None
    )

    assert _authenticators_of(source, config)[0]._tokens == ["token_1"]
    assert not requests_mock.called


def test_a_key_path_missing_from_an_item_is_deferred_to_first_use(requests_mock):
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(key="{{ config['credential']['meta']['id'] }}"), {"accounts": ACCOUNTS}
    )

    with pytest.raises(AirbyteTracedException, match="position 1 has no identifier") as error:
        authenticator(_request())

    assert error.value.failure_type == FailureType.config_error
    assert not requests_mock.called


# --- guards ---------------------------------------------------------------------------------


def test_a_static_tokens_quota_status_failure_is_raised_unchanged(requests_mock):
    requests_mock.get(QUOTA_STATUS_URL, status_code=401, json={"message": "bad token"})
    authenticator = RateLimitedMultipleTokenAuthenticator(
        tokens=["secret-token-1"], quotas=_quotas(), quota_status_url=QUOTA_STATUS_URL
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator(_request())

    assert "secret-token-1" not in (error.value.message or "")
    assert "secret-token-1" not in (error.value.internal_message or "")
    assert not (error.value.message or "").startswith("The credential")


def test_factory_does_not_share_literal_credential_lists_that_differ_only_in_secrets():
    factory = ModelToComponentFactory()

    first = factory.create_rate_limited_multiple_token_authenticator(
        _model(items=[{"id": "111", "secret": "s1"}]), {}
    )
    second = factory.create_rate_limited_multiple_token_authenticator(
        _model(items=[{"id": "111", "secret": "rotated"}]), {}
    )

    assert first is not second


def test_credentials_injecting_into_an_empty_header_are_rejected_on_first_use():
    authenticator = _authenticator(
        [
            RotatingCredential("a", _RefreshingCredential("a", header="")),
            RotatingCredential("b", _RefreshingCredential("b", header="")),
        ]
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator(_request())

    assert "same request header" in error.value.internal_message


# --- round 5: dynamic streams, the split warning, evaluation details --------------------------


def _login_requester_of(authenticator: RateLimitedMultipleTokenAuthenticator):
    credential = authenticator._credentials["111"]
    token_provider = getattr(credential, "token_provider", None) or credential._token_provider
    return token_provider.login_requester


def test_dynamic_streams_share_the_login_of_static_streams_and_fill_none_of_its_fields(caplog):
    import copy

    from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
        ConcurrentDeclarativeSource,
    )

    stream_parameters = {"url": f"{API_URL}/other", "request_parameters": {"per_page": "100"}}
    manifest = copy.deepcopy(_manifest())
    for stream in manifest["streams"]:
        stream["$parameters"].update(stream_parameters)
    template = copy.deepcopy(manifest["streams"][0])
    template["name"] = "template"
    template["$parameters"]["name"] = "template"
    manifest["dynamic_streams"] = [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": template,
            "components_resolver": {
                "type": "ConfigComponentsResolver",
                "stream_config": {"type": "StreamConfig", "configs_pointer": ["projects"]},
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{ components_values['name'] }}",
                    }
                ],
            },
        }
    ]
    config = {
        "auth_mode": "accounts",
        "accounts": ACCOUNTS,
        "projects": [{"name": "alpha"}, {"name": "beta"}],
    }

    with caplog.at_level("WARNING", logger="airbyte.model_to_component_factory"):
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )
        authenticators = _authenticators_of(source, config)

    assert len(authenticators) == 4
    assert len({id(authenticator) for authenticator in authenticators}) == 1
    login_requester = _login_requester_of(authenticators[0])
    assert login_requester.get_request_params() == {}
    assert not [r for r in caplog.records if "more than one instance" in r.getMessage()]


def test_a_split_through_a_parent_stream_is_warned_about(caplog):
    import copy

    from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
        ConcurrentDeclarativeSource,
    )

    manifest = copy.deepcopy(_manifest())
    users, teams = manifest["streams"]
    login = teams["retriever"]["requester"]["authenticator"]["authenticators"]["accounts"]
    # Keyed per stream, so the parent built by the substream factory gets its own instance.
    login["credentials"]["authenticator"]["login_requester"]["path"] = (
        "/login/{{ config['credential']['id'] }}/{{ parameters['name'] }}"
    )
    parent = copy.deepcopy(teams)
    parent["name"] = "users"
    parent["$parameters"]["name"] = "users"
    teams["retriever"]["partition_router"] = {
        "type": "SubstreamPartitionRouter",
        "parent_stream_configs": [
            {
                "type": "ParentStreamConfig",
                "stream": parent,
                "parent_key": "id",
                "partition_field": "user_id",
            }
        ],
    }
    manifest["streams"] = [teams]
    manifest["check"]["stream_names"] = ["teams"]
    config = {"auth_mode": "accounts", "accounts": ACCOUNTS}

    with caplog.at_level("WARNING", logger="airbyte.model_to_component_factory"):
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )
        source.streams(config)

    warnings = [r for r in caplog.records if "more than one instance" in r.getMessage()]
    assert len(warnings) == 1


def test_a_split_by_the_quota_status_request_is_warned_about(caplog):
    factory = ModelToComponentFactory()

    with caplog.at_level("WARNING", logger="airbyte.model_to_component_factory"):
        first, second = (
            factory.create_rate_limited_multiple_token_authenticator(
                _propagated(_model(), {"name": name, "request_headers": {"X-Stream": name}}),
                {"accounts": ACCOUNTS},
            )
            for name in ("users", "teams")
        )

    assert first is not second
    warnings = [r for r in caplog.records if "more than one instance" in r.getMessage()]
    assert len(warnings) == 1
    assert "quota-status request" in warnings[0].getMessage()


def test_an_unused_credentials_branch_is_not_warned_about(caplog):
    from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
        ConcurrentDeclarativeSource,
    )

    manifest = _manifest()
    for stream in manifest["streams"]:
        accounts = stream["retriever"]["requester"]["authenticator"]["authenticators"]["accounts"]
        accounts["credentials"]["authenticator"]["login_requester"]["request_headers"] = {
            "X-Stream": "{{ parameters['name'] }}"
        }
    config = {"auth_mode": "tokens", "pat": "token_1"}

    with caplog.at_level("WARNING", logger="airbyte.model_to_component_factory"):
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )
        source.streams(config)

    assert not [r for r in caplog.records if "more than one instance" in r.getMessage()]


def test_the_same_credentials_under_different_quota_pools_are_not_warned_about(caplog):
    factory = ModelToComponentFactory()
    search_quotas = [
        {"type": "TokenQuota", "name": "search", "remaining_path": ["s"], "reset_path": ["t"]}
    ]

    with caplog.at_level("WARNING", logger="airbyte.model_to_component_factory"):
        rest = factory.create_rate_limited_multiple_token_authenticator(
            _model(), {"accounts": ACCOUNTS}
        )
        search = factory.create_rate_limited_multiple_token_authenticator(
            _model(quotas=search_quotas), {"accounts": ACCOUNTS}
        )

    assert rest is not search
    assert not [r for r in caplog.records if "more than one instance" in r.getMessage()]


@pytest.mark.parametrize(
    "model_updates, user_message, expression",
    [
        pytest.param(
            {"items": "{{ config['acounts'][0] }}"},
            "Authentication credentials are missing from the configuration.",
            "`items` \"{{ config['acounts'][0] }}\" could not be evaluated",
            id="items",
        ),
        pytest.param(
            {"key": "{{ config['credential']['meta']['id'] }}"},
            "The authentication credentials are invalid. "
            "The credential at position 1 has no identifier.",
            "`key` \"{{ config['credential']['meta']['id'] }}\" could not be evaluated for the "
            "credential at position 1",
            id="key",
        ),
    ],
)
def test_an_expression_that_cannot_be_evaluated_is_named_in_the_internal_message(
    requests_mock, model_updates, user_message, expression
):
    authenticator = ModelToComponentFactory().create_rate_limited_multiple_token_authenticator(
        _model(**model_updates), {"accounts": ACCOUNTS}
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator(_request())

    assert error.value.message == user_message
    assert expression in error.value.internal_message
    assert "UndefinedError" not in error.value.message
    assert not requests_mock.called
