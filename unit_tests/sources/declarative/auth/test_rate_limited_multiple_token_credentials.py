#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
"""`RateLimitedMultipleTokenAuthenticator` with `credentials`: rotation across credentials that
log in and refresh their own tokens, with quota state kept per credential key."""

import json
import time
from typing import Any, List, Mapping, Optional

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


def test_credentials_injecting_into_different_headers_are_rejected():
    with pytest.raises(ValueError, match="same request header"):
        _authenticator(
            [
                RotatingCredential("a", _RefreshingCredential("a")),
                RotatingCredential("b", _RefreshingCredential("b", header="X-Api-Key")),
            ]
        )


# --- factory ------------------------------------------------------------------------------


def _model(
    items: Any = "{{ config['accounts'] }}",
    key: Optional[str] = "{{ config['credential']['id'] }}",
    tokens: Any = None,
) -> RateLimitedMultipleTokenAuthenticatorModel:
    credentials: Mapping[str, Any] = {
        "type": "CredentialSet",
        "items": items,
        "authenticator": {
            "type": "SessionTokenAuthenticator",
            "login_requester": {
                "type": "HttpRequester",
                "url_base": API_URL,
                "path": "/login/{{ config['credential']['id'] }}",
                "http_method": "POST",
                "request_body_json": {"secret": "{{ config['credential']['secret'] }}"},
            },
            "session_token_path": ["token"],
            "expiration_duration": "PT55M",
            "request_authentication": {"type": "Bearer"},
        },
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
    return RateLimitedMultipleTokenAuthenticatorModel.parse_obj(definition)


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


def test_factory_shares_one_instance_across_streams_whose_parameters_differ():
    factory = ModelToComponentFactory()
    transformer = ManifestComponentTransformer()

    def _propagated(stream_name):
        return RateLimitedMultipleTokenAuthenticatorModel.parse_obj(
            transformer.propagate_types_and_parameters(
                "authenticator",
                _model().dict(exclude_none=True, by_alias=True),
                {"name": stream_name},
            )
        )

    first_model, second_model = _propagated("stream_a"), _propagated("stream_b")
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

    (cache_key,) = factory._rate_limited_authenticators
    assert sorted(json.loads(cache_key)) == [
        "auth_method",
        "budget_min_reserve",
        "budget_reserve_fraction",
        "header",
        "max_wait_time",
        "quota_status_headers",
        "quota_status_http_method",
        "quota_status_unavailable_status_codes",
        "quota_status_url",
        "quotas",
        "tokens",
    ]


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
            "$parameters": {"name": name},
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
