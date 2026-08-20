#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

"""Tests for `ConcurrentDeclarativeSource._config_overridden_for_check` and its guards.

The behaviour under test lives in `ConcurrentDeclarativeSource`, not in the check components - the
check factories deliberately ignore `model.config_overrides`. Kept beside
`test_concurrent_declarative_source.py` rather than inside it because that module is already 6k lines.
"""

import json
import logging
import pkgutil
from copy import deepcopy

import pytest
import yaml

from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.models import ConnectorSpecification, FailureType, Status, Type
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    CheckDynamicStream as CheckDynamicStreamModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    CheckStream as CheckStreamModel,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

logger = logging.getLogger("test")


_CONFIG_DRIVEN_PATH_CONFIG = {"resource": "sync"}

_MANIFEST_WITH_CONFIG_DRIVEN_PATH = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["items"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "items",
            "primary_key": "id",
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url": "https://api.test.com/{{ config['resource'] }}",
                    "http_method": "GET",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
                "paginator": {"type": "NoPagination"},
            },
        }
    ],
}


def _source_with_check_component(check_component):
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = check_component
    return ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )


def test_given_no_config_overrides_when_check_then_components_use_the_user_config():
    source = _source_with_check_component({"type": "CheckStream", "stream_names": ["items"]})

    with HttpMocker() as http_mocker:
        # Only the user-configured path is mocked, so a request to any other path fails the test.
        http_mocker.get(
            HttpRequest(url="https://api.test.com/sync"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED


def test_given_config_overrides_when_check_then_components_built_during_check_see_them():
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )

    with HttpMocker() as http_mocker:
        # Only the overridden path is mocked. Reaching this endpoint proves the overlay was applied to
        # the stream the checker built, and not merely stored on the source.
        overridden_request = HttpRequest(url="https://api.test.com/check-only")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED
        http_mocker.assert_number_of_calls(overridden_request, 1)


def test_given_config_overrides_when_check_then_the_config_is_restored_afterwards():
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/check-only"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED

    assert source._config == _CONFIG_DRIVEN_PATH_CONFIG

    # A sync that follows a check in the same process must go back to the user's value.
    with HttpMocker() as http_mocker:
        sync_request = HttpRequest(url="https://api.test.com/sync")
        http_mocker.get(sync_request, HttpResponse(body=json.dumps([{"id": 1}])))

        stream = source.streams(_CONFIG_DRIVEN_PATH_CONFIG)[0]
        assert stream.check_availability().is_available

        http_mocker.assert_number_of_calls(sync_request, 1)


def test_given_config_overrides_when_check_raises_then_the_config_is_restored():
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["not_in_the_catalog"],
            "config_overrides": {"resource": "check-only"},
        }
    )

    with pytest.raises(ValueError):
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)

    assert source._config == _CONFIG_DRIVEN_PATH_CONFIG


def test_given_config_overrides_when_check_then_config_validations_run_against_the_user_config():
    """An override is authored in the manifest, so it must not be held to validations written for the
    user's own input - the user has no way to satisfy them."""
    config = {"resource": "sync", "settings": {"mode": "sync"}}
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only", "settings": {"mode": "check-only"}},
    }
    manifest["spec"] = {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "resource": {"type": "string"},
                "settings": {"type": "object"},
            },
        },
        "config_normalization_rules": {
            "type": "ConfigNormalizationRules",
            "validations": [
                {
                    "type": "DpathValidator",
                    "field_path": ["settings"],
                    "validation_strategy": {
                        "type": "ValidateAdheresToSchema",
                        "base_schema": {
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "type": "object",
                            "properties": {"mode": {"type": "string", "enum": ["sync"]}},
                            "required": ["mode"],
                            "additionalProperties": False,
                        },
                    },
                }
            ],
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=config,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/check-only"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        # `settings.mode` is overridden to a value outside the validator's enum, so this would fail were
        # the overlay validated instead of the config the user supplied.
        assert source.check(logger, config).status == Status.SUCCEEDED


def test_given_config_overrides_when_check_then_values_are_not_interpolated():
    """Pins the verbatim contract. A value containing `{{ }}` reaches components as that literal string,
    so turning interpolation on later is a deliberate, test-breaking decision rather than a silent
    reinterpretation of overrides already written."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "{{config['resource']}}"},
        }
    )

    with HttpMocker() as http_mocker:
        literal_request = HttpRequest(url="https://api.test.com/%7B%7Bconfig%5B'resource'%5D%7D%7D")
        http_mocker.get(literal_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED
        http_mocker.assert_number_of_calls(literal_request, 1)


_MANIFEST_WITH_CONFIG_DRIVEN_DYNAMIC_STREAM = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckDynamicStream", "stream_count": 1},
    "streams": [],
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "name": "dynamic_items",
            "stream_template": {
                "type": "DeclarativeStream",
                "name": "",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "type": "object",
                        "properties": {"id": {"type": "integer"}},
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url": "https://api.test.com/{{ config['resource'] }}",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "paginator": {"type": "NoPagination"},
                },
            },
            "components_resolver": {
                "type": "ConfigComponentsResolver",
                "stream_config": {
                    "type": "StreamConfig",
                    "configs_pointer": ["custom_streams"],
                },
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{components_values['name']}}",
                    }
                ],
            },
        }
    ],
}


def test_given_config_overrides_on_check_dynamic_stream_then_components_see_them():
    """The overlay is read from the raw check definition, so it is checker-agnostic. Without this test a
    refactor moving the read into `create_check_stream` would silently drop `CheckDynamicStream`."""
    config = {"resource": "sync", "custom_streams": [{"name": "items"}]}
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_DYNAMIC_STREAM)
    manifest["check"] = {
        "type": "CheckDynamicStream",
        "stream_count": 1,
        "config_overrides": {"resource": "check-only"},
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=config,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        overridden_request = HttpRequest(url="https://api.test.com/check-only")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, config).status == Status.SUCCEEDED
        http_mocker.assert_number_of_calls(overridden_request, 1)


_OAUTH_WITH_REFRESH_TOKEN_UPDATER = {
    "type": "OAuthAuthenticator",
    "token_refresh_endpoint": "https://api.test.com/oauth/token",
    "client_id": "{{ config['credentials']['client_id'] }}",
    "client_secret": "{{ config['credentials']['client_secret'] }}",
    "refresh_token": "{{ config['credentials']['refresh_token'] }}",
    "refresh_token_updater": {"type": "RefreshTokenUpdater", "refresh_token_name": "refresh_token"},
}


def _manifest_with_refresh_token_updater(check_component, refresh_token_updater=None):
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = check_component
    authenticator = deepcopy(_OAUTH_WITH_REFRESH_TOKEN_UPDATER)
    if refresh_token_updater is not None:
        authenticator["refresh_token_updater"] = deepcopy(refresh_token_updater)
    manifest["streams"][0]["retriever"]["requester"]["authenticator"] = authenticator
    return manifest


@pytest.mark.parametrize(
    "refresh_token_updater",
    [
        pytest.param(
            {"type": "RefreshTokenUpdater", "refresh_token_name": "refresh_token"}, id="populated"
        ),
        # Every field of `RefreshTokenUpdater` has a default, so an empty mapping is a valid way to take
        # all of them. It builds the same single-use authenticator a populated one does, and the
        # transformer injects no `type` into it, so it stays falsy - a truthiness test would miss it.
        pytest.param({}, id="empty-taking-all-defaults"),
    ],
)
def test_given_refresh_token_updater_when_config_overrides_then_manifest_is_rejected(
    refresh_token_updater,
):
    """A `refresh_token_updater` emits the whole config it was handed as a CONNECTOR_CONFIG control
    message, which the platform persists - so a check-only override would become the connection's saved
    config. The restore cannot recall a message already on stdout, so the combination is refused."""
    source = ConcurrentDeclarativeSource(
        source_config=_manifest_with_refresh_token_updater(
            {
                "type": "CheckStream",
                "stream_names": ["items"],
                "config_overrides": {"resource": "check-only"},
            },
            refresh_token_updater=refresh_token_updater,
        ),
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    with pytest.raises(AirbyteTracedException, match="refresh_token_updater") as raised:
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)
    assert raised.value.failure_type == FailureType.system_error


def test_given_no_refresh_token_updater_when_config_overrides_then_manifest_is_accepted():
    """The scan must not reject every OAuth manifest. The same authenticator without the updater writes
    nothing back, so the overlay is allowed."""
    manifest = _manifest_with_refresh_token_updater(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )
    del manifest["streams"][0]["retriever"]["requester"]["authenticator"]["refresh_token_updater"]

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source._source_config) is False

    # Asserted through the overlay rather than a full check: this manifest still authenticates with
    # OAuth, and mocking a token exchange would test the handshake rather than the guard.
    with source._config_overridden_for_check({"resource": "check-only"}):
        assert source._config["resource"] == "check-only"


def test_given_spec_property_named_refresh_token_updater_then_overrides_are_allowed():
    """The scan walks the raw manifest looking for the key anywhere, because an authenticator reached
    through a `$ref` is only found under `definitions`. A connector whose spec happens to declare a
    config field by that name must not be caught by that net - nothing in `spec` is a component."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only"},
    }
    manifest["spec"] = {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "resource": {"type": "string"},
                "refresh_token_updater": {"type": "string"},
            },
        },
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source._source_config) is False

    with HttpMocker() as http_mocker:
        overridden_request = HttpRequest(url="https://api.test.com/check-only")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED


def test_given_override_of_a_field_named_refresh_token_updater_then_it_is_allowed():
    """`config_overrides` holds config values, not components, so a key that collides with the
    authenticator field name is just a config field and must not trip the guard."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only", "refresh_token_updater": "check-only"},
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source._source_config) is False

    with HttpMocker() as http_mocker:
        overridden_request = HttpRequest(url="https://api.test.com/check-only")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED


def test_given_refresh_token_updater_behind_a_ref_then_manifest_is_rejected():
    """The coarse walk is what makes a `$ref`-ed authenticator detectable at all: the raw manifest holds
    only the reference, and the authenticator itself sits under `definitions`. Narrowing the walk must
    not lose that."""
    manifest = _manifest_with_refresh_token_updater(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )
    manifest["definitions"] = {
        "authenticator": manifest["streams"][0]["retriever"]["requester"]["authenticator"]
    }
    manifest["streams"][0]["retriever"]["requester"]["authenticator"] = {
        "$ref": "#/definitions/authenticator"
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    with pytest.raises(AirbyteTracedException, match="refresh_token_updater") as raised:
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)
    assert raised.value.failure_type == FailureType.system_error


def test_given_refresh_token_updater_without_config_overrides_then_nothing_is_rejected():
    """The rejection is scoped to the feature. A manifest that does not use `config_overrides` keeps
    working with a `refresh_token_updater` exactly as before, even though the manifest scan detects it."""
    source = ConcurrentDeclarativeSource(
        source_config=_manifest_with_refresh_token_updater(
            {"type": "CheckStream", "stream_names": ["items"]}
        ),
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source.resolved_manifest) is True

    # No overrides means no overlay, so the guard must not fire and the config must not be copied.
    with source._config_overridden_for_check(None):
        assert source._config is _CONFIG_DRIVEN_PATH_CONFIG


def test_given_config_overrides_when_check_then_overridden_keys_are_logged_without_values(caplog):
    """An override may name a secret field, so the log records which keys were overridden but never what
    they were set to."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "s3cr3t-value"},
        }
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/s3cr3t-value"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        with caplog.at_level(logging.INFO):
            assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED

    assert "Overriding config keys for the check operation: resource" in caplog.text
    assert "s3cr3t-value" not in caplog.text


def test_given_override_key_absent_from_the_spec_then_a_warning_is_logged(caplog):
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only", "typoed_key": 1},
    }
    manifest["spec"] = {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"resource": {"type": "string"}},
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/check-only"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        with caplog.at_level(logging.WARNING):
            assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED

    assert "typoed_key" in caplog.text
    assert "not declared in the connector spec" in caplog.text
    assert "'resource'" not in caplog.text


def test_given_an_airbyte_reserved_override_key_then_the_manifest_is_rejected():
    """`__airbyte`-prefixed keys are the platform's channel into the config - `CheckStream` reads
    `__airbyte_check_stream_names` from the very config the overlay writes to. The feature refuses to
    touch that namespace rather than leaving it available."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"__airbyte_check_stream_names": ["something_else"]},
        }
    )

    with pytest.raises(AirbyteTracedException, match="__airbyte_check_stream_names") as raised:
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)
    assert raised.value.failure_type == FailureType.system_error


def _spec_with(properties):
    return {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": properties,
        },
    }


def test_given_a_reserved_key_when_check_through_the_entrypoint_then_a_trace_is_emitted_and_it_raises():
    """The guards fire on a manifest authoring mistake, so they raise `system_error`, and
    `AirbyteEntrypoint.check` treats anything other than `config_error` as exceptional: it emits the
    TRACE and then re-raises rather than reporting a FAILED connection status. That is deliberate -- a
    broken manifest is a connector bug and should exit non-zero, not be reported to the user as a bad
    connection. What must not regress is the TRACE: a bare `ValueError` would escape `run()` entirely
    and throw away the message the guard was written to deliver."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"__airbyte_check_stream_names": ["something_else"]},
        }
    )
    entrypoint = AirbyteEntrypoint(source)

    messages = []
    with pytest.raises(AirbyteTracedException) as raised:
        for message in entrypoint.check(
            ConnectorSpecification(connectionSpecification={}), _CONFIG_DRIVEN_PATH_CONFIG
        ):
            messages.append(message)

    assert raised.value.failure_type == FailureType.system_error
    traces = [message.trace for message in messages if message.type == Type.TRACE]
    assert len(traces) == 1
    assert traces[0].error.failure_type == FailureType.system_error
    assert "__airbyte_check_stream_names" in traces[0].error.message
    assert not [message for message in messages if message.type == Type.CONNECTION_STATUS]


def test_given_a_config_override_shaped_like_a_reference_then_it_stays_a_literal():
    """`ManifestReferenceResolver` treats any string starting with `#/` as a reference, wherever it
    appears. Override values are connector config, so the resolver has to leave them alone - otherwise a
    config value that happens to look like a pointer is silently replaced by whatever it resolves to."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["definitions"] = {"somewhere": {"a": 1}}
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "#/definitions/somewhere"},
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source.resolved_manifest["check"]["config_overrides"] == {
        "resource": "#/definitions/somewhere"
    }

    with HttpMocker() as http_mocker:
        overridden_request = HttpRequest(url="https://api.test.com/#/definitions/somewhere")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED


def test_given_an_unresolvable_reference_shaped_override_then_the_source_still_constructs():
    """The worst version of the same bug: an unresolvable pointer raises inside `_pre_process_manifest`,
    which runs in `__init__` - so `spec`, `discover` and `read` would all die over a field only `check`
    ever reads."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "#/nothing/here"},
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert [stream.name for stream in source.streams(_CONFIG_DRIVEN_PATH_CONFIG)] == ["items"]


@pytest.mark.parametrize(
    "mutate_manifest, description",
    [
        pytest.param(
            lambda manifest: manifest["streams"][0]["schema_loader"]["schema"]["properties"].update(
                {"refresh_token_updater": {"type": "string"}}
            ),
            "record schema property",
            id="inline-schema-property",
        ),
        pytest.param(
            lambda manifest: manifest["streams"][0]["retriever"]["requester"].update(
                {"request_parameters": {"refresh_token_updater": "x"}}
            ),
            "request parameter name",
            id="request-parameter-name",
        ),
        pytest.param(
            lambda manifest: manifest.update(
                {
                    "schemas": {
                        "items": {"properties": {"refresh_token_updater": {"type": "string"}}}
                    }
                }
            ),
            "top-level schemas block",
            id="top-level-schemas",
        ),
        pytest.param(
            lambda manifest: manifest["check"]["config_overrides"].update(
                {"credentials": {"type": "settings", "refresh_token_updater": "a-value"}}
            ),
            "object-valued override carrying both keys",
            id="override-value-with-type-and-key",
        ),
    ],
)
def test_given_the_name_appears_in_a_data_blob_then_overrides_are_still_allowed(
    mutate_manifest, description
):
    """The scan looks for `refresh_token_updater` anywhere, because an authenticator can sit under any
    requester. What keeps that from catching data is the `type` on the mapping holding the key: a
    component always has one, a record schema property or a request body entry does not. Without that
    condition these manifests - which declare no authenticator at all - are refused with an error
    telling the author to remove something that does not exist."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only"},
    }
    mutate_manifest(manifest)

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source._source_config) is False

    with HttpMocker() as http_mocker:
        overridden_request = HttpRequest(
            url="https://api.test.com/check-only",
            query_params=manifest["streams"][0]["retriever"]["requester"].get("request_parameters"),
        )
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED


def test_given_a_nested_override_then_the_object_is_replaced_rather_than_merged():
    """The one-level merge is a documented semantic, not an accident: replacing the object wholesale is
    what keeps "remove this nested key during check" expressible. A recursive merge would leave every
    sibling key in place, so this asserts a sibling is gone."""
    config = {"resource": "sync", "settings": {"mode": "sync", "sibling": "present"}}
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"settings": {"mode": "check-only"}},
    }
    manifest["streams"][0]["retriever"]["requester"]["url"] = (
        "https://api.test.com/{{ config['settings'].get('sibling', 'dropped') }}"
    )

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=config,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        replaced_request = HttpRequest(url="https://api.test.com/dropped")
        http_mocker.get(replaced_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, config).status == Status.SUCCEEDED
        http_mocker.assert_number_of_calls(replaced_request, 1)


def test_given_non_string_override_keys_then_the_manifest_is_rejected_cleanly():
    """`type: object` in the schema does not constrain key types, and YAML will happily produce an
    integer key. Every consumer downstream treats a key as a string, so this is refused with an
    author-facing message rather than a `TypeError` from a join."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {0: "check-only"},
        }
    )

    with pytest.raises(AirbyteTracedException, match="are not strings") as raised:
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)
    assert raised.value.failure_type == FailureType.system_error


def test_given_a_spec_composed_with_all_of_then_no_spurious_warning_is_logged(caplog):
    """A spec does not have to enumerate its fields at the top level. Reading only `properties` reports
    an `allOf`-composed field as undeclared, which sends an author chasing a warning about a key that is
    perfectly valid."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only"},
    }
    manifest["spec"] = {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {},
            "allOf": [{"properties": {"resource": {"type": "string"}}}],
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/check-only"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        with caplog.at_level(logging.WARNING):
            assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED

    assert "not declared in the connector spec" not in caplog.text


def test_given_an_override_on_a_secret_field_then_the_value_is_registered_for_redaction():
    """The entrypoint builds the secret list from the config the user supplied, so a value substituted
    here is unknown to `filter_secrets` - and would print in the clear at a path where the user's own
    value prints as `****`."""
    from airbyte_cdk.utils.airbyte_secrets_utils import filter_secrets, update_secrets

    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only-secret"},
    }
    manifest["spec"] = _spec_with({"resource": {"type": "string", "airbyte_secret": True}})
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    update_secrets([])
    try:
        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url="https://api.test.com/check-only-secret"),
                HttpResponse(body=json.dumps([{"id": 1}])),
            )

            assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED

        assert filter_secrets("saw check-only-secret") == "saw ****"
    finally:
        update_secrets([])


def test_config_overrides_is_published_on_both_check_components():
    """The schema is the contract the Connector Builder and the manifest server read, and the factory
    deliberately ignores the field - so nothing at runtime notices if it disappears from the published
    surface."""
    schema = yaml.safe_load(
        pkgutil.get_data(
            "airbyte_cdk.sources.declarative", "declarative_component_schema.yaml"
        ).decode()
    )

    for component in ("CheckStream", "CheckDynamicStream"):
        assert "config_overrides" in schema["definitions"][component]["properties"]

    assert CheckStreamModel(
        type="CheckStream", stream_names=["items"], config_overrides={"a": 1}
    ).config_overrides == {"a": 1}
    assert CheckDynamicStreamModel(
        type="CheckDynamicStream", stream_count=1, config_overrides={"a": 1}
    ).config_overrides == {"a": 1}


def test_given_a_custom_authenticator_declaring_a_refresh_token_updater_then_it_is_rejected():
    """`refresh_token_updater` is declared on `OAuthAuthenticator` alone, so matching that type would be
    enough for the schema as written. `CustomAuthenticator` is matched too because the transformer
    injects that type for a `class_name` component, and custom code that declares the field is the shape
    most likely to write the config back - the one case of custom code the manifest does name."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only"},
    }
    manifest["streams"][0]["retriever"]["requester"]["authenticator"] = {
        "type": "CustomAuthenticator",
        "class_name": "unit_tests.sources.declarative.test_concurrent_declarative_source_config_overrides.NotBuilt",
        "refresh_token_updater": {},
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    # Matched on the guard's own sentence: a failure to import the custom class also mentions
    # `refresh_token_updater`, because the message echoes the component definition.
    with pytest.raises(
        AirbyteTracedException, match="cannot be used by a manifest that declares"
    ) as raised:
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)
    assert raised.value.failure_type == FailureType.system_error
