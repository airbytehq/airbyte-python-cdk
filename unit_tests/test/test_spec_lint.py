# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the credential-free spec lint in `airbyte_cdk.test.standard_tests._spec_lint`."""

import base64
import json
import logging
import subprocess
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import pytest
from pydantic import BaseModel, Field

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConnectorSpecification,
    Status,
)
from airbyte_cdk.sources.source import Source
from airbyte_cdk.test.entrypoint_wrapper import AirbyteEntrypointException, EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests import connector_base, docker_base, source_base
from airbyte_cdk.test.standard_tests._spec_lint import (
    assert_no_secrets_in_output,
    assert_spec_is_valid,
    find_config_secrets,
    find_leaked_secrets,
    find_secret_marking_errors,
    get_single_spec,
    is_secret_property_name,
)
from airbyte_cdk.test.standard_tests.connector_base import ConnectorTestSuiteBase
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.test.standard_tests.source_base import SourceTestSuiteBase
from airbyte_cdk.utils import docker as docker_utils
from airbyte_cdk.utils.airbyte_secrets_utils import get_secrets

SECRET = "sk_live_51UWRAsFuIbeygfIY3"
POKEAPI_CONNECTOR_ROOT = (
    Path(__file__).parent.parent / "resources" / "source_pokeapi_w_components_py"
)


def _spec(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": properties}


def _output(*messages: dict[str, Any]) -> EntrypointOutput:
    return EntrypointOutput(messages=[json.dumps(message) for message in messages])


def _log(message: str) -> dict[str, Any]:
    return {"type": "LOG", "log": {"level": "INFO", "message": message}}


def _spec_message(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "SPEC", "spec": {"connectionSpecification": _spec(properties)}}


@pytest.mark.parametrize(
    "name, expected",
    [
        pytest.param("password", True, id="exact_name"),
        pytest.param("Client_Secret", True, id="case_insensitive"),
        pytest.param("api_key", True, id="api_key_suffix"),
        pytest.param("x-api-key", True, id="dashes_normalized"),
        pytest.param("personal_access_token", True, id="token_suffix"),
        pytest.param("aws_secret_access_key", True, id="access_key_suffix"),
        pytest.param("tunnel_user_password", True, id="password_suffix"),
        pytest.param("tenant_id", False, id="tenant_id_is_an_identifier"),
        pytest.param("app_id", False, id="app_id_is_an_identifier"),
        pytest.param("token_expiry_date", False, id="token_prefix_only"),
        pytest.param("primary_key", False, id="primary_key"),
        pytest.param("access_key_id", False, id="access_key_id"),
        pytest.param("start_date", False, id="unrelated"),
        pytest.param("clientSecret", True, id="camel_case_client_secret"),
        pytest.param("accessToken", True, id="camel_case_access_token"),
        pytest.param("privateKey", True, id="camel_case_private_key"),
        pytest.param("apiKey", True, id="camel_case_api_key"),
        pytest.param("APIKey", True, id="upper_case_acronym"),
        pytest.param("OAuthRefreshToken", True, id="pascal_case"),
        pytest.param("tokenExpiryDate", False, id="camel_case_token_prefix_only"),
        pytest.param("accessKeyId", False, id="camel_case_access_key_id"),
        pytest.param("access_key", True, id="lone_access_key"),
    ],
)
def test_is_secret_property_name(name: str, expected: bool) -> None:
    assert is_secret_property_name(name) == expected


@pytest.mark.parametrize(
    "name, sibling_names, expected",
    [
        pytest.param("app_access_key", ["app_secret"], False, id="100ms_key_pair"),
        pytest.param("access_key", ["secret_key"], False, id="access_key_with_secret_key"),
        pytest.param("accessKey", ["secretKey"], False, id="camel_case_key_pair"),
        pytest.param("access_key", ["secret"], False, id="access_key_with_secret"),
        pytest.param("access_key", ["bucket", "region"], True, id="no_secret_sibling"),
        pytest.param("access_key", ["access_key"], True, id="itself_is_not_a_sibling"),
        pytest.param(
            "aws_secret_access_key", ["aws_access_key_id", "client_secret"], True, id="secret_half"
        ),
        pytest.param("api_key", ["api_secret"], True, id="only_access_keys_are_exempt"),
    ],
)
def test_is_secret_property_name_exempts_the_public_half_of_a_key_pair(
    name: str, sibling_names: list[str], expected: bool
) -> None:
    assert is_secret_property_name(name, sibling_names) == expected


@pytest.mark.parametrize(
    "properties, expected_errors",
    [
        pytest.param(
            {"api_key": {"type": "string", "airbyte_secret": True}},
            [],
            id="marked_secret_passes",
        ),
        pytest.param(
            {"start_date": {"type": "string"}, "tenant_id": {"type": "string"}},
            [],
            id="non_secret_names_pass",
        ),
        pytest.param(
            {"api_key": {"type": "string"}},
            ["`/properties/api_key` looks like a secret"],
            id="unmarked_api_key_fails",
        ),
        pytest.param(
            {"password": {"type": ["null", "integer"]}},
            ["`/properties/password` looks like a secret"],
            id="unmarked_nullable_type_list_fails",
        ),
        pytest.param(
            {
                "credentials": {
                    "type": "object",
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "auth_type": {"type": "string", "const": "oauth2.0"},
                                "client_secret": {"type": "string", "airbyte_secret": True},
                            },
                        },
                        {
                            "type": "object",
                            "properties": {
                                "auth_type": {"type": "string", "const": "api_key"},
                                "access_token": {"type": "string"},
                            },
                        },
                    ],
                }
            },
            ["`/properties/credentials/oneOf/1/properties/access_token` looks like a secret"],
            id="unmarked_secret_inside_one_of_fails",
        ),
        pytest.param(
            {"tokens": {"type": "array", "items": {"type": "string"}}},
            [],
            id="array_of_non_secret_name_passes",
        ),
        pytest.param(
            {"refresh_token": {"type": "array", "items": {"type": "string"}}},
            ["`/properties/refresh_token/items` looks like a secret"],
            id="unmarked_items_of_secret_array_fail",
        ),
        pytest.param(
            {"token": {"type": "string", "const": "fixed"}},
            [],
            id="const_secret_name_passes",
        ),
        pytest.param(
            {"password": {"type": "boolean", "airbyte_secret": True}},
            ["`/properties/password` is marked `airbyte_secret: true` but its type `boolean`"],
            id="marked_boolean_fails",
        ),
        pytest.param(
            {"credentials": {"type": "object", "airbyte_secret": True, "properties": {}}},
            ["`/properties/credentials` is marked `airbyte_secret: true` but its type `object`"],
            id="marked_object_fails",
        ),
        pytest.param(
            {"credentials": {"type": "object", "properties": {"type": {"type": "string"}}}},
            [],
            id="property_named_type_inside_secret_object_passes",
        ),
        pytest.param(
            {"access_key": {"type": "string", "airbyte_secret": "true"}},
            ['`/properties/access_key` sets `airbyte_secret` to `"true"`, which is not a boolean'],
            id="string_marking_on_secret_name_fails_once",
        ),
        pytest.param(
            {"deployment_url": {"type": "string", "airbyte_secret": "yes"}},
            [
                '`/properties/deployment_url` sets `airbyte_secret` to `"yes"`, which is not a boolean'
            ],
            id="string_marking_on_any_name_fails",
        ),
        pytest.param(
            {"start_date": {"type": "string", "airbyte_secret": False}},
            [],
            id="false_marking_on_non_secret_name_passes",
        ),
        pytest.param(
            {
                "app_access_key": {"type": "string"},
                "app_secret": {"type": "string", "airbyte_secret": True},
            },
            [],
            id="unmarked_public_half_of_a_key_pair_passes",
        ),
        pytest.param(
            {"access_key": {"type": "string"}},
            ["`/properties/access_key` looks like a secret"],
            id="unmarked_lone_access_key_fails",
        ),
        pytest.param(
            {
                "keys": {
                    "type": "object",
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "access_key": {"type": "string"},
                                "secret_key": {"type": "string", "airbyte_secret": True},
                            },
                        }
                    ],
                }
            },
            [],
            id="key_pair_inside_one_of_passes",
        ),
        pytest.param(
            {"clientSecret": {"type": "string"}},
            ["`/properties/clientSecret` looks like a secret"],
            id="unmarked_camel_case_secret_fails",
        ),
    ],
)
def test_find_secret_marking_errors(
    properties: dict[str, Any],
    expected_errors: list[str],
) -> None:
    errors = find_secret_marking_errors(_spec(properties))
    assert len(errors) == len(expected_errors), errors
    for error, expected_error in zip(errors, expected_errors):
        assert error.startswith(expected_error)


def test_assert_spec_is_valid_lists_every_unmarked_secret() -> None:
    spec = ConnectorSpecification(
        connectionSpecification=_spec(
            {"api_key": {"type": "string"}, "client_secret": {"type": "string"}}
        )
    )
    with pytest.raises(AssertionError) as error:
        assert_spec_is_valid(spec, connector_name="source-test")
    assert "/properties/api_key" in str(error.value)
    assert "/properties/client_secret" in str(error.value)


@pytest.mark.parametrize("spec_count", [0, 2])
def test_get_single_spec_requires_exactly_one_spec_message(spec_count: int) -> None:
    output = _output(*[_spec_message({}) for _ in range(spec_count)], _log("done"))
    with pytest.raises(AssertionError, match=f"emitted {spec_count} SPEC messages"):
        get_single_spec(output, connector_name="source-test")


def test_get_single_spec_returns_the_spec() -> None:
    spec = get_single_spec(_output(_spec_message({"a": {"type": "string"}})), connector_name="x")
    assert spec.connectionSpecification["properties"] == {"a": {"type": "string"}}


@pytest.mark.parametrize(
    "message, location",
    [
        pytest.param(
            _log(f"Calling https://api.example.com/?key={SECRET}"),
            "message #1 (LOG)",
            id="log",
        ),
        pytest.param(
            {
                "type": "TRACE",
                "trace": {
                    "type": "ERROR",
                    "emitted_at": 0,
                    "error": {"message": "Request failed", "stack_trace": f"token={SECRET}"},
                },
            },
            "message #1 (TRACE)",
            id="trace_stack_trace",
        ),
        pytest.param(
            {
                "type": "CONNECTION_STATUS",
                "connectionStatus": {"status": "FAILED", "message": f"401 for {SECRET}"},
            },
            "message #1 (CONNECTION_STATUS)",
            id="connection_status",
        ),
        pytest.param(
            {
                "type": "RECORD",
                "record": {"stream": "users", "data": {"token": SECRET}, "emitted_at": 0},
            },
            "message #1 (RECORD, stream `users`)",
            id="record_names_the_stream",
        ),
        pytest.param(
            {
                "type": "TRACE",
                "trace": {
                    "type": "ERROR",
                    "emitted_at": 0,
                    "error": {
                        "message": f"Failed with {SECRET}",
                        "stream_descriptor": {"name": "users"},
                    },
                },
            },
            "message #1 (TRACE, stream `users`)",
            id="trace_names_the_stream",
        ),
        pytest.param(
            {
                "type": "RECORD",
                "record": {"stream": f"users_{SECRET}", "data": {}, "emitted_at": 0},
            },
            "message #1 (RECORD)",
            id="stream_name_holding_a_secret_is_left_out",
        ),
    ],
)
def test_find_leaked_secrets_reports_where_without_any_text(
    message: dict[str, Any], location: str
) -> None:
    leaks = find_leaked_secrets(_output(message), [("/api_key", SECRET)])
    assert leaks == [f"`/api_key` in {location}"]


def test_find_leaked_secrets_numbers_messages_and_groups_pointers() -> None:
    other_secret = "pk_test_99887766554433"
    output = _output(
        _log("starting"),
        _log(f"{SECRET} and {other_secret}"),
        _log(f"again {SECRET}"),
    )
    leaks = find_leaked_secrets(output, [("/b", other_secret), ("/a", SECRET)])
    assert leaks == ["`/a`, `/b` in message #2 (LOG)", "`/a` in message #3 (LOG)"]


def test_find_leaked_secrets_searches_stderr() -> None:
    output = EntrypointOutput(
        messages=[json.dumps(_log("ok"))],
        stderr=f"Traceback (most recent call last):\n  KeyError: {SECRET}\n",
    )
    assert find_leaked_secrets(output, [("/api_key", SECRET)]) == ["`/api_key` in stderr line 2"]


def test_find_leaked_secrets_ignores_control_messages() -> None:
    control_message = {
        "type": "CONTROL",
        "control": {
            "type": "CONNECTOR_CONFIG",
            "emitted_at": 0,
            "connectorConfig": {"config": {"api_key": SECRET}},
        },
    }
    assert find_leaked_secrets(_output(control_message), [("/api_key", SECRET)]) == []


@pytest.mark.parametrize(
    "secret, text",
    [
        pytest.param("123", "Fetched 123 records", id="too_short"),
        pytest.param("invalid_api_key", "Got invalid_api_key from the API", id="no_digit"),
        pytest.param(True, "True", id="boolean"),
        pytest.param(None, "None", id="none"),
    ],
)
def test_find_leaked_secrets_skips_values_that_match_by_coincidence(secret: Any, text: str) -> None:
    assert find_leaked_secrets(_output(_log(text)), [("/secret", secret)]) == []


_PRIVATE_KEY = (
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKj\n"
    "MzEfYyjiWA4R4/M2bS1GB4t7NXp98C3SC6dVMvDuictGeurT8jNbvJZHtCSuYEvu\n"
    'NMoSfm76oqFvAp8Gy0iz5sxjZmSnXyCdPEovGhLa0VzMaQ8s+CLOyS56YyCFGeJZ"q\n'
    "-----END PRIVATE KEY-----\n"
)
_SERVICE_ACCOUNT_INFO = json.dumps(
    {
        "type": "service_account",
        "project_id": "test-project-424242",
        "private_key_id": "0f1e2d3c4b5a69788796a5b4c3d2e1f0aabbccdd",
        "private_key": _PRIVATE_KEY,
        "client_email": "airbyte@test-project-424242.iam.gserviceaccount.com",
    }
)
_MULTI_LINE_SECRETS_SPEC = _spec(
    {
        "api_key": {"type": "string", "airbyte_secret": True},
        "credentials": {
            "type": "object",
            "properties": {
                "private_key": {"type": "string", "airbyte_secret": True},
                "service_account_info": {"type": "string", "airbyte_secret": True},
            },
        },
    }
)


def _secret_fragments() -> list[str]:
    """Return every piece of the multi-line secrets that the failure text must not contain."""
    fragments: list[str] = []
    for secret in (_PRIVATE_KEY, _SERVICE_ACCOUNT_INFO):
        fragments += [
            secret,
            json.dumps(secret)[1:-1],
            repr(secret)[1:-1],
            base64.b64encode(secret.encode()).decode(),
        ]
    fragments += [line for line in _PRIVATE_KEY.splitlines() if "PRIVATE KEY" not in line]
    fragments += [line for line in json.dumps(_PRIVATE_KEY)[1:-1].split("\\n") if line]
    fragments += ["0f1e2d3c4b5a69788796a5b4c3d2e1f0aabbccdd", "test-project-424242"]
    return fragments


@pytest.mark.parametrize(
    "printed_text, expected_pointers",
    [
        pytest.param(
            lambda config: "bad config: " + json.dumps(config),
            ["/api_key", "/credentials/private_key", "/credentials/service_account_info"],
            id="json_dumped_config",
        ),
        pytest.param(
            lambda config: f"bad config: {config!r}",
            ["/api_key", "/credentials/private_key", "/credentials/service_account_info"],
            id="repr_of_config",
        ),
        pytest.param(
            lambda config: "key: " + json.dumps(config["credentials"]["private_key"]),
            ["/credentials/private_key", "/credentials/service_account_info"],
            id="json_escaped_key_alone",
        ),
        pytest.param(
            lambda config: "line: " + config["credentials"]["private_key"].splitlines()[2],
            ["/credentials/private_key", "/credentials/service_account_info"],
            id="single_line_of_the_key",
        ),
    ],
)
def test_leak_report_contains_no_part_of_a_multi_line_secret(
    printed_text: Any,
    expected_pointers: list[str],
) -> None:
    config = {
        "api_key": SECRET,
        "credentials": {
            "private_key": _PRIVATE_KEY,
            "service_account_info": _SERVICE_ACCOUNT_INFO,
            "auth_type": "service_account",
        },
    }
    text = printed_text(config)
    output = EntrypointOutput(
        messages=[
            json.dumps(
                {
                    "type": "CONNECTION_STATUS",
                    "connectionStatus": {"status": "FAILED", "message": text},
                }
            ),
            json.dumps(_log(base64.b64encode(_PRIVATE_KEY.encode()).decode())),
        ],
        stderr=text,
    )
    with pytest.raises(AssertionError) as error:
        assert_no_secrets_in_output(
            output,
            spec=ConnectorSpecification(connectionSpecification=_MULTI_LINE_SECRETS_SPEC),
            config=config,
            verb="check",
            connector_name="source-test",
        )

    failure_text = str(error.value)
    pointers = ", ".join(f"`{pointer}`" for pointer in expected_pointers)
    assert f"{pointers} in message #1 (CONNECTION_STATUS)" in failure_text
    assert "stderr line 1" in failure_text
    assert "message #2" not in failure_text, "base64 is not searched, so it is not reported"
    for fragment in _secret_fragments():
        assert fragment not in failure_text
    for line in failure_text.splitlines()[1:]:
        assert line.startswith("`/"), line


_ONESIGNAL_STYLE_SPEC = _spec(
    {
        "user_auth_key": {"type": "string", "airbyte_secret": True},
        "applications": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "app_id": {"type": "string"},
                    "app_api_key": {"type": "string", "airbyte_secret": True},
                },
            },
        },
    }
)


@pytest.mark.parametrize(
    "spec, config, expected",
    [
        pytest.param(
            _ONESIGNAL_STYLE_SPEC,
            {
                "user_auth_key": "uak_0000000000000001",
                "applications": [
                    {"app_id": "app-1", "app_api_key": "key_0000000000000001"},
                    {"app_id": "app-2", "app_api_key": "key_0000000000000002"},
                ],
            },
            [
                ("/user_auth_key", "uak_0000000000000001"),
                ("/applications/0/app_api_key", "key_0000000000000001"),
                ("/applications/1/app_api_key", "key_0000000000000002"),
            ],
            id="secrets_in_array_items",
        ),
        pytest.param(
            _spec(
                {
                    "auth": {
                        "anyOf": [
                            {"type": "object", "properties": {"token": {"airbyte_secret": True}}},
                            {"type": "object", "properties": {"user": {"type": "string"}}},
                        ],
                        "allOf": [
                            {"properties": {"password": {"airbyte_secret": True}}},
                        ],
                    },
                }
            ),
            {"auth": {"token": "t-1", "password": "p-1", "user": "u"}},
            [("/auth/token", "t-1"), ("/auth/password", "p-1")],
            id="any_of_and_all_of_variants",
        ),
        pytest.param(
            _spec({"a/b": {"type": "string", "airbyte_secret": True}}),
            {"a/b": "s-1"},
            [("/a~1b", "s-1")],
            id="pointer_is_escaped",
        ),
        pytest.param(
            _spec({"headers": {"type": "object", "airbyte_secret": True}}),
            {"headers": {"X-Token": "tok-1", "region2025x": ["tok-2"]}},
            [("/headers", "tok-1"), ("/headers", "tok-2")],
            id="secret_object_yields_values_not_keys",
        ),
        pytest.param(
            _spec(
                {
                    "credentials": {
                        "type": "object",
                        "oneOf": [
                            {
                                "properties": {
                                    "auth": {"const": "oauth"},
                                    "client_id": {"type": "string", "airbyte_secret": True},
                                }
                            },
                            {
                                "properties": {
                                    "auth": {"enum": ["basic"]},
                                    "client_id": {"type": "string"},
                                    "password": {"type": "string", "airbyte_secret": True},
                                }
                            },
                        ],
                    }
                }
            ),
            {"credentials": {"auth": "basic", "client_id": "cid-12345678", "password": "p-1"}},
            [("/credentials/password", "p-1")],
            id="one_of_walks_only_the_selected_variant",
        ),
        pytest.param(
            _spec(
                {
                    "credentials": {
                        "type": "object",
                        "oneOf": [
                            {"properties": {"client_secret": {"airbyte_secret": True}}},
                            {"properties": {"api_key": {"airbyte_secret": True}}},
                        ],
                    }
                }
            ),
            {"credentials": {"client_secret": "s-1", "api_key": "k-1"}},
            [("/credentials/client_secret", "s-1"), ("/credentials/api_key", "k-1")],
            id="one_of_without_discriminators_walks_every_variant",
        ),
        pytest.param(
            _spec(
                {
                    "credentials": {
                        "type": "object",
                        "oneOf": [
                            {
                                "properties": {
                                    "auth": {"const": "oauth"},
                                    "client_secret": {"airbyte_secret": True},
                                }
                            },
                            {
                                "properties": {
                                    "auth": {"const": "token"},
                                    "api_key": {"airbyte_secret": True},
                                }
                            },
                        ],
                    }
                }
            ),
            {"credentials": {"client_secret": "s-1", "api_key": "k-1"}},
            [("/credentials/client_secret", "s-1"), ("/credentials/api_key", "k-1")],
            id="one_of_with_no_matching_discriminator_walks_every_variant",
        ),
    ],
)
def test_find_config_secrets(
    spec: dict[str, Any], config: dict[str, Any], expected: list[tuple[str, Any]]
) -> None:
    assert find_config_secrets(spec, config) == expected


@pytest.mark.parametrize(
    "spec, config",
    [
        pytest.param(_ONESIGNAL_STYLE_SPEC, {"user_auth_key": "a", "applications": []}, id="items"),
        pytest.param(
            _spec(
                {
                    "credentials": {
                        "type": "object",
                        "oneOf": [
                            {"properties": {"client_secret": {"airbyte_secret": True}}},
                            {"properties": {"api_key": {"airbyte_secret": True}}},
                        ],
                    },
                    "password": {"type": "string", "airbyte_secret": True},
                }
            ),
            {"credentials": {"api_key": "k"}, "password": "p"},
            id="one_of",
        ),
        pytest.param(_MULTI_LINE_SECRETS_SPEC, {"api_key": "k", "credentials": {}}, id="nested"),
    ],
)
def test_find_config_secrets_finds_every_runtime_secret(
    spec: dict[str, Any], config: dict[str, Any]
) -> None:
    """Every value the runtime log filter masks, in the variant the config selects, is searched."""
    found = {value for _, value in find_config_secrets(spec, config)}
    assert set(get_secrets(spec, config)) <= found


def test_assert_no_secrets_in_output_reads_secret_paths_from_the_spec() -> None:
    spec = ConnectorSpecification(
        connectionSpecification=_spec(
            {
                "credentials": {
                    "type": "object",
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {"api_key": {"type": "string", "airbyte_secret": True}},
                        }
                    ],
                },
                "account_id": {"type": "string"},
            }
        )
    )
    config = {"credentials": {"api_key": SECRET}, "account_id": "acct_12345678"}

    # The non-secret account ID may appear in the output.
    assert_no_secrets_in_output(
        _output(_log("Connected to acct_12345678")),
        spec=spec,
        config=config,
        verb="check",
        connector_name="source-test",
    )
    with pytest.raises(AssertionError, match="`check` for connector 'source-test' printed"):
        assert_no_secrets_in_output(
            _output(_log(f"Connected with {SECRET}")),
            spec=spec,
            config=config,
            verb="check",
            connector_name="source-test",
        )


class _DockerSuite(DockerConnectorTestSuite):
    @classmethod
    def get_connector_root_dir(cls) -> Path:
        return POKEAPI_CONNECTOR_ROOT


@pytest.fixture(autouse=True)
def _clear_docker_spec_cache() -> Iterator[None]:
    docker_base._docker_spec_cache.clear()
    yield
    docker_base._docker_spec_cache.clear()


def _fake_docker(
    monkeypatch: pytest.MonkeyPatch,
    outputs: dict[str, EntrypointOutput],
) -> list[list[str]]:
    """Make `run_docker_airbyte_command` return `outputs[verb]` and record each command."""
    commands: list[list[str]] = []

    def run_docker_airbyte_command(cmd: list[str], *, raise_if_errors: bool) -> EntrypointOutput:
        commands.append(cmd)
        verb = next(verb for verb in outputs if verb in cmd)
        if raise_if_errors:
            outputs[verb].raise_if_errors()
        return outputs[verb]

    monkeypatch.setattr(docker_base, "run_docker_airbyte_command", run_docker_airbyte_command)
    return commands


def test_docker_spec_test_fails_when_the_image_emits_no_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_docker(monkeypatch, {"spec": _output(_log("starting up"))})
    with pytest.raises(AssertionError, match="emitted 0 SPEC messages"):
        _DockerSuite().test_docker_image_build_and_spec(
            connector_image_override="source-test:dev",
            connector_base_image_override=None,
        )


def test_docker_spec_test_lints_the_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_docker(monkeypatch, {"spec": _output(_spec_message({"api_key": {"type": "string"}}))})
    with pytest.raises(AssertionError, match="/properties/api_key"):
        _DockerSuite().test_docker_image_build_and_spec(
            connector_image_override="source-test:dev",
            connector_base_image_override=None,
        )


_SECRET_SPEC_MESSAGE = _spec_message({"api_key": {"type": "string", "airbyte_secret": True}})


def _connection_status(status: str, message: str) -> dict[str, Any]:
    return {"type": "CONNECTION_STATUS", "connectionStatus": {"status": status, "message": message}}


def _run_docker_check(scenario: ConnectorTestScenario) -> None:
    _DockerSuite().test_docker_image_build_and_check(
        scenario=scenario,
        connector_image_override="source-test:dev",
        connector_base_image_override=None,
    )


@pytest.mark.parametrize(
    "status_message, should_pass",
    [
        pytest.param("Invalid API key", True, id="no_leak"),
        pytest.param(f"Invalid API key {SECRET}", False, id="leak"),
    ],
)
def test_docker_check_test_fails_when_check_prints_a_secret(
    monkeypatch: pytest.MonkeyPatch,
    status_message: str,
    should_pass: bool,
) -> None:
    check_output = _output(_connection_status("FAILED", status_message))
    _fake_docker(monkeypatch, {"spec": _output(_SECRET_SPEC_MESSAGE), "check": check_output})
    scenario = ConnectorTestScenario(config_dict={"api_key": SECRET}, status="failed")

    if should_pass:
        _run_docker_check(scenario)
    else:
        with pytest.raises(AssertionError, match="printed secret values"):
            _run_docker_check(scenario)


@pytest.mark.parametrize(
    "check_output",
    [
        pytest.param(
            _output(_connection_status("FAILED", f"401 Unauthorized: token {SECRET}")),
            id="failed_status_on_a_success_scenario",
        ),
        pytest.param(
            _output(
                {
                    "type": "TRACE",
                    "trace": {
                        "type": "ERROR",
                        "emitted_at": 0,
                        "error": {"message": "boom", "stack_trace": f"KeyError: {SECRET}"},
                    },
                }
            ),
            id="trace_error_on_a_success_scenario",
        ),
        pytest.param(
            EntrypointOutput(
                messages=[json.dumps(_connection_status("SUCCEEDED", "ok"))],
                stderr=f"warning: retrying with {SECRET}",
            ),
            id="stderr",
        ),
    ],
)
def test_docker_check_test_runs_the_leak_check_before_the_outcome_assertions(
    monkeypatch: pytest.MonkeyPatch,
    check_output: EntrypointOutput,
) -> None:
    """The outcome assertions print raw messages, so the leak check must fail first."""
    _fake_docker(monkeypatch, {"spec": _output(_SECRET_SPEC_MESSAGE), "check": check_output})
    scenario = ConnectorTestScenario(config_dict={"api_key": SECRET}, status="succeed")

    with pytest.raises(AssertionError, match="printed secret values") as error:
        _run_docker_check(scenario)
    assert SECRET not in str(error.value)


def test_docker_check_test_still_asserts_the_outcome_without_a_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_output = _output(_connection_status("FAILED", "401 Unauthorized"))
    _fake_docker(monkeypatch, {"spec": _output(_SECRET_SPEC_MESSAGE), "check": check_output})
    scenario = ConnectorTestScenario(config_dict={"api_key": SECRET}, status="succeed")

    with pytest.raises(AssertionError, match="did not succeed"):
        _run_docker_check(scenario)


def test_docker_check_tests_share_one_spec_run(monkeypatch: pytest.MonkeyPatch) -> None:
    check_output = _output(_connection_status("FAILED", "Invalid API key"))
    commands = _fake_docker(
        monkeypatch, {"spec": _output(_SECRET_SPEC_MESSAGE), "check": check_output}
    )
    _DockerSuite().test_docker_image_build_and_spec(
        connector_image_override="source-test:dev",
        connector_base_image_override=None,
    )
    for _ in range(2):
        _run_docker_check(ConnectorTestScenario(config_dict={"api_key": SECRET}, status="failed"))

    assert [command[-1] for command in commands if "spec" in command] == ["spec"]


class _LeakySource(Source):
    """A source whose `check` echoes its API key in the connection status message."""

    succeed = False

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            connectionSpecification=_spec({"api_key": {"type": "string", "airbyte_secret": True}})
        )

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        status = Status.SUCCEEDED if self.succeed else Status.FAILED
        return AirbyteConnectionStatus(status=status, message=f"Used key {config['api_key']}")

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(streams=[])

    def read(self, *args: Any, **kwargs: Any) -> Iterator[AirbyteMessage]:
        yield from ()


class _SucceedingLeakySource(_LeakySource):
    succeed = True


class _InProcessSuite(SourceTestSuiteBase):
    connector = _LeakySource

    @classmethod
    def get_connector_root_dir(cls) -> Path:
        return POKEAPI_CONNECTOR_ROOT


@pytest.mark.parametrize(
    "connector, status",
    [
        pytest.param(_LeakySource, "succeed", id="failed_check_on_a_success_scenario"),
        pytest.param(_SucceedingLeakySource, "failed", id="succeeded_check_on_a_failure_scenario"),
    ],
)
@pytest.mark.parametrize(
    "suite_test",
    [
        pytest.param(SourceTestSuiteBase.test_check, id="source_suite"),
        pytest.param(ConnectorTestSuiteBase.test_check, id="connector_suite"),
    ],
)
def test_in_process_check_test_fails_when_check_prints_a_secret(
    monkeypatch: pytest.MonkeyPatch,
    connector: type[Source],
    status: str,
    suite_test: Any,
) -> None:
    """The leak check fails first, whatever the outcome, and prints no part of the secret."""
    monkeypatch.setattr(_InProcessSuite, "connector", connector)
    scenario = ConnectorTestScenario(config_dict={"api_key": SECRET}, status=status)

    with pytest.raises(AssertionError, match="printed secret values") as error:
        suite_test(_InProcessSuite(), scenario)
    assert "`/api_key` in message #" in str(error.value)
    assert SECRET not in str(error.value)


class _OptionalSecretSpec(BaseModel):
    """How a pydantic-v2 spec declares an optional secret: the flag sits next to `anyOf`."""

    api_key: Optional[str] = Field(None, json_schema_extra={"airbyte_secret": True})


class _OptionalUnmarkedSpec(BaseModel):
    api_key: Optional[str] = None


_NULLABLE_STRING = [{"type": "string"}, {"type": "null"}]
_MARKED_STRING_OR_NULL = [{"type": "string", "airbyte_secret": True}, {"type": "null"}]


@pytest.mark.parametrize(
    "connection_specification, expected_errors",
    [
        pytest.param(
            _OptionalSecretSpec.model_json_schema(),
            [],
            id="pydantic_v2_optional_secret_passes",
        ),
        pytest.param(
            _OptionalUnmarkedSpec.model_json_schema(),
            ["`/properties/api_key` looks like a secret"],
            id="pydantic_v2_optional_unmarked_secret_fails_once_at_the_property",
        ),
        pytest.param(
            _spec({"api_key": {"anyOf": _MARKED_STRING_OR_NULL}}),
            [
                "`/properties/api_key/anyOf/0` sets `airbyte_secret: true` under `anyOf`, where "
                "the CDK's secret filter does not look, so the value is not masked in logs. Set it "
                "on `/properties/api_key` instead."
            ],
            id="flag_inside_any_of_variant_fails",
        ),
        pytest.param(
            _spec({"api_key": {"allOf": _MARKED_STRING_OR_NULL[:1]}}),
            ["`/properties/api_key/allOf/0` sets `airbyte_secret: true` under `allOf`"],
            id="flag_inside_all_of_variant_fails",
        ),
        pytest.param(
            _spec({"api_key": {"oneOf": _MARKED_STRING_OR_NULL}}),
            [],
            id="flag_inside_one_of_variant_passes",
        ),
        pytest.param(
            _spec(
                {
                    "credentials": {
                        "anyOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "token": {"type": "string", "airbyte_secret": True},
                                },
                            }
                        ]
                    }
                }
            ),
            [
                "`/properties/credentials/anyOf/0/properties/token` sets `airbyte_secret: true` "
                "under `anyOf`, where the CDK's secret filter does not look, so the value is not "
                "masked in logs. Declare the variants with `oneOf` instead of `anyOf`."
            ],
            id="flag_on_a_property_of_an_any_of_object_fails",
        ),
        pytest.param(
            {
                "type": "object",
                "oneOf": [
                    {"properties": {"api_key": {"type": "string", "airbyte_secret": True}}},
                ],
            },
            [
                "`/oneOf/0/properties/api_key` sets `airbyte_secret: true` under `oneOf`, where "
                "the CDK's secret filter does not look, so the value is not masked in logs. The "
                "filter reads only the top-level `properties` of the spec."
            ],
            id="flag_under_a_top_level_one_of_fails",
        ),
        pytest.param(
            _spec({"password": {"airbyte_secret": True, "anyOf": [{"type": "boolean"}]}}),
            [
                "`/properties/password` is marked `airbyte_secret: true` but its type `boolean` "
                "cannot hold a secret value."
            ],
            id="marked_property_with_only_boolean_variants_fails",
        ),
        pytest.param(
            _spec({"api_key": {"anyOf": _NULLABLE_STRING}}),
            ["`/properties/api_key` looks like a secret"],
            id="unmarked_property_typed_by_its_variants_fails",
        ),
        pytest.param(
            _spec({"key": {"type": "string", "airbyte_secret": False}}),
            [],
            id="explicit_false_records_a_non_secret",
        ),
        pytest.param(
            _spec({"sort": {"type": "object", "properties": {"key": {"type": "string"}}}}),
            [
                "`/properties/sort/properties/key` looks like a secret but is not marked "
                "`airbyte_secret: true`. If it holds no secret, set `airbyte_secret: false` on it "
                "explicitly."
            ],
            id="failure_text_names_the_opt_out",
        ),
    ],
)
def test_find_secret_marking_errors_follows_the_runtime_secret_filter(
    connection_specification: dict[str, Any],
    expected_errors: list[str],
) -> None:
    errors = find_secret_marking_errors(connection_specification)
    assert len(errors) == len(expected_errors), errors
    for error, expected_error in zip(errors, expected_errors):
        assert error.startswith(expected_error), error


@pytest.mark.parametrize(
    "connection_specification, config",
    [
        pytest.param(_OptionalSecretSpec.model_json_schema(), {"api_key": SECRET}, id="optional"),
        pytest.param(_spec({"api_key": {"anyOf": _MARKED_STRING_OR_NULL}}), {"api_key": SECRET}),
        pytest.param(
            _spec({"api_key": {"allOf": _MARKED_STRING_OR_NULL[:1]}}), {"api_key": SECRET}
        ),
        pytest.param(_spec({"api_key": {"oneOf": _MARKED_STRING_OR_NULL}}), {"api_key": SECRET}),
        pytest.param(
            _spec(
                {
                    "credentials": {
                        "anyOf": [
                            {"properties": {"token": {"type": "string", "airbyte_secret": True}}}
                        ]
                    }
                }
            ),
            {"credentials": {"token": SECRET}},
            id="any_of_object",
        ),
        pytest.param(
            _spec(
                {
                    "credentials": {
                        "oneOf": [
                            {"properties": {"token": {"type": "string", "airbyte_secret": True}}}
                        ]
                    }
                }
            ),
            {"credentials": {"token": SECRET}},
            id="one_of_object",
        ),
        pytest.param(
            {
                "type": "object",
                "oneOf": [{"properties": {"api_key": {"type": "string", "airbyte_secret": True}}}],
            },
            {"api_key": SECRET},
            id="top_level_one_of",
        ),
    ],
)
def test_marking_lint_agrees_with_the_runtime_secret_filter(
    connection_specification: dict[str, Any], config: dict[str, Any]
) -> None:
    """The lint reports a flag the runtime cannot resolve exactly when `get_secrets` misses it."""
    errors = find_secret_marking_errors(connection_specification)
    lint_says_unmasked = any("where the CDK's secret filter does not look" in e for e in errors)
    runtime_masks = SECRET in get_secrets(connection_specification, config)
    assert lint_says_unmasked != runtime_masks, errors


def test_find_leaked_secrets_finds_a_numeric_secret_emitted_as_a_json_number() -> None:
    record = {
        "type": "RECORD",
        "record": {"stream": "pins", "data": {"pin": 12345678}, "emitted_at": 0},
    }
    leaks = find_leaked_secrets(_output(record), [("/pin", 12345678)])
    assert leaks == ["`/pin` in message #1 (RECORD, stream `pins`)"]


@pytest.mark.parametrize(
    "secret, printed_text",
    [
        pytest.param('ab"cd\\1234ef', lambda config: json.dumps(config), id="json_escaped"),
        pytest.param(
            "p@ss/w0rd&123",
            lambda config: "GET https://api.example.com/?token="
            + quote(config["api_key"], safe=""),
            id="url_encoded",
        ),
        pytest.param("it's\"s3cret", lambda config: f"config: {config!r}", id="repr_escaped"),
    ],
)
def test_find_leaked_secrets_finds_each_encoded_form(secret: str, printed_text: Any) -> None:
    text = printed_text({"api_key": secret})
    assert secret not in text, "the literal value must not match, so only the encoded form can"
    leaks = find_leaked_secrets(_output(_log(text)), [("/api_key", secret)])
    assert leaks == ["`/api_key` in message #1 (LOG)"]


def test_find_leaked_secrets_shows_the_stream_despite_a_short_unrelated_secret() -> None:
    record = {
        "type": "RECORD",
        "record": {"stream": "users", "data": {"t": SECRET}, "emitted_at": 0},
    }
    leaks = find_leaked_secrets(_output(record), [("/api_key", SECRET), ("/pin", "s")])
    assert leaks == ["`/api_key` in message #1 (RECORD, stream `users`)"]


def test_leak_in_a_cdk_config_validation_status_is_labeled() -> None:
    """A CDK older than the masking fix leaks the value; the failure says the CDK built it."""
    output = _output(
        _connection_status("FAILED", f"Config validation error: '{SECRET}' does not match '^a$'")
    )
    with pytest.raises(AssertionError) as error:
        assert_no_secrets_in_output(
            output,
            spec=ConnectorSpecification(
                connectionSpecification=_SECRET_SPEC_MESSAGE["spec"]["connectionSpecification"]
            ),
            config={"api_key": SECRET},
            verb="check",
            connector_name="source-test",
        )
    failure_text = str(error.value)
    assert (
        "`/api_key` in message #1 (CONNECTION_STATUS, built by the CDK's config validation)"
        in failure_text
    )
    assert "Upgrade `airbyte-cdk`, or make the scenario config pass the spec." in failure_text
    assert SECRET not in failure_text


class _PatternSecretSource(_LeakySource):
    """A source whose spec rejects the scenario's API key, so only the CDK reports on it."""

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            connectionSpecification=_spec(
                {"api_key": {"type": "string", "pattern": "^[a-z]+$", "airbyte_secret": True}}
            )
        )

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        raise AssertionError("connector code must not run when the config fails validation")


@pytest.mark.parametrize(
    "suite_test",
    [
        pytest.param(SourceTestSuiteBase.test_check, id="source_suite"),
        pytest.param(ConnectorTestSuiteBase.test_check, id="connector_suite"),
    ],
)
def test_in_process_check_passes_when_the_config_fails_validation(
    monkeypatch: pytest.MonkeyPatch, suite_test: Any
) -> None:
    """The CDK masks the secret in its own `Config validation error` CONNECTION_STATUS."""
    monkeypatch.setattr(_InProcessSuite, "connector", _PatternSecretSource)
    scenario = ConnectorTestScenario(config_dict={"api_key": "abcd1234"}, status="failed")
    suite_test(_InProcessSuite(), scenario)


class _UnmarkedSpecSource(_LeakySource):
    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            connectionSpecification=_spec({"api_key": {"type": "string"}})
        )


def test_in_process_spec_test_lints_the_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_InProcessSuite, "connector", _UnmarkedSpecSource)
    with pytest.raises(AssertionError, match="/properties/api_key"):
        _InProcessSuite().test_spec()


def test_in_process_spec_test_requires_a_spec_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(source_base, "run_test_job", lambda *args, **kwargs: _output(_log("hi")))
    with pytest.raises(AssertionError, match="emitted 0 SPEC messages"):
        _InProcessSuite().test_spec()


class _BrokenSpecSource(_LeakySource):
    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        raise ValueError("No module named 'source_test.run'")


def test_in_process_check_is_skipped_when_spec_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_InProcessSuite, "connector", _BrokenSpecSource)
    scenario = ConnectorTestScenario(config_dict={"api_key": SECRET}, status="failed")
    with pytest.raises(pytest.skip.Exception, match="`spec` failed"):
        _InProcessSuite().test_check(scenario)


def test_in_process_check_is_skipped_when_the_spec_run_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_run_test_job = connector_base.run_test_job

    def run_test_job(connector: Any, verb: str, **kwargs: Any) -> EntrypointOutput:
        if verb == "spec":
            raise AirbyteEntrypointException("spec failed")
        return real_run_test_job(connector, verb, **kwargs)

    monkeypatch.setattr(connector_base, "run_test_job", run_test_job)
    scenario = ConnectorTestScenario(config_dict={"api_key": SECRET}, status="failed")
    with pytest.raises(pytest.skip.Exception, match="`spec` failed"):
        ConnectorTestSuiteBase.test_check(_InProcessSuite(), scenario)


_TRACE_ERROR = {
    "type": "TRACE",
    "trace": {"type": "ERROR", "emitted_at": 0, "error": {"message": "Invalid API key"}},
}


def test_docker_check_accepts_a_trace_error_on_a_failure_scenario(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_output = _output(_TRACE_ERROR, _connection_status("FAILED", "Invalid API key"))
    _fake_docker(monkeypatch, {"spec": _output(_SECRET_SPEC_MESSAGE), "check": check_output})
    _run_docker_check(ConnectorTestScenario(config_dict={"api_key": SECRET}, status="failed"))


def test_docker_check_raises_a_trace_error_on_a_success_scenario(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_output = _output(_TRACE_ERROR, _connection_status("SUCCEEDED", "ok"))
    _fake_docker(monkeypatch, {"spec": _output(_SECRET_SPEC_MESSAGE), "check": check_output})
    with pytest.raises(AirbyteEntrypointException, match="Invalid API key"):
        _run_docker_check(ConnectorTestScenario(config_dict={"api_key": SECRET}, status="succeed"))


def test_docker_spec_failure_runs_spec_once_and_skips_the_check_tests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_output = _output(_connection_status("FAILED", "Invalid API key"))
    commands = _fake_docker(monkeypatch, {"spec": _output(_TRACE_ERROR), "check": check_output})
    with pytest.raises(AirbyteEntrypointException):
        _DockerSuite().test_docker_image_build_and_spec(
            connector_image_override="source-test:dev",
            connector_base_image_override=None,
        )
    for _ in range(2):
        with pytest.raises(pytest.skip.Exception, match="`spec` failed"):
            _run_docker_check(
                ConnectorTestScenario(config_dict={"api_key": SECRET}, status="failed")
            )

    assert [command[-1] for command in commands if "spec" in command] == ["spec"]


def test_run_docker_airbyte_command_keeps_stderr(monkeypatch: pytest.MonkeyPatch) -> None:
    def run_docker_command(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="warning: retrying\n")

    monkeypatch.setattr(docker_utils, "run_docker_command", run_docker_command)
    output = docker_utils.run_docker_airbyte_command(["docker", "run", "image", "spec"])
    assert output.stderr == "warning: retrying\n"
