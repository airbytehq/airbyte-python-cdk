# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the credential-free spec lint in `airbyte_cdk.test.standard_tests._spec_lint`."""

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from airbyte_cdk.models import ConnectorSpecification
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests import docker_base
from airbyte_cdk.test.standard_tests._spec_lint import (
    assert_no_secrets_in_output,
    assert_spec_is_valid,
    find_config_secrets,
    find_leaked_secrets,
    find_secret_marking_errors,
    get_single_spec,
    is_secret_property_name,
)
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
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
    ],
)
def test_is_secret_property_name(name: str, expected: bool) -> None:
    assert is_secret_property_name(name) == expected


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
            {"headers": {"X-Token": "tok-1", "extra": ["tok-2"]}},
            [
                ("/headers", "X-Token"),
                ("/headers", "tok-1"),
                ("/headers", "extra"),
                ("/headers", "tok-2"),
            ],
            id="secret_object_never_names_its_keys",
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
    """Every value the runtime log filter masks is also searched for in the output."""
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


def _fake_docker(
    monkeypatch: pytest.MonkeyPatch,
    outputs: dict[str, EntrypointOutput],
) -> list[list[str]]:
    """Make `run_docker_airbyte_command` return `outputs[verb]` and record each command."""
    commands: list[list[str]] = []

    def run_docker_airbyte_command(cmd: list[str], *, raise_if_errors: bool) -> EntrypointOutput:
        commands.append(cmd)
        verb = next(verb for verb in outputs if verb in cmd)
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
