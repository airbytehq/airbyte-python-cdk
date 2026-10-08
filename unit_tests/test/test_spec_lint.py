# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the credential-free spec lint in `airbyte_cdk.test.standard_tests._spec_lint`."""

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
    find_leaked_secrets,
    find_secret_marking_errors,
    get_single_spec,
    is_secret_property_name,
)
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite

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
    "message",
    [
        pytest.param(_log(f"Calling https://api.example.com/?key={SECRET}"), id="log"),
        pytest.param(
            {
                "type": "TRACE",
                "trace": {
                    "type": "ERROR",
                    "emitted_at": 0,
                    "error": {"message": "Request failed", "stack_trace": f"token={SECRET}"},
                },
            },
            id="trace_stack_trace",
        ),
        pytest.param(
            {
                "type": "CONNECTION_STATUS",
                "connectionStatus": {"status": "FAILED", "message": f"401 for {SECRET}"},
            },
            id="connection_status",
        ),
    ],
)
def test_find_leaked_secrets_reports_masked_excerpt(message: dict[str, Any]) -> None:
    leaks = find_leaked_secrets(_output(message), [SECRET])
    assert len(leaks) == 1
    assert leaks[0].startswith(message["type"])
    assert "****" in leaks[0]
    assert SECRET not in leaks[0]


def test_find_leaked_secrets_ignores_control_messages() -> None:
    control_message = {
        "type": "CONTROL",
        "control": {
            "type": "CONNECTOR_CONFIG",
            "emitted_at": 0,
            "connectorConfig": {"config": {"api_key": SECRET}},
        },
    }
    assert find_leaked_secrets(_output(control_message), [SECRET]) == []


@pytest.mark.parametrize(
    "secret, text",
    [
        pytest.param("123", "Fetched 123 records", id="too_short"),
        pytest.param("invalid_api_key", "Got invalid_api_key from the API", id="no_digit"),
        pytest.param(True, "True", id="boolean"),
        pytest.param({"nested": SECRET}, SECRET, id="not_a_scalar"),
    ],
)
def test_find_leaked_secrets_skips_values_that_match_by_coincidence(secret: Any, text: str) -> None:
    assert find_leaked_secrets(_output(_log(text)), [secret]) == []


def test_leak_excerpt_masks_overlapping_secrets_whole() -> None:
    short_secret = "abc12345"
    long_secret = short_secret + "6789xyz"
    leaks = find_leaked_secrets(_output(_log(f"token={long_secret}")), [short_secret, long_secret])
    assert leaks == ["LOG message: token=****"]


def test_leak_excerpt_is_centered_on_the_leak() -> None:
    text = "x" * 1000 + SECRET + "y" * 1000
    [leak] = find_leaked_secrets(_output(_log(text)), [SECRET])
    assert "****" in leak
    assert len(leak) < 400


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
    secret_spec = _spec_message({"api_key": {"type": "string", "airbyte_secret": True}})
    check_output = _output(
        {
            "type": "CONNECTION_STATUS",
            "connectionStatus": {"status": "FAILED", "message": status_message},
        }
    )
    _fake_docker(monkeypatch, {"spec": _output(secret_spec), "check": check_output})
    scenario = ConnectorTestScenario(config_dict={"api_key": SECRET}, status="failed")

    def run_check() -> None:
        _DockerSuite().test_docker_image_build_and_check(
            scenario=scenario,
            connector_image_override="source-test:dev",
            connector_base_image_override=None,
        )

    if should_pass:
        run_check()
    else:
        with pytest.raises(AssertionError, match="printed secret values"):
            run_check()
