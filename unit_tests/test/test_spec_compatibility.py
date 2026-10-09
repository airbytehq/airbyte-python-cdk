# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the spec backward-compatibility check."""

from __future__ import annotations

import http.server
import json
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import requests
import requests_mock
import yaml

from airbyte_cdk.test.standard_tests import _spec_compatibility, docker_base
from airbyte_cdk.test.standard_tests._spec_compatibility import (
    BREAKING_CHANGES_DOCS_URL,
    MAX_REPORTED_CHANGES,
    REGISTRY_TIMEOUT_SECONDS,
    PublishedSpec,
    SpecComparison,
    compare_specs,
    declared_breaking_changes,
    disabled_for_version,
    fetch_published_spec,
    format_breaking_spec_changes,
    format_spec_change_summary,
    is_newer_version,
    registry_retry,
)
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite


def _spec(
    properties: dict[str, Any],
    required: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "connectionSpecification": {
            "type": "object",
            "required": required if required is not None else ["api_key"],
            "properties": properties,
        },
        **extra,
    }


_BASE_PROPERTIES: dict[str, Any] = {
    "api_key": {"type": "string", "title": "API key", "airbyte_secret": True},
    "start_date": {"type": "string", "format": "date-time"},
}


# compare_specs


def test_unchanged_spec_has_nothing_to_report() -> None:
    comparison = compare_specs(_spec(_BASE_PROPERTIES), _spec(_BASE_PROPERTIES))

    assert comparison.is_backward_compatible
    assert not comparison.breaking
    assert not comparison.compatible


def test_added_optional_property_is_compatible() -> None:
    comparison = compare_specs(
        _spec(_BASE_PROPERTIES),
        _spec({**_BASE_PROPERTIES, "page_size": {"type": "integer"}}),
    )

    assert comparison.is_backward_compatible
    assert any("page_size" in change for change in comparison.compatible)


def test_removed_property_is_breaking() -> None:
    comparison = compare_specs(
        _spec(_BASE_PROPERTIES),
        _spec({"api_key": _BASE_PROPERTIES["api_key"]}),
    )

    assert comparison.breaking == ["`connectionSpecification.properties.start_date` was removed"]


def test_changed_property_type_is_breaking_and_dropped_format_is_compatible() -> None:
    comparison = compare_specs(
        _spec(_BASE_PROPERTIES),
        _spec({**_BASE_PROPERTIES, "start_date": {"type": "integer"}}),
    )

    assert (
        "`connectionSpecification.properties.start_date.type` no longer allows string"
        in comparison.breaking
    )
    assert (
        "`connectionSpecification.properties.start_date.format` was removed, "
        "widening what a config may set"
    ) in comparison.compatible


def test_newly_required_property_is_breaking_even_when_new() -> None:
    comparison = compare_specs(
        _spec(_BASE_PROPERTIES),
        _spec({**_BASE_PROPERTIES, "region": {"type": "string"}}, required=["api_key", "region"]),
    )

    assert comparison.breaking == ["`connectionSpecification`: `region` is now required"]


def test_first_required_list_is_breaking() -> None:
    previous = _spec(_BASE_PROPERTIES)
    del previous["connectionSpecification"]["required"]

    comparison = compare_specs(previous, _spec(_BASE_PROPERTIES, required=["start_date"]))

    assert comparison.breaking == ["`connectionSpecification`: `start_date` is now required"]


def test_dropped_requirement_is_compatible() -> None:
    comparison = compare_specs(
        _spec(_BASE_PROPERTIES, required=["api_key", "start_date"]),
        _spec(_BASE_PROPERTIES, required=["api_key"]),
    )

    assert comparison.is_backward_compatible
    assert any("no longer required" in change for change in comparison.compatible)


@pytest.mark.parametrize(
    "added",
    [
        pytest.param({"enum": ["us", "eu"]}, id="enum"),
        pytest.param({"pattern": "^[0-9]+$"}, id="pattern"),
        pytest.param({"const": "fixed"}, id="const"),
        pytest.param({"maxLength": 8}, id="maxLength"),
        pytest.param({"format": "date-time"}, id="format"),
        pytest.param({"additionalProperties": False}, id="additionalProperties"),
    ],
)
def test_added_constraint_is_breaking(added: dict[str, Any]) -> None:
    comparison = compare_specs(
        _spec({"region": {"type": "string"}}),
        _spec({"region": {"type": "string", **added}}),
    )

    assert len(comparison.breaking) == 1
    assert "narrowing what a config may set" in comparison.breaking[0]


def test_becoming_nullable_is_compatible() -> None:
    comparison = compare_specs(
        _spec({"start_date": {"type": "string"}}),
        _spec({"start_date": {"type": ["null", "string"]}}),
    )

    assert comparison.is_backward_compatible
    assert "also allows null" in comparison.compatible[0]


def test_becoming_non_nullable_is_breaking() -> None:
    comparison = compare_specs(
        _spec({"start_date": {"type": ["null", "string"]}}),
        _spec({"start_date": {"type": "string"}}),
    )

    assert comparison.breaking == [
        "`connectionSpecification.properties.start_date.type` no longer allows null"
    ]


def test_changed_default_is_compatible() -> None:
    comparison = compare_specs(
        _spec({"page_size": {"type": "integer", "default": 100}}),
        _spec({"page_size": {"type": "integer", "default": 500}}),
    )

    assert comparison.is_backward_compatible
    assert "changes behavior, not validity" in comparison.compatible[0]


@pytest.mark.parametrize(
    "key, previous_bound, current_bound, compatible",
    [
        pytest.param("maximum", 100, 1000, True, id="raised-maximum-relaxes"),
        pytest.param("maximum", 1000, 100, False, id="lowered-maximum-tightens"),
        pytest.param("minLength", 8, 1, True, id="lowered-minimum-relaxes"),
        pytest.param("minLength", 1, 8, False, id="raised-minimum-tightens"),
    ],
)
def test_moved_bound_is_judged_by_its_direction(
    key: str, previous_bound: int, current_bound: int, compatible: bool
) -> None:
    comparison = compare_specs(
        _spec({"page_size": {"type": "integer", key: previous_bound}}),
        _spec({"page_size": {"type": "integer", key: current_bound}}),
    )

    assert comparison.is_backward_compatible is compatible


@pytest.mark.parametrize(
    "key, previous_value, current_value, compatible",
    [
        pytest.param("additionalProperties", False, True, True, id="unsealed-relaxes"),
        pytest.param("additionalProperties", True, False, False, id="sealed-tightens"),
        pytest.param("uniqueItems", True, False, True, id="duplicates-allowed-relaxes"),
        pytest.param("uniqueItems", False, True, False, id="uniqueness-tightens"),
    ],
)
def test_flipped_boolean_constraint_is_judged_by_its_direction(
    key: str, previous_value: bool, current_value: bool, compatible: bool
) -> None:
    comparison = compare_specs(
        _spec({"options": {"type": "object", key: previous_value}}),
        _spec({"options": {"type": "object", key: current_value}}),
    )

    assert comparison.is_backward_compatible is compatible


def test_narrowed_enum_is_breaking_and_widened_enum_is_compatible() -> None:
    previous = _spec({"region": {"type": "string", "enum": ["us", "eu"]}})

    narrowed = compare_specs(previous, _spec({"region": {"type": "string", "enum": ["us"]}}))
    widened = compare_specs(
        previous, _spec({"region": {"type": "string", "enum": ["us", "eu", "apac"]}})
    )

    assert narrowed.breaking == [
        "`connectionSpecification.properties.region.enum` no longer allows 'eu'"
    ]
    assert widened.is_backward_compatible


def test_enum_of_objects_has_set_semantics() -> None:
    one = _spec({"mode": {"enum": [{"kind": "full"}]}})
    two = _spec({"mode": {"enum": [{"kind": "full"}, {"kind": "incremental"}]}})

    assert compare_specs(one, two).is_backward_compatible
    assert not compare_specs(two, one).is_backward_compatible


def test_removed_destination_sync_mode_is_breaking() -> None:
    previous = _spec({}, supported_destination_sync_modes=["append", "overwrite"])
    current = _spec({}, supported_destination_sync_modes=["overwrite"])

    assert compare_specs(previous, current).breaking == [
        "`supported_destination_sync_modes` no longer allows 'append'"
    ]


def test_documentation_changes_are_compatible() -> None:
    comparison = compare_specs(
        _spec(_BASE_PROPERTIES, documentationUrl="https://example.com/v1"),
        _spec(
            {
                **_BASE_PROPERTIES,
                "api_key": {
                    "type": "string",
                    "title": "API token",
                    "description": "The token to authenticate with.",
                    "order": 0,
                    "airbyte_secret": True,
                },
            },
            documentationUrl="https://example.com/v2",
        ),
    )

    assert comparison.is_backward_compatible


def test_config_field_named_description_is_still_a_property() -> None:
    comparison = compare_specs(
        _spec({**_BASE_PROPERTIES, "description": {"type": "string"}}),
        _spec(_BASE_PROPERTIES),
    )

    assert comparison.breaking == ["`connectionSpecification.properties.description` was removed"]


def test_config_field_named_properties_keeps_its_own_documentation() -> None:
    comparison = compare_specs(
        _spec({"properties": {"type": "object", "title": "Extra properties"}}),
        _spec({"properties": {"type": "object", "title": "Additional properties"}}),
    )

    assert comparison.is_backward_compatible


def test_finding_does_not_quote_a_whole_subtree() -> None:
    wide = {f"field_{index}": {"type": "string", "description": "x" * 40} for index in range(30)}

    comparison = compare_specs(
        _spec({"cfg": {"properties": wide}}),
        _spec({"cfg": {"properties": [1, 2]}}),
    )

    assert len(comparison.breaking) == 1
    assert len(comparison.breaking[0]) < 400
    assert comparison.breaking[0].endswith("to [1, 2]")


_OAUTH_BRANCH: dict[str, Any] = {
    "title": "OAuth",
    "properties": {
        "auth_type": {"type": "string", "const": "oauth2.0"},
        "client_id": {"type": "string"},
    },
    "required": ["auth_type", "client_id"],
}
_API_KEY_BRANCH: dict[str, Any] = {
    "title": "API key",
    "properties": {
        "auth_type": {"type": "string", "const": "api_key"},
        "api_key": {"type": "string"},
    },
    "required": ["auth_type", "api_key"],
}


def test_reordered_oneof_branches_are_unchanged() -> None:
    comparison = compare_specs(
        _spec({"credentials": {"oneOf": [_API_KEY_BRANCH, _OAUTH_BRANCH]}}),
        _spec({"credentials": {"oneOf": [_OAUTH_BRANCH, _API_KEY_BRANCH]}}),
    )

    assert comparison.is_backward_compatible
    assert not comparison.compatible


def test_removed_oneof_branch_is_breaking_when_the_rest_are_reordered() -> None:
    token = {"properties": {"auth_type": {"const": "token"}}}

    comparison = compare_specs(
        _spec({"credentials": {"oneOf": [_API_KEY_BRANCH, _OAUTH_BRANCH, token]}}),
        _spec({"credentials": {"oneOf": [_OAUTH_BRANCH, _API_KEY_BRANCH]}}),
    )

    assert comparison.breaking == [
        "`connectionSpecification.properties.credentials.oneOf[2]` was removed"
    ]


def test_new_required_field_inside_a_matched_branch_is_breaking() -> None:
    oauth_with_scope = {
        **_OAUTH_BRANCH,
        "properties": {**_OAUTH_BRANCH["properties"], "scope": {"type": "string"}},
        "required": [*_OAUTH_BRANCH["required"], "scope"],
    }

    comparison = compare_specs(
        _spec({"credentials": {"oneOf": [_API_KEY_BRANCH, _OAUTH_BRANCH]}}),
        _spec({"credentials": {"oneOf": [oauth_with_scope, _API_KEY_BRANCH]}}),
    )

    assert comparison.breaking == [
        "`connectionSpecification.properties.credentials.oneOf[1]`: `scope` is now required"
    ]


def test_branch_key_does_not_depend_on_property_order() -> None:
    previous = _spec(
        {
            "credentials": {
                "oneOf": [
                    {
                        "properties": {
                            "auth_type": {"const": "oauth2.0"},
                            "flavor": {"const": "std"},
                        }
                    }
                ]
            }
        }
    )
    current = _spec(
        {
            "credentials": {
                "oneOf": [
                    {
                        "properties": {
                            "flavor": {"const": "std"},
                            "auth_type": {"const": "oauth2.0"},
                            "scopes": {"type": "string"},
                        }
                    }
                ]
            }
        }
    )

    comparison = compare_specs(previous, current)

    assert comparison.is_backward_compatible
    assert any("scopes` was added" in change for change in comparison.compatible)


def test_swapped_auth_method_reads_as_one_removed_and_one_added() -> None:
    def branch(auth_type: str) -> dict[str, Any]:
        return {"properties": {"auth_type": {"const": auth_type}}, "required": ["auth_type"]}

    comparison = compare_specs(
        _spec({"credentials": {"oneOf": [branch("api_key")]}}),
        _spec({"credentials": {"oneOf": [branch("oauth2.0")]}}),
    )

    assert comparison.breaking == [
        "`connectionSpecification.properties.credentials.oneOf[0]` was removed"
    ]
    assert comparison.compatible == [
        "`connectionSpecification.properties.credentials.oneOf[0]` was added"
    ]


def test_moved_oauth_output_path_is_breaking() -> None:
    def advanced_auth(path: list[str]) -> dict[str, Any]:
        return {
            "auth_flow_type": "oauth2.0",
            "oauth_config_specification": {
                "complete_oauth_output_specification": {
                    "properties": {"access_token": {"path_in_connector_config": path}}
                }
            },
        }

    comparison = compare_specs(
        _spec({}, advanced_auth=advanced_auth(["access_token"])),
        _spec({}, advanced_auth=advanced_auth(["credentials", "access_token"])),
    )

    assert not comparison.is_backward_compatible


def _declarative_oauth(consent_flow: dict[str, Any] | None) -> dict[str, Any]:
    oauth_config_specification: dict[str, Any] = {
        "complete_oauth_output_specification": {
            "properties": {
                "refresh_token": {"path_in_connector_config": ["credentials", "refresh_token"]}
            }
        },
    }
    if consent_flow is not None:
        oauth_config_specification["oauth_connector_input_specification"] = consent_flow
    return {"auth_flow_type": "oauth2.0", "oauth_config_specification": oauth_config_specification}


@pytest.mark.parametrize(
    "current_consent_flow",
    [
        pytest.param(
            {
                "consent_url": "https://example.com/v2/authorize?scope={{ scope }}",
                "access_token_url": "https://example.com/v2/token",
                "scope": "read write",
                "extract_output": ["access_token", "refresh_token"],
            },
            id="urls-scope-and-outputs-changed",
        ),
        pytest.param(None, id="consent-flow-removed"),
    ],
)
def test_oauth_consent_flow_changes_are_compatible(
    current_consent_flow: dict[str, Any] | None,
) -> None:
    previous_consent_flow = {
        "consent_url": "https://example.com/authorize?scope={{ scope }}",
        "access_token_url": "https://example.com/token",
        "scope": "read",
        "extract_output": ["refresh_token"],
    }

    comparison = compare_specs(
        _spec({}, advanced_auth=_declarative_oauth(previous_consent_flow)),
        _spec({}, advanced_auth=_declarative_oauth(current_consent_flow)),
    )

    assert comparison.is_backward_compatible
    assert len(comparison.compatible) == 1
    assert "oauth_connector_input_specification" in comparison.compatible[0]


def test_spec_keys_existing_connections_do_not_depend_on_are_ignored() -> None:
    comparison = compare_specs(
        _spec(
            _BASE_PROPERTIES,
            supportsIncremental=True,
            supportsNormalization=True,
            protocol_version="0.2.0",
            supported_sync_modes=["full_refresh", "incremental"],
        ),
        _spec(_BASE_PROPERTIES, supportsIncremental=False),
    )

    assert comparison.is_backward_compatible
    assert not comparison.compatible


# Annotations: every key that is not a validation keyword or a config-locating protocol key


def _convex_published_spec() -> dict[str, Any]:
    """The shape of destination-convex 0.2.19 as the registry publishes it."""
    return {
        "connectionSpecification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Destination Convex",
            "type": "object",
            "required": ["deployment_url", "access_key"],
            "additionalProperties": False,
            "properties": {
                "deployment_url": {
                    "type": "string",
                    "description": "URL of the Convex deployment that is the destination",
                    "examples": ["https://murky-swan-635.convex.cloud"],
                },
                "access_key": {
                    "type": "string",
                    "description": "API access key used to send data to a Convex deployment.",
                    "airbyte_secret": "true",
                },
            },
        },
        "documentationUrl": "https://docs.airbyte.com/integrations/destinations/convex",
        "supported_destination_sync_modes": ["overwrite", "append", "append_dedup"],
        "supportsIncremental": True,
    }


def test_string_airbyte_secret_rewritten_as_a_boolean_is_compatible() -> None:
    previous = _convex_published_spec()
    current = _convex_published_spec()
    current["connectionSpecification"]["properties"]["access_key"]["airbyte_secret"] = True

    comparison = compare_specs(previous, current)

    assert comparison.is_backward_compatible
    assert comparison.compatible == [
        "`connectionSpecification.properties.access_key.airbyte_secret` changed from 'true' to "
        "True (same meaning)"
    ]


@pytest.mark.parametrize(
    "previous_value, current_value",
    [
        pytest.param(False, True, id="false-to-true"),
        pytest.param(None, True, id="added"),
        pytest.param(False, None, id="non-secret-flag-removed"),
        pytest.param("true", True, id="string-to-boolean"),
    ],
)
def test_marking_more_values_secret_is_compatible(previous_value: Any, current_value: Any) -> None:
    def spec(value: Any) -> dict[str, Any]:
        schema: dict[str, Any] = {"type": "string"}
        if value is not None:
            schema["airbyte_secret"] = value
        return _spec({"token": schema})

    assert compare_specs(spec(previous_value), spec(current_value)).is_backward_compatible


@pytest.mark.parametrize(
    "current_value",
    [pytest.param(False, id="true-to-false"), pytest.param(None, id="removed")],
)
@pytest.mark.parametrize("previous_value", [True, "true"])
def test_unmarking_a_secret_is_breaking(previous_value: Any, current_value: Any) -> None:
    current_schema: dict[str, Any] = {"type": "string"}
    if current_value is not None:
        current_schema["airbyte_secret"] = current_value

    comparison = compare_specs(
        _spec({"token": {"type": "string", "airbyte_secret": previous_value}}),
        _spec({"token": current_schema}),
    )

    assert len(comparison.breaking) == 1
    assert "no longer a secret" in comparison.breaking[0]


@pytest.mark.parametrize(
    "previous_schema, current_schema",
    [
        pytest.param({"airbyte_hidden": True}, {}, id="airbyte_hidden-removed"),
        pytest.param({}, {"airbyte_hidden": True}, id="airbyte_hidden-added"),
        pytest.param({"multiline": True}, {}, id="multiline-removed"),
        pytest.param({"always_show": True}, {"always_show": False}, id="always_show-changed"),
        pytest.param({"order": 1}, {"order": 3}, id="order-changed"),
        pytest.param({"group": "auth"}, {}, id="group-removed"),
        pytest.param({"examples": ["a"]}, {"examples": ["a", "b"]}, id="examples-changed"),
        pytest.param({"title": "Token"}, {}, id="title-removed"),
        pytest.param({"x-display": {"width": 2}}, {"x-display": {"width": 3}}, id="vendor-key"),
        pytest.param({"deprecated": False}, {"deprecated": True}, id="deprecated"),
    ],
)
def test_annotation_changes_are_compatible(
    previous_schema: dict[str, Any], current_schema: dict[str, Any]
) -> None:
    comparison = compare_specs(
        _spec({"token": {"type": "string", **previous_schema}}),
        _spec({"token": {"type": "string", **current_schema}}),
    )

    assert comparison.is_backward_compatible
    assert all("(annotation)" in change for change in comparison.compatible)


@pytest.mark.parametrize(
    "current_schema_keyword",
    [
        pytest.param({"$schema": "https://json-schema.org/draft-07/schema#"}, id="changed"),
        pytest.param({}, id="removed"),
    ],
)
def test_schema_keyword_changes_are_compatible(current_schema_keyword: dict[str, Any]) -> None:
    previous = _spec(_BASE_PROPERTIES)
    previous["connectionSpecification"]["$schema"] = "http://json-schema.org/draft-07/schema#"
    current = _spec(_BASE_PROPERTIES)
    current["connectionSpecification"].update(current_schema_keyword)

    assert compare_specs(previous, current).is_backward_compatible


def test_connection_specification_groups_are_annotations() -> None:
    previous = _spec(_BASE_PROPERTIES)
    previous["connectionSpecification"]["groups"] = [{"id": "auth", "title": "Auth"}]
    current = _spec(_BASE_PROPERTIES)
    current["connectionSpecification"]["groups"] = [{"id": "auth", "title": "Authentication"}]

    assert compare_specs(previous, current).is_backward_compatible


@pytest.mark.parametrize(
    "key, previous_value, current_value",
    [
        pytest.param("predicate_key", ["credentials", "auth_type"], ["auth_type"], id="moved"),
        pytest.param("predicate_value", "oauth2.0", "oauth", id="changed"),
        pytest.param("auth_flow_type", "oauth2.0", None, id="removed"),
    ],
)
def test_advanced_auth_protocol_keys_are_still_breaking(
    key: str, previous_value: Any, current_value: Any
) -> None:
    def advanced_auth(value: Any) -> dict[str, Any]:
        auth: dict[str, Any] = {"auth_flow_type": "oauth2.0", "predicate_value": "oauth2.0"}
        if value is None:
            del auth[key]
        else:
            auth[key] = value
        return auth

    comparison = compare_specs(
        _spec({}, advanced_auth=advanced_auth(previous_value)),
        _spec({}, advanced_auth=advanced_auth(current_value)),
    )

    assert not comparison.is_backward_compatible


@pytest.mark.parametrize(
    "previous_schema, current_schema, compatible",
    [
        pytest.param({}, {"not": {"const": "x"}}, False, id="added"),
        pytest.param(
            {"dependencies": {"a": ["b"]}}, {"dependencies": {"a": ["c"]}}, False, id="changed"
        ),
        pytest.param(
            {"if": {"required": ["a"]}, "then": {"required": ["b"]}}, {}, True, id="removed"
        ),
    ],
)
def test_unmodeled_validation_keywords(
    previous_schema: dict[str, Any], current_schema: dict[str, Any], compatible: bool
) -> None:
    comparison = compare_specs(
        _spec({"options": {"type": "object", **previous_schema}}),
        _spec({"options": {"type": "object", **current_schema}}),
    )

    assert comparison.is_backward_compatible is compatible


# Keywords at their default value


@pytest.mark.parametrize(
    "added",
    [
        pytest.param({"additionalProperties": True}, id="additionalProperties-true"),
        pytest.param({"additionalProperties": {}}, id="additionalProperties-empty-schema"),
        pytest.param({"uniqueItems": False}, id="uniqueItems-false"),
        pytest.param({"minLength": 0}, id="minLength-0"),
        pytest.param({"minItems": 0}, id="minItems-0"),
        pytest.param({"minProperties": 0}, id="minProperties-0"),
        pytest.param({"items": {}}, id="items-empty-schema"),
        pytest.param({"required": []}, id="empty-required"),
        pytest.param({"exclusiveMinimum": False}, id="draft-04-exclusiveMinimum-false"),
    ],
)
def test_keyword_added_at_its_default_is_compatible(added: dict[str, Any]) -> None:
    previous = _spec({"region": {"type": "string"}})
    current = _spec({"region": {"type": "string", **added}})

    assert compare_specs(previous, current).is_backward_compatible
    assert compare_specs(current, previous).is_backward_compatible


def test_root_additional_properties_true_after_a_manifest_migration_is_compatible() -> None:
    current = _spec(_BASE_PROPERTIES)
    current["connectionSpecification"]["additionalProperties"] = True

    comparison = compare_specs(_spec(_BASE_PROPERTIES), current)

    assert comparison.is_backward_compatible
    assert comparison.compatible == [
        "`connectionSpecification.additionalProperties` was added at its default value True, "
        "which allows the same configs"
    ]


@pytest.mark.parametrize(
    "added",
    [
        pytest.param({"minLength": 1}, id="minLength-1"),
        pytest.param({"uniqueItems": True}, id="uniqueItems-true"),
        pytest.param(
            {"additionalProperties": {"type": "string"}}, id="additionalProperties-schema"
        ),
        pytest.param({"exclusiveMaximum": True}, id="draft-04-exclusiveMaximum-true"),
        pytest.param({"minItems": False}, id="false-is-not-zero"),
    ],
)
def test_keyword_added_away_from_its_default_is_breaking(added: dict[str, Any]) -> None:
    comparison = compare_specs(
        _spec({"region": {"type": "string"}}),
        _spec({"region": {"type": "string", **added}}),
    )

    assert not comparison.is_backward_compatible


@pytest.mark.parametrize(
    "previous_value, current_value, compatible",
    [
        pytest.param({"type": "string"}, True, True, id="schema-relaxed-to-true"),
        pytest.param({"type": "string"}, {}, True, id="schema-relaxed-to-empty-schema"),
        pytest.param(True, {"type": "string"}, False, id="true-tightened-to-schema"),
        pytest.param(False, {}, True, id="false-relaxed-to-empty-schema"),
    ],
)
def test_additional_properties_schema_is_judged_by_its_direction(
    previous_value: Any, current_value: Any, compatible: bool
) -> None:
    comparison = compare_specs(
        _spec({"options": {"type": "object", "additionalProperties": previous_value}}),
        _spec({"options": {"type": "object", "additionalProperties": current_value}}),
    )

    assert comparison.is_backward_compatible is compatible


@pytest.mark.parametrize(
    "key, previous_value",
    [
        pytest.param("properties", {"client_id": {"type": "string"}}, id="properties-emptied"),
        pytest.param(
            "items",
            {"type": "object", "properties": {"name": {"type": "string"}}},
            id="items-emptied",
        ),
    ],
)
def test_emptying_a_field_keyword_still_removes_its_fields(key: str, previous_value: Any) -> None:
    comparison = compare_specs(
        _spec({"options": {"type": "object", key: previous_value}}),
        _spec({"options": {"type": "object", key: {}}}),
    )

    assert len(comparison.breaking) == 1
    assert comparison.breaking[0].endswith("was removed")


def test_draft_04_exclusive_bound_flag_is_judged_by_its_direction() -> None:
    def spec(exclusive: bool) -> dict[str, Any]:
        return _spec(
            {"page_size": {"type": "integer", "maximum": 10, "exclusiveMaximum": exclusive}}
        )

    assert not compare_specs(spec(False), spec(True)).is_backward_compatible
    assert compare_specs(spec(True), spec(False)).is_backward_compatible


# Integer and number


@pytest.mark.parametrize(
    "previous_type, current_type, compatible",
    [
        pytest.param("integer", "number", True, id="integer-widened-to-number"),
        pytest.param(["integer", "null"], ["null", "number"], True, id="nullable-widened"),
        pytest.param("number", "integer", False, id="number-narrowed-to-integer"),
        pytest.param("integer", "string", False, id="integer-to-string"),
    ],
)
def test_integer_and_number(previous_type: Any, current_type: Any, compatible: bool) -> None:
    comparison = compare_specs(
        _spec({"page_size": {"type": previous_type}}),
        _spec({"page_size": {"type": current_type}}),
    )

    assert comparison.is_backward_compatible is compatible


def test_integer_widened_to_number_is_reported_once() -> None:
    comparison = compare_specs(
        _spec({"page_size": {"type": "integer"}}),
        _spec({"page_size": {"type": "number"}}),
    )

    assert comparison.compatible == [
        "`connectionSpecification.properties.page_size.type` widened from integer to number"
    ]


# oneOf/anyOf branch identity


def _credentials(*branches: dict[str, Any]) -> dict[str, Any]:
    return _spec({"credentials": {"type": "object", "oneOf": list(branches)}})


def _branch(
    title: str, *fields: str, required: list[str] | None = None, **consts: str
) -> dict[str, Any]:
    properties: dict[str, Any] = {
        name: {"type": "string", "const": value} for name, value in consts.items()
    }
    properties.update({name: {"type": "string"} for name in fields})
    branch: dict[str, Any] = {"title": title, "type": "object", "properties": properties}
    if required is not None:
        branch["required"] = required
    return branch


def test_optional_discriminator_added_beside_the_existing_one_is_compatible() -> None:
    previous = _credentials(
        _branch("OAuth", "client_id", option_title="OAuth Credentials"),
        _branch("API key", "api_key", option_title="API Key Credentials"),
    )
    with_auth_type = [
        _branch("OAuth", "client_id", option_title="OAuth Credentials"),
        _branch("API key", "api_key", option_title="API Key Credentials"),
    ]
    with_auth_type[0]["properties"]["auth_type"] = {"const": "oauth2.0", "default": "oauth2.0"}
    with_auth_type[1]["properties"]["auth_type"] = {"const": "api_key", "default": "api_key"}

    comparison = compare_specs(previous, _credentials(*reversed(with_auth_type)))

    assert comparison.is_backward_compatible
    assert not any("was removed" in change for change in comparison.compatible)


def test_renamed_title_of_a_branch_without_a_discriminator_is_compatible() -> None:
    comparison = compare_specs(
        _credentials(_branch("Basic", "username", "password"), _branch("Token", "token")),
        _credentials(
            _branch("Username and password", "username", "password"), _branch("Token", "token")
        ),
    )

    assert comparison.is_backward_compatible
    assert comparison.compatible == [
        "`connectionSpecification.properties.credentials.oneOf[0].title` changed (annotation)"
    ]


def test_reordered_branches_without_a_discriminator_are_matched_by_their_fields() -> None:
    basic = _branch("Basic", "username", "password")
    token = _branch("Token", "token")
    renamed_token = _branch("Access token", "token")

    comparison = compare_specs(_credentials(basic, token), _credentials(renamed_token, basic))

    assert comparison.is_backward_compatible


def test_removed_branch_without_a_discriminator_is_breaking() -> None:
    basic = _branch("Basic", "username", "password")
    token = _branch("Token", "token")
    key = _branch("Key", "api_key")

    comparison = compare_specs(_credentials(basic, token, key), _credentials(key, basic))

    assert comparison.breaking == [
        "`connectionSpecification.properties.credentials.oneOf[1]` was removed"
    ]


def test_branches_sharing_a_discriminator_are_told_apart_by_title() -> None:
    app = _branch("OAuth app", "client_id", required=["client_id"], auth_type="oauth2.0")
    token = _branch("OAuth token", "access_token", required=["access_token"], auth_type="oauth2.0")
    key = _branch("API key", "api_key", required=["api_key"], auth_type="api_key")

    comparison = compare_specs(_credentials(app, token), _credentials(token, app, key))

    assert comparison.is_backward_compatible
    assert comparison.compatible == [
        "`connectionSpecification.properties.credentials.oneOf[2]` was added"
    ]


def test_renamed_discriminator_is_breaking_even_with_the_same_title() -> None:
    comparison = compare_specs(
        _credentials(_branch("OAuth", "client_id", required=["auth_type"], auth_type="oauth")),
        _credentials(_branch("OAuth", "client_id", required=["auth_type"], auth_type="oauth2.0")),
    )

    assert comparison.breaking == [
        "`connectionSpecification.properties.credentials.oneOf[0]` was removed"
    ]


@pytest.mark.parametrize(
    "current_branches, compatible",
    [
        pytest.param([{"type": "integer"}, {"type": "string"}], True, id="reordered"),
        pytest.param([{"type": "string"}], False, id="branch-removed"),
    ],
)
def test_anyof_type_branches(current_branches: list[Any], compatible: bool) -> None:
    comparison = compare_specs(
        _spec({"page_size": {"anyOf": [{"type": "string"}, {"type": "integer"}]}}),
        _spec({"page_size": {"anyOf": current_branches}}),
    )

    assert comparison.is_backward_compatible is compatible


# oneOf branches that overlap

_KEY_BRANCH = {
    "properties": {"auth_type": {"const": "key"}, "api_key": {"type": "string"}},
    "required": ["api_key"],
}
_TOKEN_BRANCH = {
    "properties": {"auth_type": {"const": "tok"}, "token": {"type": "string"}},
    "required": ["token"],
}


def _one_of(*branches: dict[str, Any], keyword: str = "oneOf") -> dict[str, Any]:
    return _spec({"credentials": {"type": "object", keyword: list(branches)}})


def test_added_oneof_branch_that_matches_existing_configs_is_breaking() -> None:
    # `{"api_key": "k"}` matched only the first branch; it matches the new one too, and a config
    # that matches two `oneOf` branches is rejected.
    loose = {"properties": {"auth_type": {"const": "none"}}}

    comparison = compare_specs(
        _one_of(_KEY_BRANCH, _TOKEN_BRANCH), _one_of(_KEY_BRANCH, _TOKEN_BRANCH, loose)
    )

    assert len(comparison.breaking) == 1
    assert comparison.breaking[0].startswith(
        "`connectionSpecification.properties.credentials.oneOf[2]` was added and may also match "
        "existing configs"
    )


@pytest.mark.parametrize(
    "added",
    [
        pytest.param(
            {"properties": {"refresh_token": {"type": "string"}}, "required": ["refresh_token"]},
            id="requires-a-field-no-other-branch-declares",
        ),
        pytest.param(
            {"properties": {"auth_type": {"const": "none"}}, "required": ["auth_type"]},
            id="requires-its-own-discriminator",
        ),
    ],
)
def test_added_oneof_branch_that_rejects_existing_configs_is_compatible(
    added: dict[str, Any],
) -> None:
    comparison = compare_specs(
        _one_of(_KEY_BRANCH, _TOKEN_BRANCH), _one_of(_KEY_BRANCH, _TOKEN_BRANCH, added)
    )

    assert comparison.is_backward_compatible
    assert comparison.compatible == [
        "`connectionSpecification.properties.credentials.oneOf[2]` was added"
    ]


def test_added_oneof_branch_pinned_apart_from_branches_that_overlap_without_it_is_compatible() -> (
    None
):
    # No branch requires `filetype`, but a config that leaves it out matches the `jsonl` branch
    # as well as its own, so the configs that were valid all set it, and the new branch rejects
    # them. This is the shape of the file-based `format` options.
    def file_format(filetype: str, **fields: Any) -> dict[str, Any]:
        return {
            "title": filetype,
            "type": "object",
            "properties": {"filetype": {"type": "string", "const": filetype}, **fields},
        }

    previous = [
        file_format("avro", double_as_string={"type": "boolean"}),
        file_format("csv", delimiter={"type": "string"}),
        file_format("jsonl"),
    ]

    comparison = compare_specs(_one_of(*previous), _one_of(*previous, file_format("unstructured")))

    assert comparison.is_backward_compatible


def test_added_anyof_branch_only_widens() -> None:
    loose = {"properties": {"auth_type": {"const": "none"}}}

    comparison = compare_specs(
        _one_of(_KEY_BRANCH, _TOKEN_BRANCH, keyword="anyOf"),
        _one_of(_KEY_BRANCH, _TOKEN_BRANCH, loose, keyword="anyOf"),
    )

    assert comparison.is_backward_compatible


def test_oneof_branch_widened_into_another_branch_is_breaking() -> None:
    # Without its discriminator and its required field, the first branch also matches every
    # config of the second one.
    widened_key = {"properties": {"auth_type": {}, "api_key": {"type": "string"}}}
    tokens = {
        "properties": {"auth_type": {"const": "tok"}, "token": {"type": "string"}},
        "required": ["auth_type", "token"],
    }
    keys = {**_KEY_BRANCH, "required": ["auth_type", "api_key"]}

    comparison = compare_specs(_one_of(keys, tokens), _one_of(widened_key, tokens))

    assert (
        "`connectionSpecification.properties.credentials.oneOf[0]` may now also match the configs "
        "of `connectionSpecification.properties.credentials.oneOf[1]`, which `oneOf` then rejects"
    ) in comparison.breaking


def test_removed_discriminator_of_a_oneof_branch_is_breaking() -> None:
    keys = {**_KEY_BRANCH, "required": ["auth_type"]}
    tokens = {**_TOKEN_BRANCH, "required": ["auth_type"]}
    keys_without_const = {"properties": {"auth_type": {}, "api_key": {"type": "string"}}}

    comparison = compare_specs(_one_of(keys, tokens), _one_of(keys_without_const, tokens))

    assert comparison.breaking == [
        "`connectionSpecification.properties.credentials.oneOf[0]` may now also match the configs "
        "of `connectionSpecification.properties.credentials.oneOf[1]`, which `oneOf` then rejects"
    ]


def test_oneof_branch_that_keeps_its_discriminator_is_compatible() -> None:
    keys = {**_KEY_BRANCH, "required": ["auth_type", "api_key"]}
    tokens = {**_TOKEN_BRANCH, "required": ["auth_type", "token"]}
    keys_with_optional_key = {**keys, "required": ["auth_type"]}

    comparison = compare_specs(_one_of(keys, tokens), _one_of(keys_with_optional_key, tokens))

    assert comparison.is_backward_compatible


# patternProperties


@pytest.mark.parametrize(
    "previous_schema",
    [
        pytest.param({}, id="keyword-added"),
        pytest.param({"patternProperties": {}}, id="pattern-added-to-an-empty-map"),
        pytest.param(
            {"patternProperties": {"^y_": {"type": "string"}}}, id="pattern-added-beside-another"
        ),
    ],
)
def test_added_pattern_property_is_breaking(previous_schema: dict[str, Any]) -> None:
    patterns = {**previous_schema.get("patternProperties", {}), "^x_": {"type": "integer"}}

    comparison = compare_specs(
        _spec({"options": {"type": "object", **previous_schema}}),
        _spec({"options": {"type": "object", "patternProperties": patterns}}),
    )

    assert not comparison.is_backward_compatible


def test_pattern_properties_added_at_its_default_is_compatible() -> None:
    comparison = compare_specs(
        _spec({"options": {"type": "object"}}),
        _spec({"options": {"type": "object", "patternProperties": {}}}),
    )

    assert comparison.is_backward_compatible


def test_config_field_named_pattern_properties_is_a_field() -> None:
    comparison = compare_specs(
        _spec({"options": {"type": "object", "properties": {}}}),
        _spec({"options": {"type": "object", "properties": {"patternProperties": {}}}}),
    )

    assert comparison.is_backward_compatible


# JSON equality and widenings the generic comparison cannot see


@pytest.mark.parametrize(
    "previous_schema, current_schema",
    [
        pytest.param({"enum": [1]}, {"enum": [True]}, id="enum-1-to-true"),
        pytest.param({"enum": [True]}, {"enum": [1]}, id="enum-true-to-1"),
        pytest.param({"const": 0}, {"const": False}, id="const-0-to-false"),
        pytest.param({"enum": [{"a": 1}]}, {"enum": [{"a": True}]}, id="nested-enum"),
    ],
)
def test_booleans_are_not_numbers(
    previous_schema: dict[str, Any], current_schema: dict[str, Any]
) -> None:
    comparison = compare_specs(_spec({"flag": previous_schema}), _spec({"flag": current_schema}))

    assert not comparison.is_backward_compatible


def test_integer_and_float_of_the_same_value_are_equal() -> None:
    assert (
        compare_specs(
            _spec({"page_size": {"type": "number", "maximum": 10}}),
            _spec({"page_size": {"type": "number", "maximum": 10.0}}),
        )
        == SpecComparison()
    )


@pytest.mark.parametrize(
    "keyword, current_value",
    [
        pytest.param("additionalProperties", {"type": "string"}, id="additionalProperties"),
        pytest.param("items", {"type": "string"}, id="items"),
    ],
)
def test_false_relaxed_to_a_schema_is_compatible(keyword: str, current_value: Any) -> None:
    comparison = compare_specs(
        _spec({"options": {"type": "object", keyword: False}}),
        _spec({"options": {"type": "object", keyword: current_value}}),
    )

    assert comparison.is_backward_compatible
    assert comparison.compatible == [
        f"`connectionSpecification.properties.options.{keyword}` relaxed from False to "
        "{'type': 'string'}"
    ]


@pytest.mark.parametrize(
    "previous_value, current_value, compatible",
    [
        pytest.param(4, 2, True, id="divisor-relaxes"),
        pytest.param(0.5, 0.25, True, id="float-divisor-relaxes"),
        pytest.param(2, 4, False, id="multiple-tightens"),
        pytest.param(4, 3, False, id="unrelated-tightens"),
    ],
)
def test_multiple_of_is_judged_by_divisibility(
    previous_value: float, current_value: float, compatible: bool
) -> None:
    comparison = compare_specs(
        _spec({"page_size": {"type": "integer", "multipleOf": previous_value}}),
        _spec({"page_size": {"type": "integer", "multipleOf": current_value}}),
    )

    assert comparison.is_backward_compatible is compatible


# declared_breaking_changes


_METADATA_WITH_BREAKING_CHANGES: dict[str, Any] = {
    "releases": {
        "breakingChanges": {
            "1.0.0": {"message": "first", "upgradeDeadline": "2026-01-01"},
            "2.0.0": {"message": "second", "upgradeDeadline": "2026-06-01"},
        }
    }
}


@pytest.mark.parametrize(
    "previous_version, current_version, expected",
    [
        pytest.param("1.9.0", "2.0.0", ["2.0.0"], id="major-under-test"),
        pytest.param("1.9.0", "2.0.1", ["2.0.0"], id="patch-on-top-of-major-still-rolling-out"),
        pytest.param("2.0.0", "2.0.1", [], id="major-already-published"),
        pytest.param("1.2.0", "1.2.1", [], id="no-major-in-range"),
        pytest.param("0.9.0", "2.0.0", ["1.0.0", "2.0.0"], id="several-majors-in-range"),
        pytest.param("2.0.0", "2.0.0", [], id="version-not-bumped"),
        pytest.param("1.9.0", "2.0.0-rc.1", ["2.0.0"], id="release-candidate-of-the-major"),
        pytest.param("1.9.0", "2.0.1-rc.1", ["2.0.0"], id="release-candidate-after-the-major"),
        pytest.param("1.9.0", "1.9.1-rc.1", [], id="release-candidate-before-the-major"),
        pytest.param("2.0.0", "2.0.1-rc.1", [], id="release-candidate-after-a-published-major"),
        pytest.param("not-a-version", "2.0.0", ["2.0.0"], id="unparsable-falls-back-to-exact"),
    ],
)
def test_declared_breaking_changes(
    previous_version: str, current_version: str, expected: list[str]
) -> None:
    assert (
        declared_breaking_changes(
            _METADATA_WITH_BREAKING_CHANGES, previous_version, current_version
        )
        == expected
    )


def test_declared_breaking_changes_without_releases() -> None:
    assert declared_breaking_changes({}, "1.0.0", "2.0.0") == []


@pytest.mark.parametrize(
    "version, than, expected",
    [
        pytest.param("1.2.4", "1.2.3", True, id="newer"),
        pytest.param("1.2.3", "1.2.3", False, id="same"),
        pytest.param("1.2.3", "1.10.0", False, id="older-by-number-not-by-string"),
        pytest.param("2.0.0", "2.0.0-rc.1", True, id="release-after-its-candidate"),
        pytest.param("dev", "1.2.3", False, id="unparsable"),
    ],
)
def test_is_newer_version(version: str, than: str, expected: bool) -> None:
    assert is_newer_version(version, than) is expected


# disabled_for_version


def test_disabled_for_version_reads_every_spec_entry() -> None:
    acceptance_test_config = {
        "acceptance_tests": {
            "spec": {
                "tests": [
                    {"spec_path": "manifest.yaml"},
                    {"backward_compatibility_tests_config": {"disable_for_version": "1.2.3"}},
                    {"backward_compatibility_tests_config": {"disable_for_version": 2.0}},
                ]
            },
            "discovery": {
                "tests": [{"backward_compatibility_tests_config": {"disable_for_version": "9.9.9"}}]
            },
        }
    }

    assert disabled_for_version(acceptance_test_config) == ["1.2.3", "2.0"]


@pytest.mark.parametrize(
    "acceptance_test_config",
    [
        pytest.param({}, id="empty"),
        pytest.param({"acceptance_tests": {}}, id="no-spec-section"),
        pytest.param({"acceptance_tests": {"spec": {"bypass_reason": "n/a"}}}, id="no-tests"),
        pytest.param({"acceptance_tests": {"spec": {"tests": [None]}}}, id="null-entry"),
    ],
)
def test_disabled_for_version_without_waivers(acceptance_test_config: dict[str, Any]) -> None:
    assert disabled_for_version(acceptance_test_config) == []


# fetch_published_spec

_REGISTRY_URL = "https://connectors.airbyte.com/files/metadata/airbyte/source-test/latest/oss.json"


def test_fetch_published_spec_returns_the_latest_entry() -> None:
    spec = _spec(_BASE_PROPERTIES)
    with requests_mock.Mocker() as mocker:
        mocker.get(_REGISTRY_URL, json={"dockerImageTag": "1.2.3", "spec": spec})

        published = fetch_published_spec("airbyte/source-test", "oss")

    assert published == PublishedSpec(version="1.2.3", spec=spec, url=_REGISTRY_URL)


def test_fetch_published_spec_returns_none_when_not_published() -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(_REGISTRY_URL, status_code=404)

        assert fetch_published_spec("airbyte/source-test", "oss") is None


def test_fetch_published_spec_raises_when_the_registry_errors() -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(_REGISTRY_URL, status_code=403)

        with pytest.raises(requests.HTTPError):
            fetch_published_spec("airbyte/source-test", "oss")


class _ScriptedRegistry:
    """A local HTTP server answering each request with the next scripted status code.

    `requests_mock` replaces the transport adapter, so it cannot exercise the retry policy that
    `fetch_published_spec` mounts; a real server can.
    """

    def __init__(self, statuses: list[int]) -> None:
        self.statuses = statuses
        self.requests = 0
        registry = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - the name http.server dispatches to
                status = registry.statuses[min(registry.requests, len(registry.statuses) - 1)]
                registry.requests += 1
                body = json.dumps({"dockerImageTag": "1.2.3", "spec": {}}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_: Any) -> None:
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url_template(self) -> str:
        port = self.server.server_address[1]
        return f"http://127.0.0.1:{port}/{{docker_repository}}/{{version}}/{{registry}}.json"

    def __enter__(self) -> _ScriptedRegistry:
        self.thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.mark.parametrize(
    "statuses, expected_requests",
    [
        pytest.param([503, 503, 200], 3, id="recovers-after-two-server-errors"),
        pytest.param([429, 200], 2, id="recovers-after-a-rate-limit"),
        pytest.param([200], 1, id="first-try"),
    ],
)
def test_fetch_published_spec_retries_transient_errors(
    monkeypatch: pytest.MonkeyPatch, statuses: list[int], expected_requests: int
) -> None:
    with _ScriptedRegistry(statuses) as registry:
        monkeypatch.setattr(
            _spec_compatibility, "REGISTRY_ENTRY_URL_TEMPLATE", registry.url_template
        )

        published = fetch_published_spec("airbyte/source-test", "oss")

    assert published is not None
    assert published.version == "1.2.3"
    assert registry.requests == expected_requests


@pytest.mark.parametrize(
    "statuses, error, expected_requests",
    [
        pytest.param([503], requests.exceptions.RetryError, 3, id="gives-up-after-two-retries"),
        pytest.param([403], requests.HTTPError, 1, id="client-error-is-not-retried"),
    ],
)
def test_fetch_published_spec_raises_when_retries_do_not_help(
    monkeypatch: pytest.MonkeyPatch,
    statuses: list[int],
    error: type[Exception],
    expected_requests: int,
) -> None:
    with _ScriptedRegistry(statuses) as registry:
        monkeypatch.setattr(
            _spec_compatibility, "REGISTRY_ENTRY_URL_TEMPLATE", registry.url_template
        )

        with pytest.raises(error):
            fetch_published_spec("airbyte/source-test", "oss")

    assert registry.requests == expected_requests


def test_fetch_published_spec_does_not_retry_a_missing_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _ScriptedRegistry([404]) as registry:
        monkeypatch.setattr(
            _spec_compatibility, "REGISTRY_ENTRY_URL_TEMPLATE", registry.url_template
        )

        assert fetch_published_spec("airbyte/source-test", "oss") is None

    assert registry.requests == 1


def test_unreachable_registry_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    with socket.socket() as closed:
        closed.bind(("127.0.0.1", 0))
        port = closed.getsockname()[1]
    monkeypatch.setattr(
        _spec_compatibility,
        "REGISTRY_ENTRY_URL_TEMPLATE",
        f"http://127.0.0.1:{port}/{{docker_repository}}/{{version}}/{{registry}}.json",
    )

    started = time.monotonic()
    with pytest.raises(requests.ConnectionError):
        fetch_published_spec("airbyte/source-test", "oss")

    assert time.monotonic() - started < 5


def test_registry_wait_is_bounded() -> None:
    retry = registry_retry()
    assert isinstance(retry.total, int)
    attempts = retry.total + 1
    backoff = sum(
        retry.backoff_factor * 2 ** (consecutive_errors - 1)
        for consecutive_errors in range(2, attempts)
    )

    assert retry.respect_retry_after_header is False
    assert set(retry.status_forcelist) >= {429, 500, 502, 503, 504}
    assert attempts * sum(REGISTRY_TIMEOUT_SECONDS) + backoff <= 60
    assert attempts * REGISTRY_TIMEOUT_SECONDS[0] + backoff <= 20


def test_failure_message_points_at_the_breaking_change_process() -> None:
    published = PublishedSpec(version="1.2.3", spec={}, url=_REGISTRY_URL)
    comparison = compare_specs(
        _spec(_BASE_PROPERTIES), _spec({"api_key": _BASE_PROPERTIES["api_key"]})
    )

    message = format_breaking_spec_changes(
        connector_name="source-test",
        current_version="1.2.4",
        registry="oss",
        published=published,
        comparison=comparison,
    )

    assert "The OSS spec of `source-test` 1.2.4" in message
    assert "published version 1.2.3" in message
    assert "  - `connectionSpecification.properties.start_date` was removed" in message
    assert BREAKING_CHANGES_DOCS_URL in message
    assert "`releases.breakingChanges`" in message
    assert 'disable_for_version: "1.2.3"' in message


def test_change_summary_lists_compatible_and_waived_changes() -> None:
    published = PublishedSpec(version="1.2.3", spec={}, url=_REGISTRY_URL)
    comparison = SpecComparison(breaking=["`a` was removed"], compatible=["`b` was added"])

    summary = format_spec_change_summary(
        registry="cloud", published=published, comparison=comparison, waiver="declared in 2.0.0"
    )

    assert summary.splitlines() == [
        "CLOUD spec compared with the published version 1.2.3: 1 breaking, 1 compatible change(s).",
        "Breaking changes, waived (declared in 2.0.0):",
        "  - `a` was removed",
        "Compatible changes:",
        "  - `b` was added",
    ]


def test_change_summary_is_capped() -> None:
    published = PublishedSpec(version="1.2.3", spec={}, url=_REGISTRY_URL)
    changes = [f"`field_{index}` was added" for index in range(MAX_REPORTED_CHANGES + 5)]

    summary = format_spec_change_summary(
        registry="oss", published=published, comparison=SpecComparison(compatible=changes)
    )

    lines = summary.splitlines()
    assert len(lines) == 2 + MAX_REPORTED_CHANGES + 1
    assert lines[-1] == "  … and 5 more"
    assert "Breaking" not in summary


# DockerConnectorTestSuite.test_docker_image_spec_backward_compatibility

_PREVIOUS_SPEC = _spec(_BASE_PROPERTIES)
_BREAKING_SPEC = _spec({"api_key": _BASE_PROPERTIES["api_key"]})


def _make_suite(
    tmp_path: Path,
    *,
    docker_image_tag: str = "1.2.4",
    breaking_changes: dict[str, Any] | None = None,
    acceptance_test_config: dict[str, Any] | None = None,
) -> type[DockerConnectorTestSuite]:
    connector_root = tmp_path / "source-test"
    connector_root.mkdir()
    data: dict[str, Any] = {
        "dockerRepository": "airbyte/source-test",
        "dockerImageTag": docker_image_tag,
        "tags": ["language:manifest-only"],
    }
    if breaking_changes:
        data["releases"] = {"breakingChanges": breaking_changes}
    (connector_root / "metadata.yaml").write_text(yaml.safe_dump({"data": data}))
    if acceptance_test_config is not None:
        (connector_root / "acceptance-test-config.yml").write_text(
            yaml.safe_dump(acceptance_test_config)
        )

    return type(
        "TestSuite",
        (DockerConnectorTestSuite,),
        {"get_connector_root_dir": classmethod(lambda cls: connector_root)},
    )


def _patch_specs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    published: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
) -> list[str]:
    """Serve `published` from the registry and `current` from the image; record modes run."""
    modes_run: list[str] = []

    def fake_fetch(docker_repository: str, registry: str) -> PublishedSpec | None:
        assert docker_repository == "airbyte/source-test"
        if registry not in published:
            return None
        return PublishedSpec(version="1.2.3", spec=published[registry], url=f"{registry}.json")

    def fake_run_spec(connector_image: str, deployment_mode: str) -> dict[str, Any]:
        assert connector_image == "airbyte/source-test:dev"
        modes_run.append(deployment_mode)
        return current[deployment_mode]

    monkeypatch.setattr(docker_base, "fetch_published_spec", fake_fetch)
    monkeypatch.setattr(docker_base, "_run_spec_in_image", fake_run_spec)
    return modes_run


def _run_test(suite: type[DockerConnectorTestSuite]) -> None:
    suite().test_docker_image_spec_backward_compatibility(
        connector_image_override="airbyte/source-test:dev",
        connector_base_image_override=None,
    )


def test_compatible_spec_passes_in_every_published_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    modes_run = _patch_specs(
        monkeypatch,
        published={"oss": _PREVIOUS_SPEC, "cloud": _PREVIOUS_SPEC},
        current={"oss": _PREVIOUS_SPEC, "cloud": _PREVIOUS_SPEC},
    )

    _run_test(_make_suite(tmp_path))

    assert modes_run == ["oss", "cloud"]


def test_breaking_spec_fails_with_the_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_specs(
        monkeypatch,
        published={"oss": _PREVIOUS_SPEC},
        current={"oss": _BREAKING_SPEC},
    )

    with pytest.raises(pytest.fail.Exception) as error:
        _run_test(_make_suite(tmp_path))

    assert "`connectionSpecification.properties.start_date` was removed" in str(error.value)
    assert BREAKING_CHANGES_DOCS_URL in str(error.value)


def test_breaking_change_only_in_cloud_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_specs(
        monkeypatch,
        published={"oss": _PREVIOUS_SPEC, "cloud": _PREVIOUS_SPEC},
        current={"oss": _PREVIOUS_SPEC, "cloud": _BREAKING_SPEC},
    )

    with pytest.raises(pytest.fail.Exception) as error:
        _run_test(_make_suite(tmp_path))

    assert "The CLOUD spec of `source-test` 1.2.4" in str(error.value)
    assert "The OSS spec" not in str(error.value)


def test_registry_without_an_entry_is_not_compared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    modes_run = _patch_specs(
        monkeypatch,
        published={"oss": _PREVIOUS_SPEC},
        current={"oss": _PREVIOUS_SPEC, "cloud": _BREAKING_SPEC},
    )

    _run_test(_make_suite(tmp_path))

    assert modes_run == ["oss"]


def test_unpublished_connector_is_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    modes_run = _patch_specs(monkeypatch, published={}, current={})

    with pytest.raises(pytest.skip.Exception, match="no published version"):
        _run_test(_make_suite(tmp_path))

    assert modes_run == []


def test_version_older_than_the_published_one_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    modes_run = _patch_specs(
        monkeypatch,
        published={"oss": _PREVIOUS_SPEC},
        current={"oss": _BREAKING_SPEC},
    )

    with pytest.raises(pytest.skip.Exception, match="older than the published version 1.2.3"):
        _run_test(_make_suite(tmp_path, docker_image_tag="1.2.2"))

    assert modes_run == []


def test_declared_breaking_change_waives_the_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": _BREAKING_SPEC})
    suite = _make_suite(
        tmp_path,
        docker_image_tag="2.0.0",
        breaking_changes={"2.0.0": {"message": "Removes `start_date`."}},
    )

    with pytest.raises(pytest.skip.Exception, match="declared as breaking in 2.0.0"):
        _run_test(suite)


def test_breaking_change_declared_for_an_older_version_does_not_waive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": _BREAKING_SPEC})
    suite = _make_suite(tmp_path, breaking_changes={"1.0.0": {"message": "Old change."}})

    with pytest.raises(pytest.fail.Exception):
        _run_test(suite)


def test_disable_for_version_matching_the_published_version_waives_the_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": _BREAKING_SPEC})
    suite = _make_suite(
        tmp_path,
        acceptance_test_config={
            "acceptance_tests": {
                "spec": {
                    "tests": [
                        {"backward_compatibility_tests_config": {"disable_for_version": "1.2.3"}}
                    ]
                }
            }
        },
    )

    with pytest.raises(pytest.skip.Exception, match="disable_for_version: 1.2.3"):
        _run_test(suite)


def test_stale_disable_for_version_does_not_waive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": _BREAKING_SPEC})
    suite = _make_suite(
        tmp_path,
        acceptance_test_config={
            "acceptance_tests": {
                "spec": {
                    "tests": [
                        {"backward_compatibility_tests_config": {"disable_for_version": "0.1.0"}}
                    ]
                }
            }
        },
    )

    with pytest.raises(pytest.fail.Exception):
        _run_test(suite)


def test_release_candidate_of_a_declared_major_is_waived(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": _BREAKING_SPEC})
    suite = _make_suite(
        tmp_path,
        docker_image_tag="2.0.0-rc.1",
        breaking_changes={"2.0.0": {"message": "Removes `start_date`."}},
    )

    with pytest.raises(pytest.skip.Exception, match="declared as breaking in 2.0.0"):
        _run_test(suite)


def test_compatible_changes_are_printed_on_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    extended = _spec({**_BASE_PROPERTIES, "page_size": {"type": "integer"}})
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": extended})

    _run_test(_make_suite(tmp_path))

    printed = capsys.readouterr().out
    assert "OSS spec compared with the published version 1.2.3" in printed
    assert "`connectionSpecification.properties.page_size` was added" in printed


def test_nothing_is_printed_for_an_unchanged_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": _PREVIOUS_SPEC})

    _run_test(_make_suite(tmp_path))

    assert capsys.readouterr().out == ""


def test_waived_changes_are_printed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_specs(monkeypatch, published={"oss": _PREVIOUS_SPEC}, current={"oss": _BREAKING_SPEC})
    suite = _make_suite(
        tmp_path,
        docker_image_tag="2.0.0",
        breaking_changes={"2.0.0": {"message": "Removes `start_date`."}},
    )

    with pytest.raises(pytest.skip.Exception):
        _run_test(suite)

    printed = capsys.readouterr().out
    assert "Breaking changes, waived (declared as breaking in 2.0.0):" in printed
    assert "`connectionSpecification.properties.start_date` was removed" in printed


def test_unreachable_registry_fails_with_how_to_deselect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unreachable(docker_repository: str, registry: str) -> PublishedSpec | None:
        raise requests.ConnectionError("connection refused")

    monkeypatch.setattr(docker_base, "fetch_published_spec", unreachable)

    with pytest.raises(pytest.fail.Exception) as error:
        _run_test(_make_suite(tmp_path))

    assert "connection refused" in str(error.value)
    assert "-k 'not test_docker_image_spec_backward_compatibility'" in str(error.value)


def test_run_spec_in_image_returns_the_raw_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    raw_spec = {"connectionSpecification": {"type": "object"}, "x_unknown_key": True}
    commands: list[list[str]] = []

    def fake_run_docker_command(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(cmd)
        stdout = "\n".join(
            [
                "not json",
                json.dumps({"type": "LOG", "log": {"level": "INFO", "message": "hello"}}),
                json.dumps({"type": "SPEC", "spec": raw_spec}),
            ]
        )
        return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(docker_base, "run_docker_command", fake_run_docker_command)

    assert docker_base._run_spec_in_image("image:tag", "cloud") == raw_spec
    assert commands == [
        [
            "docker",
            "run",
            "--rm",
            "-e",
            "DEPLOYMENT_MODE=cloud",
            "-e",
            "AIRBYTE_EDITION=CLOUD",
            "image:tag",
            "spec",
        ]
    ]


def test_run_spec_in_image_without_a_spec_message_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        docker_base,
        "run_docker_command",
        lambda cmd, **_: subprocess.CompletedProcess(cmd, 0, stdout="", stderr="boom"),
    )

    with pytest.raises(AssertionError, match="emitted no SPEC message"):
        docker_base._run_spec_in_image("image:tag", "oss")
