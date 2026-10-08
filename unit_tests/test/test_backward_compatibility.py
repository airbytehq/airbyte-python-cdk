# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the spec backward-compatibility check."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
import requests
import requests_mock
import yaml

from airbyte_cdk.test.standard_tests import docker_base
from airbyte_cdk.test.standard_tests.backward_compatibility import (
    BREAKING_CHANGES_DOCS_URL,
    PublishedSpec,
    compare_specs,
    declared_breaking_changes,
    disabled_for_version,
    fetch_published_spec,
    format_breaking_spec_changes,
    is_newer_version,
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
    comparison = compare_specs(
        _spec({"credentials": {"oneOf": [{"properties": {"auth_type": {"const": "api_key"}}}]}}),
        _spec({"credentials": {"oneOf": [{"properties": {"auth_type": {"const": "oauth2.0"}}}]}}),
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
        pytest.param("1.9.0", "2.0.0-rc.1", [], id="release-candidate-before-major"),
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
