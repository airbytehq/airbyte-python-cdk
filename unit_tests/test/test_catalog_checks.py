# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the catalog checks run by the `discover` standard test."""

from __future__ import annotations

import copy
import json
import warnings
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

from airbyte_cdk.models import AirbyteCatalog, AirbyteStream, SyncMode
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests import (
    DeclarativeSourceTestSuite,
    DiscoveredCatalogWarning,
    SourceTestSuiteBase,
    source_base,
)
from airbyte_cdk.test.standard_tests._catalog_checks import (
    CATALOG_CHECKS,
    CatalogCheck,
    check_additional_properties_not_false,
    check_catalog_has_streams,
    check_cursor_fields_exist_in_schema,
    check_no_unsupported_keywords,
    check_primary_keys_exist_in_schema,
    check_primary_keys_have_scalar_types,
    check_refs_are_resolved,
    check_schemas_are_valid_json_schema,
    check_stream_names_are_unique,
    check_streams_declare_sync_modes,
    check_supported_data_types,
    find_catalog_problems,
)

_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "updated_at": {"type": "string", "format": "date-time"},
        "author": {
            "type": ["null", "object"],
            "properties": {"login": {"type": "string"}},
        },
        "tags": {"type": "array", "items": {"type": "string"}},
    },
}


def _stream(
    name: str = "items",
    json_schema: dict[str, Any] | None = None,
    **kwargs: Any,
) -> AirbyteStream:
    return AirbyteStream(
        name=name,
        json_schema=_SCHEMA if json_schema is None else json_schema,
        supported_sync_modes=kwargs.pop("supported_sync_modes", [SyncMode.full_refresh]),
        **kwargs,
    )


def _catalog(*streams: AirbyteStream) -> AirbyteCatalog:
    return AirbyteCatalog(streams=list(streams))


def _with_property(name: str, property_schema: dict[str, Any]) -> dict[str, Any]:
    return {**_SCHEMA, "properties": {**_SCHEMA["properties"], name: property_schema}}


Check = Callable[[AirbyteCatalog], list[str]]


@pytest.mark.parametrize(
    "check, catalog",
    [
        pytest.param(check_catalog_has_streams, _catalog(_stream()), id="has_streams"),
        pytest.param(
            check_stream_names_are_unique,
            _catalog(_stream("a"), _stream("b"), _stream("a", namespace="other")),
            id="unique_names_namespace_disambiguates",
        ),
        pytest.param(check_streams_declare_sync_modes, _catalog(_stream()), id="sync_modes"),
        pytest.param(
            check_schemas_are_valid_json_schema,
            _catalog(_stream(json_schema=_with_property("v", {"type": ["null", "string"]}))),
            id="valid_json_schema",
        ),
        pytest.param(
            check_cursor_fields_exist_in_schema,
            _catalog(
                _stream(default_cursor_field=["updated_at"]),
                _stream("nested", default_cursor_field=["author", "login"]),
                _stream("no_cursor"),
            ),
            id="cursor_exists_top_level_and_nested",
        ),
        pytest.param(
            check_primary_keys_exist_in_schema,
            _catalog(_stream(source_defined_primary_key=[["id"], ["author", "login"]])),
            id="primary_keys_exist",
        ),
        pytest.param(
            check_primary_keys_have_scalar_types,
            _catalog(
                _stream(source_defined_primary_key=[["id"]]),
                _stream(
                    "untyped",
                    json_schema=_with_property("id", {}),
                    source_defined_primary_key=[["id"]],
                ),
                _stream("missing", source_defined_primary_key=[["not_in_schema"]]),
            ),
            id="primary_keys_scalar_untyped_or_missing_is_skipped",
        ),
        pytest.param(
            check_refs_are_resolved,
            _catalog(
                _stream(
                    json_schema=_with_property(
                        "$ref", {"type": "string", "examples": [{"$ref": "data"}]}
                    )
                )
            ),
            id="property_named_ref_and_ref_in_examples_are_not_refs",
        ),
        pytest.param(
            check_no_unsupported_keywords,
            _catalog(
                _stream(
                    json_schema={
                        "type": "object",
                        "properties": {
                            "not": {"type": "boolean"},
                            "allOf": {"type": "string", "default": {"not": "data"}},
                            "choice": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                        },
                    }
                )
            ),
            id="properties_named_like_keywords_are_allowed",
        ),
        pytest.param(
            check_additional_properties_not_false,
            _catalog(
                _stream(json_schema={**_SCHEMA, "additionalProperties": True}),
                _stream("open_object", json_schema={**_SCHEMA, "additionalProperties": {}}),
            ),
            id="additional_properties_true_or_schema",
        ),
        pytest.param(
            check_supported_data_types,
            _catalog(
                _stream(
                    json_schema={
                        "type": ["null", "object"],
                        "properties": {
                            "created_at": {
                                "type": ["null", "string"],
                                "format": "date-time",
                                "airbyte_type": "timestamp_without_timezone",
                            },
                            "opens_at": {
                                "type": "string",
                                "format": "time",
                                "airbyte_type": "time_with_timezone",
                            },
                            "day": {"type": "string", "format": "date"},
                            "count": {"type": "number", "airbyte_type": "integer"},
                            "email": {"type": "string", "format": "email"},
                            "size": {"type": "integer", "format": "int64"},
                        },
                    }
                )
            ),
            id="supported_types_formats_and_airbyte_types",
        ),
    ],
)
def test_check_passes(check: Check, catalog: AirbyteCatalog) -> None:
    assert check(catalog) == []


@pytest.mark.parametrize(
    "check, catalog, expected_problems",
    [
        pytest.param(
            check_catalog_has_streams,
            _catalog(),
            ["The catalog does not contain any streams."],
            id="empty_catalog",
        ),
        pytest.param(
            check_stream_names_are_unique,
            _catalog(
                _stream("a"), _stream("a"), _stream("b", namespace="n"), _stream("b", namespace="n")
            ),
            [
                "Stream 'a' is declared 2 times.",
                "Stream 'b' in namespace 'n' is declared 2 times.",
            ],
            id="duplicate_names",
        ),
        pytest.param(
            check_streams_declare_sync_modes,
            _catalog(_stream(supported_sync_modes=[])),
            ["Stream 'items' does not declare any supported sync modes."],
            id="no_sync_modes",
        ),
        pytest.param(
            check_schemas_are_valid_json_schema,
            _catalog(_stream(json_schema=_with_property("v", {"type": "any"}))),
            [
                "Stream 'items' has an invalid JSON schema at #/properties/v/type: "
                "'any' is not valid under any of the given schemas"
            ],
            id="unknown_type_name",
        ),
        pytest.param(
            check_schemas_are_valid_json_schema,
            _catalog(
                _stream(
                    json_schema={
                        "type": "object",
                        "required": ["id", "id"],
                        "properties": {"zip": None, "address": None},
                    }
                )
            ),
            [
                "Stream 'items' has an invalid JSON schema at #/properties/address: "
                "None is not of type 'object', 'boolean'",
                "Stream 'items' has an invalid JSON schema at #/properties/zip: "
                "None is not of type 'object', 'boolean'",
                "Stream 'items' has an invalid JSON schema at #/required: "
                "['id', 'id'] has non-unique elements",
            ],
            id="every_violation_reported_in_path_order",
        ),
        pytest.param(
            check_cursor_fields_exist_in_schema,
            _catalog(
                _stream(default_cursor_field=["modified_at"]),
                _stream("nested", default_cursor_field=["author", "updated_at"]),
                _stream("open", json_schema={"type": "object"}, default_cursor_field=["id"]),
            ),
            [
                "Stream 'items' declares cursor field 'modified_at', "
                "which is not a property in the stream schema.",
                "Stream 'nested' declares cursor field 'author.updated_at', "
                "which is not a property in the stream schema.",
                "Stream 'open' declares cursor field 'id', "
                "which is not a property in the stream schema.",
            ],
            id="cursor_missing",
        ),
        pytest.param(
            check_primary_keys_exist_in_schema,
            _catalog(_stream(source_defined_primary_key=[["id"], ["uuid"]])),
            [
                "Stream 'items' declares primary key field 'uuid', "
                "which is not a property in the stream schema."
            ],
            id="primary_key_missing",
        ),
        pytest.param(
            check_primary_keys_have_scalar_types,
            _catalog(
                _stream(source_defined_primary_key=[["author"]]),
                _stream(
                    "array_typed",
                    json_schema=_with_property("id", {"type": "array"}),
                    source_defined_primary_key=[["id"]],
                ),
            ),
            [
                "Stream 'items' declares primary key field 'author' with type "
                "['null', 'object']. Primary key fields must not be objects or arrays.",
                "Stream 'array_typed' declares primary key field 'id' with type "
                "['array']. Primary key fields must not be objects or arrays.",
            ],
            id="primary_key_object_or_array",
        ),
        pytest.param(
            check_refs_are_resolved,
            _catalog(
                _stream(
                    json_schema=_with_property(
                        "owner", {"type": "array", "items": {"$ref": "#/definitions/user"}}
                    )
                )
            ),
            [
                "Stream 'items' has an unresolved $ref '#/definitions/user' "
                "at #/properties/owner/items."
            ],
            id="unresolved_ref",
        ),
        pytest.param(
            check_no_unsupported_keywords,
            _catalog(
                _stream(
                    json_schema=_with_property(
                        "v",
                        {
                            "allOf": [{"type": "string"}],
                            "items": [{"not": {"type": "null"}}],
                        },
                    )
                )
            ),
            [
                "Stream 'items' uses the unsupported keyword 'allOf' at #/properties/v.",
                "Stream 'items' uses the unsupported keyword 'not' at #/properties/v/items/0.",
            ],
            id="all_of_and_not",
        ),
        pytest.param(
            check_additional_properties_not_false,
            _catalog(
                _stream(
                    json_schema=_with_property(
                        "a/b", {"type": "object", "additionalProperties": False}
                    )
                )
            ),
            ["Stream 'items' sets additionalProperties to false at #/properties/a~1b."],
            id="nested_additional_properties_false",
        ),
        pytest.param(
            check_supported_data_types,
            _catalog(
                _stream(
                    json_schema={
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "big": {"type": "string", "airbyte_type": "big_integer"},
                                "ts": {
                                    "type": "integer",
                                    "airbyte_type": "timestamp_with_timezone",
                                },
                                "day": {"type": ["null", "integer"], "format": "date"},
                            },
                        },
                    }
                )
            ),
            [
                "Stream 'items' has a top-level schema of type ['array']; it must be an object.",
                "Stream 'items' uses the unknown airbyte_type 'big_integer' "
                "at #/items/properties/big.",
                "Stream 'items' uses airbyte_type 'timestamp_with_timezone' on type ['integer'] "
                "at #/items/properties/ts; it requires one of ['string'].",
                "Stream 'items' uses format 'date' on type ['integer', 'null'] "
                "at #/items/properties/day; it requires a string.",
            ],
            id="unsupported_types",
        ),
    ],
)
def test_check_fails(check: Check, catalog: AirbyteCatalog, expected_problems: list[str]) -> None:
    assert check(catalog) == expected_problems


def test_primary_key_typed_as_plain_string_object_is_caught() -> None:
    """CAT built `set("object")`, so a PK typed `"object"` (not a list) slipped through."""
    catalog = _catalog(
        _stream(
            json_schema=_with_property("id", {"type": "object"}),
            source_defined_primary_key=[["id"]],
        )
    )
    assert check_primary_keys_have_scalar_types(catalog) != []


def test_field_with_invalid_subschema_still_exists() -> None:
    """A `null` property schema is reported as invalid JSON schema, not as a missing field."""
    catalog = _catalog(
        _stream(json_schema=_with_property("ts", None), default_cursor_field=["ts"])  # type: ignore[arg-type]
    )
    assert check_cursor_fields_exist_in_schema(catalog) == []
    assert check_schemas_are_valid_json_schema(catalog) != []


def test_supported_data_types_tolerates_non_string_annotations() -> None:
    catalog = _catalog(
        _stream(
            json_schema=_with_property(
                "v", {"type": "integer", "format": ["date"], "airbyte_type": {"x": 1}}
            )
        )
    )
    assert check_supported_data_types(catalog) == [
        "Stream 'items' uses the unknown airbyte_type '{'x': 1}' at #/properties/v."
    ]


def test_find_catalog_problems_returns_only_failing_checks() -> None:
    catalog = _catalog(_stream(default_cursor_field=["missing"]))
    problems = find_catalog_problems(catalog)
    assert [check.run for check in problems] == [check_cursor_fields_exist_in_schema]


def test_find_catalog_problems_reports_a_crashing_check() -> None:
    def crash(catalog: AirbyteCatalog) -> list[str]:
        raise KeyError("boom")

    check = CatalogCheck("crashes", crash, enforced=False)
    problems = find_catalog_problems(_catalog(_stream()), checks=[check])
    assert problems == {check: ["The check could not run: KeyError: 'boom'"]}


def test_every_check_has_a_unique_name() -> None:
    names = [check.name for check in CATALOG_CHECKS]
    assert len(names) == len(set(names))


def _catalog_message(*streams: AirbyteStream) -> str:
    return json.dumps(
        {
            "type": "CATALOG",
            "catalog": {
                "streams": [
                    {
                        "name": stream.name,
                        "json_schema": stream.json_schema,
                        "supported_sync_modes": [
                            mode.value for mode in stream.supported_sync_modes
                        ],
                        "default_cursor_field": stream.default_cursor_field,
                    }
                    for stream in streams
                ]
            },
        }
    )


class _FakeSourceSuite(SourceTestSuiteBase):
    @classmethod
    def create_connector(cls, scenario: ConnectorTestScenario | None) -> Any:
        return object()

    @classmethod
    def get_connector_root_dir(cls) -> Path:
        return Path()


@pytest.fixture
def discover_output(monkeypatch: pytest.MonkeyPatch) -> Callable[[EntrypointOutput], None]:
    def _set(output: EntrypointOutput) -> None:
        monkeypatch.setattr(source_base, "run_test_job", lambda *args, **kwargs: output)

    return _set


def test_discover_fails_on_enforced_problem(
    discover_output: Callable[[EntrypointOutput], None],
) -> None:
    discover_output(EntrypointOutput(messages=[_catalog_message(_stream("a"), _stream("a"))]))
    with pytest.raises(AssertionError, match=r"\[stream names are unique\]\n  - Stream 'a'"):
        _FakeSourceSuite().test_discover(ConnectorTestScenario(status="succeed"))


def test_discover_warns_on_advisory_problem(
    discover_output: Callable[[EntrypointOutput], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    advisory = CatalogCheck("cursor check", check_cursor_fields_exist_in_schema, enforced=False)
    monkeypatch.setattr(
        source_base,
        "find_catalog_problems",
        lambda catalog: find_catalog_problems(catalog, checks=[advisory]),
    )
    discover_output(
        EntrypointOutput(messages=[_catalog_message(_stream(default_cursor_field=["x"]))])
    )
    with pytest.warns(DiscoveredCatalogWarning, match=r"\[cursor check\]"):
        _FakeSourceSuite().test_discover(ConnectorTestScenario(status="succeed"))


def test_discover_fails_without_catalog(
    discover_output: Callable[[EntrypointOutput], None],
) -> None:
    log = json.dumps({"type": "LOG", "log": {"level": "INFO", "message": "no catalog"}})
    discover_output(EntrypointOutput(messages=[log]))
    with pytest.raises(AssertionError, match="Expected exactly one CATALOG message. Got 0."):
        _FakeSourceSuite().test_discover(ConnectorTestScenario(status="succeed"))


def test_discover_skips_checks_when_scenario_allows_failure(
    discover_output: Callable[[EntrypointOutput], None],
) -> None:
    discover_output(EntrypointOutput(messages=[], uncaught_exception=ValueError("boom")))
    _FakeSourceSuite().test_discover(ConnectorTestScenario())


_MANIFEST: dict[str, Any] = {
    "version": "6.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["items"]},
    "spec": {
        "type": "Spec",
        "connection_specification": {
            "type": "object",
            "properties": {"api_key": {"type": "string"}},
        },
    },
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "items",
            "primary_key": ["id"],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.invalid",
                    "path": "/items",
                    "http_method": "GET",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
            },
            "incremental_sync": {
                "type": "DatetimeBasedCursor",
                "cursor_field": "updated_at",
                "datetime_format": "%Y-%m-%d",
                "start_datetime": "2024-01-01",
            },
            "schema_loader": {"type": "InlineSchemaLoader", "schema": _SCHEMA},
        }
    ],
}


def _declarative_suite(tmp_path: Path, manifest: dict[str, Any]) -> DeclarativeSourceTestSuite:
    (tmp_path / "manifest.yaml").write_text(yaml.safe_dump(manifest))

    class _Suite(DeclarativeSourceTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    return _Suite()


_SCENARIO = ConnectorTestScenario(config_dict={"api_key": "unused"}, status="succeed")


def test_declarative_discover_passes_for_valid_catalog(tmp_path: Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", DiscoveredCatalogWarning)
        _declarative_suite(tmp_path, _MANIFEST).test_discover(_SCENARIO)


def test_declarative_discover_warns_on_missing_cursor(tmp_path: Path) -> None:
    manifest = copy.deepcopy(_MANIFEST)
    manifest["streams"][0]["incremental_sync"]["cursor_field"] = "modified_at"
    with pytest.warns(DiscoveredCatalogWarning, match="declares cursor field 'modified_at'"):
        _declarative_suite(tmp_path, manifest).test_discover(_SCENARIO)


def test_declarative_discover_fails_on_closed_schema(tmp_path: Path) -> None:
    manifest = copy.deepcopy(_MANIFEST)
    manifest["streams"][0]["schema_loader"]["schema"] = {**_SCHEMA, "additionalProperties": False}
    with pytest.raises(AssertionError, match="sets additionalProperties to false at #."):
        _declarative_suite(tmp_path, manifest).test_discover(_SCENARIO)
