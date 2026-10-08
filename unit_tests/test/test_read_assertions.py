# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the record assertions of the `read` Standard Tests.

The assertions are exercised directly and through `SourceTestSuiteBase.test_basic_read` with a
fake source:

- every configured stream of a `basic_read` config returns at least one record, except the
  streams declared in `empty_streams` (which are not read);
- every record matches its stream's JSON schema, unless the config sets `validate_schema: false`.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest
import yaml

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationSyncMode,
    Status,
    SyncMode,
    Type,
)
from airbyte_cdk.sources import Source
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests import SourceTestSuiteBase
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests._read_assertions import (
    MAX_SCHEMA_ERRORS_PER_STREAM,
    assert_read_records,
    format_record_path,
    get_expected_schema_paths,
)
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG

MSG_MISSING_STREAMS = "Every configured stream must return at least one record"
MSG_SCHEMA = "Records do not match their stream's JSON schema"
MSG_NO_RECORDS = "Expected records but got none"

USERS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": ["null", "string"]},
        "updated_at": {"type": ["null", "string"], "format": "date-time"},
    },
}
EVENTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"id": {"type": "string"}},
}


def _catalog(**schemas: Mapping[str, Any]) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=name,
                    json_schema=dict(schema),
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            )
            for name, schema in schemas.items()
        ]
    )


def _record(stream: str, data: Mapping[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(stream=stream, data=dict(data), emitted_at=0),
    )


def _assert(
    records: list[AirbyteMessage],
    catalog: ConfiguredAirbyteCatalog,
    *,
    require_records_per_stream: bool = True,
    validate_schema: bool = True,
) -> None:
    assert_read_records(
        records=records,
        configured_catalog=catalog,
        require_records_per_stream=require_records_per_stream,
        validate_schema=validate_schema,
    )


def _schema_error(
    schema: Mapping[str, Any], *records: Mapping[str, Any], validate_schema: bool = True
) -> str | None:
    """Validate `records` of a single stream; return the failure message, or None if they pass."""
    try:
        _assert(
            [_record("s", data) for data in records],
            _catalog(s=schema),
            validate_schema=validate_schema,
        )
    except AssertionError as error:
        return str(error)
    return None


# --- Per-stream record presence ------------------------------------------------------------


def test_every_stream_with_records_passes() -> None:
    _assert(
        [_record("users", {"id": 1}), _record("events", {"id": "a"})],
        _catalog(users=USERS_SCHEMA, events=EVENTS_SCHEMA),
    )


def test_stream_without_records_fails_and_is_named() -> None:
    with pytest.raises(AssertionError, match=MSG_MISSING_STREAMS) as error:
        _assert([_record("users", {"id": 1})], _catalog(users=USERS_SCHEMA, events=EVENTS_SCHEMA))
    assert "'events'" in str(error.value)
    assert "'users'" not in str(error.value)
    assert "empty_streams" in str(error.value)


def test_streams_without_records_are_allowed_when_not_required() -> None:
    _assert(
        [_record("users", {"id": 1})],
        _catalog(users=USERS_SCHEMA, events=EVENTS_SCHEMA),
        require_records_per_stream=False,
    )


def test_records_of_unconfigured_streams_do_not_count() -> None:
    with pytest.raises(AssertionError, match=MSG_MISSING_STREAMS):
        _assert(
            [_record("users", {"id": 1}), _record("other", {"anything": True})],
            _catalog(users=USERS_SCHEMA, events=EVENTS_SCHEMA),
        )


# --- Schema validation -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "schema, data",
    [
        pytest.param(USERS_SCHEMA, {"id": 1, "name": None}, id="nullable_null"),
        pytest.param(USERS_SCHEMA, {"id": 1, "extra": {"x": 1}}, id="extra_column_is_allowed"),
        pytest.param(USERS_SCHEMA, {"id": 1, "updated_at": "2024-01-02T03:04:05Z"}, id="rfc3339"),
        pytest.param(
            USERS_SCHEMA, {"id": 1, "updated_at": "2024-01-02 03:04:05"}, id="space_no_offset"
        ),
        pytest.param(
            USERS_SCHEMA,
            {"id": 1, "updated_at": "2024-01-02T03:04:05.123456+05:30"},
            id="fraction_offset",
        ),
        pytest.param(USERS_SCHEMA, {"id": 1, "updated_at": None}, id="null_date_time"),
        pytest.param(
            {"type": "object", "properties": {"d": {"type": "string", "format": "date"}}},
            {"d": "2024-01-02"},
            id="date",
        ),
        pytest.param({"type": "object"}, {"anything": [1, 2]}, id="schema_without_properties"),
        pytest.param({}, {"anything": 1}, id="empty_schema"),
    ],
)
def test_valid_records_pass(schema: Mapping[str, Any], data: Mapping[str, Any]) -> None:
    assert _schema_error(schema, data) is None


@pytest.mark.parametrize(
    "schema, data, expected_path, expected_message",
    [
        pytest.param(
            USERS_SCHEMA, {"id": "1"}, "$.id", "expected type 'integer', got string", id="type"
        ),
        pytest.param(
            USERS_SCHEMA,
            {"id": 1.0},
            "$.id",
            "expected type 'integer', got number",
            id="float_integer",
        ),
        pytest.param(
            USERS_SCHEMA,
            {"id": True},
            "$.id",
            "expected type 'integer', got boolean",
            id="bool_integer",
        ),
        pytest.param(
            USERS_SCHEMA,
            {"id": 1, "updated_at": "2024-01-02"},
            "$.updated_at",
            "is not a valid 'date-time'",
            id="date_time_without_time",
        ),
        pytest.param(
            USERS_SCHEMA,
            {"id": 1, "updated_at": "2024-13-45T00:00:00Z"},
            "$.updated_at",
            "is not a valid 'date-time'",
            id="date_time_unparseable",
        ),
        pytest.param(
            {"type": "object", "properties": {"d": {"type": "string", "format": "date"}}},
            {"d": "2024-01-02T00:00:00Z"},
            "$.d",
            "is not a 'date'",
            id="date_with_time",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {"type": "object", "properties": {"price": {"type": "number"}}},
                    }
                },
            },
            {"items": [{"price": 1}, {"price": "free"}]},
            "$.items[1].price",
            "expected type 'number', got string",
            id="nested_array",
        ),
        pytest.param(
            {"type": "object", "properties": {"first name": {"type": "string"}}},
            {"first name": 1},
            "$['first name']",
            "expected type 'string', got integer",
            id="non_identifier_key",
        ),
    ],
)
def test_invalid_records_fail_with_stream_and_record_path(
    schema: Mapping[str, Any],
    data: Mapping[str, Any],
    expected_path: str,
    expected_message: str,
) -> None:
    message = _schema_error(schema, {**data}, {**data})
    assert message is not None
    assert MSG_SCHEMA in message
    assert "Stream 's': 2 of 2 records do not match" in message
    assert f"record #0 at `{expected_path}`" in message
    assert expected_message in message
    assert "validate_schema: false" in message


def test_type_errors_name_the_type_instead_of_printing_the_value() -> None:
    schema = {"type": "object", "properties": {"reason": {"type": ["null", "string"]}}}
    message = _schema_error(schema, {"reason": {"text": "private value"}})
    assert message is not None
    assert "record #0 at `$.reason`: expected type 'null' or 'string', got object" in message
    assert "private value" not in message


def test_validate_schema_false_skips_schema_checks() -> None:
    assert _schema_error(USERS_SCHEMA, {"id": "not an integer"}, validate_schema=False) is None


def test_record_sharing_no_field_with_schema_fails() -> None:
    # The schema accepts any object (no `required`, additional properties allowed), so only the
    # structure check catches a record that has none of the declared fields.
    message = _schema_error(USERS_SCHEMA, {"id": 1}, {"unexpected": 1})
    assert message is not None
    assert "Stream 's': 1 of 2 records share no field with the stream's JSON schema" in message
    assert "record #1 has top-level fields ['unexpected']" in message


def test_distinct_schema_errors_are_capped_per_stream() -> None:
    properties = {f"f{index}": {"type": "integer"} for index in range(8)}
    message = _schema_error(
        {"type": "object", "properties": properties}, {name: "x" for name in properties}
    )
    assert message is not None
    assert message.count("record #0 at") == MAX_SCHEMA_ERRORS_PER_STREAM
    assert f"...and {8 - MAX_SCHEMA_ERRORS_PER_STREAM} more distinct schema error(s)." in message


def test_each_schema_rule_is_reported_once_with_its_first_record() -> None:
    message = _schema_error(USERS_SCHEMA, {"id": 1}, {"id": "a"}, {"id": "b"})
    assert message is not None
    assert "2 of 3 records do not match" in message
    assert "record #1 at `$.id`" in message
    assert "record #2" not in message


def test_presence_and_schema_failures_are_reported_together() -> None:
    with pytest.raises(AssertionError) as error:
        _assert([_record("users", {"id": "1"})], _catalog(users=USERS_SCHEMA, events=EVENTS_SCHEMA))
    assert MSG_MISSING_STREAMS in str(error.value)
    assert MSG_SCHEMA in str(error.value)


def test_recursive_schema_is_supported() -> None:
    schema = {
        "type": "object",
        "definitions": {
            "node": {
                "type": "object",
                "properties": {
                    "value": {"type": "integer"},
                    "children": {"type": "array", "items": {"$ref": "#/definitions/node"}},
                },
            }
        },
        "properties": {"root": {"$ref": "#/definitions/node"}},
    }
    assert _schema_error(schema, {"root": {"value": 1, "children": [{"value": 2}]}}) is None
    message = _schema_error(schema, {"root": {"value": 1, "children": [{"value": "2"}]}})
    assert message is not None
    assert "`$.root.children[0].value`" in message


# --- Helpers ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path, expected",
    [
        pytest.param([], "$", id="root"),
        pytest.param(["a", "b"], "$.a.b", id="keys"),
        pytest.param(["a", 0, "b"], "$.a[0].b", id="index"),
        pytest.param(["first name", "x-y"], "$['first name']['x-y']", id="quoted"),
    ],
)
def test_format_record_path(path: list[Any], expected: str) -> None:
    assert format_record_path(path) == expected


def test_get_expected_schema_paths() -> None:
    schema = {
        "$ref": "#/definitions/root",
        "definitions": {
            "root": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "meta": {"type": ["null", "object"]},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "owner": {
                        "anyOf": [
                            {"type": "null"},
                            {"properties": {"login": {"type": "string"}}},
                        ]
                    },
                },
            }
        },
    }
    assert get_expected_schema_paths(schema) == {
        "/id",
        "/meta",
        "/tags/[]",
        "/owner",
        "/owner/login",
    }


# --- Through `SourceTestSuiteBase.test_basic_read` --------------------------------------------


class _FakeReadSource(Source):
    """A source that discovers the given streams and reads the given records for each one."""

    def __init__(
        self,
        schemas: Mapping[str, Mapping[str, Any]],
        records: Mapping[str, list[Mapping[str, Any]]],
    ) -> None:
        self._schemas = schemas
        self._records = records

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(connectionSpecification={"type": "object", "properties": {}})

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(
            streams=[
                AirbyteStream(
                    name=name,
                    json_schema=dict(schema),
                    supported_sync_modes=[SyncMode.full_refresh],
                )
                for name, schema in self._schemas.items()
            ]
        )

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        for configured_stream in catalog.streams:
            for data in self._records.get(configured_stream.stream.name, []):
                yield _record(configured_stream.stream.name, data)


def _run_basic_read(
    tmp_path: Path,
    scenario: ConnectorTestScenario,
    records: Mapping[str, list[Mapping[str, Any]]],
) -> None:
    source = _FakeReadSource({"users": USERS_SCHEMA, "events": EVENTS_SCHEMA}, records)

    class _Suite(SourceTestSuiteBase):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

        @classmethod
        def create_connector(cls, scenario: ConnectorTestScenario | None) -> IConnector:
            return source

    _Suite().test_basic_read(scenario)


def _scenario(
    *sections: str, status: str | None = "succeed", **fields: Any
) -> ConnectorTestScenario:
    return ConnectorTestScenario.model_validate(
        {"config_dict": {"api_key": "x"}, "status": status, "sections": sections, **fields}
    )


VALID_USERS = [{"id": 1, "name": "a"}]
VALID_EVENTS = [{"id": "e1"}]

# (scenario, records by stream, expected failure message or None)
BASIC_READ_MATRIX = [
    pytest.param(
        _scenario("connection", "basic_read"),
        {"users": VALID_USERS, "events": VALID_EVENTS},
        None,
        id="basic_read_all_streams",
    ),
    pytest.param(
        _scenario("connection", "basic_read"),
        {"users": VALID_USERS},
        MSG_MISSING_STREAMS,
        id="basic_read_empty_stream",
    ),
    pytest.param(
        _scenario("connection", "basic_read", empty_streams=[{"name": "events"}]),
        {"users": VALID_USERS, "events": [{"id": 1}]},
        None,
        id="basic_read_declared_empty_stream_is_not_read",
    ),
    pytest.param(
        _scenario(),
        {"users": VALID_USERS},
        MSG_MISSING_STREAMS,
        id="hand_built_scenario_is_a_basic_read_config",
    ),
    # A config read only because it is listed under `full_refresh` has no `basic_read` entry to
    # declare its empty streams or its `validate_schema` opt-out in, so only the "some records"
    # floor applies.
    pytest.param(
        _scenario("connection", "full_refresh"),
        {"users": VALID_USERS},
        None,
        id="full_refresh_only_empty_stream",
    ),
    pytest.param(
        _scenario("connection", "full_refresh"),
        {},
        MSG_NO_RECORDS,
        id="full_refresh_only_no_records",
    ),
    pytest.param(
        _scenario("connection", "full_refresh"),
        {"users": [{"id": "1"}]},
        None,
        id="full_refresh_only_schema_mismatch",
    ),
    pytest.param(
        _scenario("connection", "basic_read"),
        {"users": [{"id": "1"}], "events": VALID_EVENTS},
        MSG_SCHEMA,
        id="basic_read_schema_mismatch",
    ),
    pytest.param(
        _scenario("connection", "basic_read", validate_schema=False),
        {"users": [{"id": "1"}], "events": VALID_EVENTS},
        None,
        id="basic_read_schema_mismatch_opted_out",
    ),
    # No declared `status` means `succeed`, the CAT default, so both checks apply.
    pytest.param(
        _scenario("basic_read", status=None),
        {"users": VALID_USERS},
        MSG_MISSING_STREAMS,
        id="no_status_empty_stream",
    ),
    pytest.param(
        _scenario("basic_read", status=None),
        {"users": [{"id": "1"}]},
        MSG_SCHEMA,
        id="no_status_schema_mismatch",
    ),
]


@pytest.mark.parametrize("scenario, records, failure_match", BASIC_READ_MATRIX)
def test_basic_read(
    tmp_path: Path,
    scenario: ConnectorTestScenario,
    records: Mapping[str, list[Mapping[str, Any]]],
    failure_match: str | None,
) -> None:
    if failure_match is None:
        _run_basic_read(tmp_path, scenario, records)
        return

    with pytest.raises(AssertionError, match=failure_match):
        _run_basic_read(tmp_path, scenario, records)


# --- `validate_schema` in `acceptance-test-config.yml` ----------------------------------------


def test_validate_schema_opt_out_survives_scenario_dedup(tmp_path: Path) -> None:
    (tmp_path / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump(
            {
                "acceptance_tests": {
                    "connection": {
                        "tests": [{"config_path": "secrets/config.json", "status": "succeed"}]
                    },
                    "basic_read": {
                        "tests": [{"config_path": "secrets/config.json", "validate_schema": False}]
                    },
                }
            }
        )
    )

    class _Suite(DockerConnectorTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    [scenario] = _Suite.get_scenarios()
    assert scenario.validate_schema is False
    assert scenario.is_basic_read_config
    assert scenario.expected_outcome.expect_success()
