# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the primary key, state message and error trace checks of the `read` tests.

These port three more checks of the Connector Acceptance Tests (CAT) basic read test:

- every record of a stream with a `source_defined_primary_key` has a usable primary key
  (`_validate_primary_keys_data_type`);
- every configured stream emits per-stream state with `sourceStats` (`_validate_state_messages`);
- a failing read emits a well-formed `ERROR` trace message
  (`test_airbyte_trace_message_on_failure`).

They also cover the advisory stream status check (`_validate_stream_statuses`), which warns
instead of failing.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest
import yaml

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteErrorTraceMessage,
    AirbyteGlobalState,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateStats,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    AirbyteStreamStatus,
    AirbyteTraceMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationSyncMode,
    FailureType,
    Status,
    StreamDescriptor,
    SyncMode,
    TraceType,
    Type,
)
from airbyte_cdk.sources import Source
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests import SourceTestSuiteBase
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests._read_assertions import (
    INVALID_CONFIG_MARKER,
    MAX_PRIMARY_KEY_ERRORS_PER_STREAM,
    StreamStatusWarning,
    assert_error_trace_messages,
    assert_read_records,
    find_stream_status_problems,
    make_invalid_config,
)
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG
from airbyte_cdk.utils.stream_status_utils import as_airbyte_message as stream_status_message
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.test.read_test_helpers import final_state

MSG_PRIMARY_KEY = "Records have invalid primary keys"
MSG_STATE = "The read's state messages are incomplete"
MSG_NO_ERROR_TRACE = "emitted no `TRACE` message of type `ERROR`"
MSG_MALFORMED_TRACE = "trace messages of the failed read are not well-formed"
SENTINEL = "sk_live_SENTINEL_5c4b3a"

SCHEMA: dict[str, Any] = {"type": "object", "properties": {"id": {}, "org": {}, "name": {}}}


def _catalog(
    primary_keys: Mapping[str, list[list[str]] | None], namespace: str | None = None
) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=name,
                    namespace=namespace,
                    json_schema=SCHEMA,
                    supported_sync_modes=[SyncMode.full_refresh],
                    source_defined_primary_key=primary_key,
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append_dedup,
            )
            for name, primary_key in primary_keys.items()
        ]
    )


def _record(stream: str, data: Mapping[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(stream=stream, data=dict(data), emitted_at=0),
    )


def _key_error(
    primary_key: list[list[str]] | None,
    *records: Mapping[str, Any],
    validate_primary_keys: bool = True,
) -> str | None:
    """Check `records` of a single stream; return the failure message, or None if they pass."""
    try:
        assert_read_records(
            records=[_record("s", data) for data in records],
            configured_catalog=_catalog({"s": primary_key}),
            require_records_per_stream=True,
            validate_schema=False,
            validate_primary_keys=validate_primary_keys,
        )
    except AssertionError as error:
        return str(error)
    return None


# --- Primary keys ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "primary_key, data",
    [
        pytest.param([["id"]], {"id": 1}, id="integer"),
        pytest.param([["id"]], {"id": "a"}, id="string"),
        pytest.param([["id"]], {"id": 1.5}, id="number"),
        # CAT only forbade objects and arrays; a boolean is a scalar.
        pytest.param([["id"]], {"id": True}, id="boolean"),
        pytest.param([["id"]], {"id": ""}, id="empty_string"),
        pytest.param([["id"]], {"id": 0}, id="zero"),
        pytest.param([["id"], ["org"]], {"id": 1, "org": "x"}, id="composite"),
        # As in CAT, a composite key may have some of its fields null or missing.
        pytest.param([["id"], ["org"]], {"id": 1, "org": None}, id="composite_one_null"),
        pytest.param([["id"], ["org"]], {"org": "x"}, id="composite_one_missing"),
        pytest.param([["org", "id"]], {"org": {"id": 7}}, id="nested_key_path"),
    ],
)
def test_usable_primary_keys_pass(primary_key: list[list[str]], data: Mapping[str, Any]) -> None:
    assert _key_error(primary_key, data) is None


@pytest.mark.parametrize(
    "primary_key, data, expected",
    [
        pytest.param(
            [["id"]],
            {"name": "x"},
            "record #0: every key field is missing or null (`$.id` is missing)",
            id="missing",
        ),
        pytest.param(
            [["id"]],
            {"id": None},
            "record #0: every key field is missing or null (`$.id` is null)",
            id="null",
        ),
        pytest.param(
            [["id"], ["org"]],
            {"id": None},
            "every key field is missing or null (`$.id` is null, `$.org` is missing)",
            id="composite_all_missing_or_null",
        ),
        pytest.param(
            [["org", "id"]],
            {"org": "not an object"},
            "every key field is missing or null (`$.org.id` is missing)",
            id="nested_parent_not_an_object",
        ),
        pytest.param(
            [["org", "id"]],
            {"org": "an id"},
            "every key field is missing or null (`$.org.id` is missing)",
            id="nested_parent_a_string_containing_the_key",
        ),
        pytest.param(
            [["org", "id"]],
            {"org": 7},
            "every key field is missing or null (`$.org.id` is missing)",
            id="nested_parent_a_number",
        ),
        pytest.param(
            [["id"]],
            {"id": {"value": SENTINEL}},
            "record #0: key field `$.id` is an object; a primary key field must hold a scalar",
            id="object",
        ),
        pytest.param(
            [["id"]],
            {"id": [SENTINEL]},
            "record #0: key field `$.id` is an array",
            id="array",
        ),
        pytest.param(
            [["id"], ["org"]],
            {"id": 1, "org": {"name": SENTINEL}},
            "key field `$.org` is an object",
            id="composite_one_object",
        ),
    ],
)
def test_unusable_primary_keys_fail_with_stream_record_and_key_path(
    primary_key: list[list[str]], data: Mapping[str, Any], expected: str
) -> None:
    message = _key_error(primary_key, data)
    assert message is not None
    assert MSG_PRIMARY_KEY in message
    assert "- Stream 's' (primary key " in message
    assert "1 of 1 records have an invalid primary key." in message
    assert expected in message
    assert "validate_primary_keys_data_type: false" in message
    assert SENTINEL not in message


def test_primary_key_errors_never_print_key_values() -> None:
    message = _key_error([["id"], ["org"]], {"id": {"x": SENTINEL}, "org": [SENTINEL]})
    assert message is not None
    # One record with two problems is one invalid record.
    assert "1 of 1 records have an invalid primary key." in message
    assert "`$.id` is an object" in message
    assert "`$.org` is an array" in message
    assert SENTINEL not in message


def test_streams_without_a_primary_key_are_not_checked() -> None:
    assert _key_error(None, {"id": None}) is None
    assert _key_error([], {"id": {"x": 1}}) is None


def test_validate_primary_keys_false_skips_the_check() -> None:
    assert _key_error([["id"]], {"id": None}, validate_primary_keys=False) is None


def test_primary_keys_are_not_checked_by_default() -> None:
    """Callers that predate the check (the Docker read test) keep their behaviour."""
    assert_read_records(
        records=[_record("s", {"id": None})],
        configured_catalog=_catalog({"s": [["id"]]}),
        require_records_per_stream=True,
        validate_schema=False,
    )


def test_record_numbers_count_the_records_of_each_stream() -> None:
    with pytest.raises(AssertionError) as error:
        assert_read_records(
            records=[
                _record("a", {"id": 1}),
                _record("b", {"id": 1}),
                _record("a", {"id": 2}),
                _record("a", {"id": None}),
                _record("b", {"id": None}),
            ],
            configured_catalog=_catalog({"a": [["id"]], "b": [["id"]]}),
            require_records_per_stream=True,
            validate_schema=False,
            validate_primary_keys=True,
        )
    message = str(error.value)
    assert "- Stream 'a' (primary key `$.id`): 1 of 3 records" in message
    assert "record #2: every key field is missing or null" in message
    assert "- Stream 'b' (primary key `$.id`): 1 of 2 records" in message
    assert "record #1: every key field is missing or null" in message


def test_each_primary_key_problem_is_reported_once_with_its_first_record() -> None:
    message = _key_error([["id"]], {"id": 1}, {"id": None}, {"id": None}, {"id": [1]}, {"id": [2]})
    assert message is not None
    assert "4 of 5 records have an invalid primary key." in message
    assert message.count("is null") == 1
    assert "record #1: every key field is missing or null (`$.id` is null)" in message
    assert message.count("is an array") == 1
    assert "record #3: key field `$.id` is an array" in message


def test_distinct_primary_key_errors_are_capped_per_stream() -> None:
    fields = [f"k{index}" for index in range(MAX_PRIMARY_KEY_ERRORS_PER_STREAM + 2)]
    message = _key_error([[name] for name in fields], {name: [1] for name in fields})
    assert message is not None
    assert message.count("is an array") == MAX_PRIMARY_KEY_ERRORS_PER_STREAM
    assert "...and 2 more distinct primary key error(s)." in message


def test_primary_key_failures_are_reported_with_presence_failures() -> None:
    with pytest.raises(AssertionError) as error:
        assert_read_records(
            records=[_record("a", {"id": None})],
            configured_catalog=_catalog({"a": [["id"]], "b": [["id"]]}),
            require_records_per_stream=True,
            validate_schema=False,
            validate_primary_keys=True,
        )
    assert "Every configured stream must return at least one record" in str(error.value)
    assert MSG_PRIMARY_KEY in str(error.value)


# --- State messages --------------------------------------------------------------------------


def _state_error(
    state_messages: Iterable[AirbyteMessage],
    streams: Iterable[str] = ("a", "b"),
    namespace: str | None = None,
) -> str | None:
    """Check a read of `streams` (one record each) with `state_messages`."""
    stream_names = list(streams)
    try:
        assert_read_records(
            records=[_record(name, {"id": 1}) for name in stream_names],
            configured_catalog=_catalog({name: None for name in stream_names}, namespace),
            require_records_per_stream=True,
            validate_schema=False,
            state_messages=state_messages,
        )
    except AssertionError as error:
        return str(error)
    return None


def _global_state(*streams: str) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            type=AirbyteStateType.GLOBAL,
            global_=AirbyteGlobalState(
                shared_state=AirbyteStateBlob(cursor=1),
                stream_states=[
                    AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(name=name),
                        stream_state=AirbyteStateBlob(cursor=1),
                    )
                    for name in streams
                ],
            ),
            sourceStats=AirbyteStateStats(recordCount=1.0),
        ),
    )


def _legacy_state() -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            type=AirbyteStateType.LEGACY,
            data={"a": {"cursor": 1}},
            sourceStats=AirbyteStateStats(recordCount=1.0),
        ),
    )


def test_a_final_state_per_stream_passes() -> None:
    assert (
        _state_error([final_state("a", record_count=1), final_state("b", record_count=1)]) is None
    )


def test_a_global_state_covers_its_streams() -> None:
    assert _state_error([_global_state("a", "b")]) is None


def test_state_is_matched_by_namespace_and_name() -> None:
    assert (
        _state_error(
            [final_state(name, namespace="ns", record_count=1) for name in ("a", "b")],
            namespace="ns",
        )
        is None
    )
    message = _state_error(
        [final_state(name, record_count=1) for name in ("a", "b")], namespace="ns"
    )
    assert message is not None
    assert "'ns.a', 'ns.b'" in message


def test_a_stream_without_state_fails_and_is_named() -> None:
    message = _state_error([final_state("a", record_count=1)])
    assert message is not None
    assert MSG_STATE in message
    assert (
        "1 of 2 configured streams emitted no `STREAM` or `GLOBAL` state message: 'b'." in message
    )
    assert "validate_state_messages: false" in message


def test_many_states_from_one_stream_do_not_cover_another() -> None:
    """CAT only compared counts: two states for `a` hid that `b` emitted none."""
    message = _state_error([final_state("a", record_count=1), final_state("a", record_count=0)])
    assert message is not None
    assert "emitted no `STREAM` or `GLOBAL` state message: 'b'." in message


def test_a_read_without_state_fails() -> None:
    message = _state_error([])
    assert message is not None
    assert "2 of 2 configured streams emitted no" in message


def test_legacy_state_fails() -> None:
    message = _state_error(
        [final_state("a", record_count=1), final_state("b", record_count=1), _legacy_state()]
    )
    assert message is not None
    assert "1 of 3 state messages use the deprecated `LEGACY` state type" in message
    assert "state message(s) #2." in message


def _stream_state(
    stream: str,
    *,
    state_type: AirbyteStateType | None = AirbyteStateType.STREAM,
    with_stream: bool = True,
    source_stats: AirbyteStateStats | None = None,
) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            type=state_type,
            stream=(
                AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name=stream),
                    stream_state=AirbyteStateBlob(cursor=1),
                )
                if with_stream
                else None
            ),
            sourceStats=source_stats or AirbyteStateStats(recordCount=1.0),
        ),
    )


def test_untyped_state_fails_and_covers_no_stream() -> None:
    # The in-process entrypoint passes an untyped state that carries `stream`.
    message = _state_error([final_state("a", record_count=1), _stream_state("b", state_type=None)])
    assert message is not None
    assert "1 of 2 configured streams emitted no `STREAM` or `GLOBAL` state message: 'b'" in message
    assert "1 of 2 state messages use the deprecated `LEGACY` state type (or no type)" in message


@pytest.mark.parametrize(
    "state",
    [
        pytest.param(_stream_state("b", with_stream=False), id="stream_without_stream"),
        pytest.param(
            AirbyteMessage(
                type=Type.STATE,
                state=AirbyteStateMessage(
                    type=AirbyteStateType.GLOBAL, sourceStats=AirbyteStateStats(recordCount=1.0)
                ),
            ),
            id="global_without_global",
        ),
    ],
)
def test_state_without_the_object_its_type_names_fails(state: AirbyteMessage) -> None:
    message = _state_error([final_state("a", record_count=1), state])
    assert message is not None
    assert (
        "1 of 2 state messages are of type `STREAM` without a `stream` object, or `GLOBAL` "
        "without a `global` object: state message(s) #1." in message
    )
    assert "`LEGACY`" not in message


def test_state_without_a_record_count_fails() -> None:
    message = _state_error(
        [
            final_state("a", record_count=1),
            _stream_state("b", source_stats=AirbyteStateStats(recordCount=None)),
        ]
    )
    assert message is not None
    assert "1 of 2 state messages carry no `sourceStats.recordCount`" in message
    assert "state message(s) #1." in message


def test_state_without_source_stats_fails() -> None:
    message = _state_error([final_state("a", record_count=1), final_state("b")])
    assert message is not None
    assert "1 of 2 state messages carry no `sourceStats.recordCount`" in message
    assert "state message(s) #1." in message


def test_state_messages_none_skips_the_check() -> None:
    assert_read_records(
        records=[_record("a", {"id": 1})],
        configured_catalog=_catalog({"a": None}),
        require_records_per_stream=True,
        validate_schema=False,
        state_messages=None,
    )


def test_other_messages_among_the_state_messages_are_ignored() -> None:
    assert (
        _state_error(
            [_record("a", {"id": 1}), final_state("a", record_count=1), _global_state("b")]
        )
        is None
    )


# --- Error trace messages --------------------------------------------------------------------


def _error_trace(
    *,
    message: str | None = "The API rejected the credentials.",
    failure_type: FailureType | None = FailureType.config_error,
    emitted_at: float = 1_700_000_000_000.0,
    with_error: bool = True,
) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.TRACE,
        trace=AirbyteTraceMessage(
            type=TraceType.ERROR,
            emitted_at=emitted_at,
            error=(
                AirbyteErrorTraceMessage(
                    message=message,  # type: ignore[arg-type]  # A malformed trace.
                    internal_message=SENTINEL,
                    failure_type=failure_type,
                )
                if with_error
                else None
            ),
        ),
    )


def test_a_well_formed_error_trace_passes() -> None:
    assert_error_trace_messages([_record("a", {"id": 1}), _error_trace()])


def test_no_error_trace_fails() -> None:
    log = AirbyteMessage(
        type=Type.TRACE, trace=AirbyteTraceMessage(type=TraceType.ESTIMATE, emitted_at=1.0)
    )
    with pytest.raises(AssertionError, match=MSG_NO_ERROR_TRACE):
        assert_error_trace_messages([_record("a", {"id": 1}), log])


@pytest.mark.parametrize(
    "trace, expected",
    [
        pytest.param(_error_trace(message=""), "a non-blank `error.message`", id="empty_message"),
        pytest.param(_error_trace(message="  \n"), "a non-blank `error.message`", id="blank"),
        pytest.param(_error_trace(message=None), "a non-blank `error.message`", id="no_message"),
        pytest.param(
            _error_trace(failure_type=None), "an `error.failure_type`", id="no_failure_type"
        ),
        pytest.param(_error_trace(emitted_at=0), "a positive `emitted_at`", id="no_emitted_at"),
        pytest.param(
            _error_trace(emitted_at=True), "a positive `emitted_at`", id="boolean_emitted_at"
        ),
        pytest.param(_error_trace(with_error=False), "the `error` object", id="no_error_object"),
    ],
)
def test_malformed_error_traces_fail(trace: AirbyteMessage, expected: str) -> None:
    with pytest.raises(AssertionError, match=MSG_MALFORMED_TRACE) as error:
        assert_error_trace_messages([_error_trace(), trace])
    message = str(error.value)
    assert "1 of 2 `ERROR` trace messages" in message
    assert f"- error trace #1 lacks {expected}" in message
    assert SENTINEL not in message


def test_make_invalid_config() -> None:
    assert make_invalid_config({"type": "object", "required": ["api_key"]}) == {
        INVALID_CONFIG_MARKER: True
    }
    assert make_invalid_config({"type": "object", "properties": {"a": {}}}) is None
    assert make_invalid_config({"type": "object", "required": []}) is None


# --- Through the source test suite -----------------------------------------------------------

SPEC_REQUIRING_API_KEY: dict[str, Any] = {
    "type": "object",
    "required": ["api_key"],
    "properties": {"api_key": {"type": "string", "airbyte_secret": True}},
}


class _Source(Source):
    """A source over one `users` stream with primary key `id`.

    Behaviours:
    - `records`: the given records, then the final state (unless `emit_state` is false),
      between `STARTED`, `RUNNING` and `COMPLETE` stream statuses (unless `emit_statuses` is
      false).
    - `skip_config_check`: like `records`, without validating the config against the spec.
    - `malformed_trace`: without validating the config, yield an `ERROR` trace with a blank
      message, then raise.
    """

    def __init__(
        self,
        records: list[Mapping[str, Any]] | None = None,
        *,
        behavior: str = "records",
        spec: Mapping[str, Any] = SPEC_REQUIRING_API_KEY,
        emit_state: bool = True,
        emit_statuses: bool = True,
    ) -> None:
        self._records = records or []
        self._behavior = behavior
        self._spec = dict(spec)
        self._emit_state = emit_state
        self._emit_statuses = emit_statuses
        self.check_config_against_spec = behavior == "records"

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(connectionSpecification=self._spec)

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(
            streams=[
                AirbyteStream(
                    name="users",
                    json_schema=SCHEMA,
                    supported_sync_modes=[SyncMode.full_refresh],
                    source_defined_primary_key=[["id"]],
                )
            ]
        )

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        if self._behavior == "malformed_trace":
            yield _error_trace(message=" ")
            raise RuntimeError("The read failed.")
        for configured_stream in catalog.streams:
            if configured_stream.stream.name != "users":
                continue
            users = StreamDescriptor(name="users")
            if self._emit_statuses:
                yield stream_status_message(users, AirbyteStreamStatus.STARTED)
                yield stream_status_message(users, AirbyteStreamStatus.RUNNING)
            for data in self._records:
                yield _record("users", data)
            if self._emit_state:
                yield final_state("users")
            if self._emit_statuses:
                yield stream_status_message(users, AirbyteStreamStatus.COMPLETE)


def _suite(tmp_path: Path, source: _Source | Exception) -> SourceTestSuiteBase:
    # The connector's test configs are read from `acceptance-test-config.yml`, which must exist.
    # A test that needs an opt-out writes its own file first.
    config_file = tmp_path / ACCEPTANCE_TEST_CONFIG
    if not config_file.exists():
        config_file.write_text(
            yaml.safe_dump(
                {
                    "acceptance_tests": {
                        "basic_read": {"tests": [{"config_path": "secrets/config.json"}]}
                    }
                }
            )
        )

    class _Suite(SourceTestSuiteBase):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

        @classmethod
        def create_connector(cls, scenario: ConnectorTestScenario | None) -> IConnector:
            if isinstance(source, Exception):
                if scenario is not None and scenario.config_dict == {INVALID_CONFIG_MARKER: True}:
                    raise source
                return _Source()
            return source

    return _Suite()


def _scenario(**fields: Any) -> ConnectorTestScenario:
    return ConnectorTestScenario.model_validate(
        {"config_dict": {"api_key": "x"}, "sections": ("basic_read",), **fields}
    )


@pytest.mark.parametrize(
    "scenario, source, failure_match",
    [
        pytest.param(_scenario(), _Source([{"id": 1}]), None, id="valid"),
        pytest.param(
            _scenario(), _Source([{"id": None, "name": "x"}]), MSG_PRIMARY_KEY, id="null_key"
        ),
        pytest.param(
            _scenario(validate_primary_keys_data_type=False),
            _Source([{"id": None, "name": "x"}]),
            None,
            id="null_key_opted_out",
        ),
        pytest.param(_scenario(), _Source([{"id": 1}], emit_state=False), MSG_STATE, id="no_state"),
        pytest.param(
            _scenario(validate_state_messages=False),
            _Source([{"id": 1}], emit_state=False),
            None,
            id="no_state_opted_out",
        ),
        # A config read only because it is listed under `full_refresh` keeps the old floor.
        pytest.param(
            _scenario(sections=("full_refresh",)),
            _Source([{"id": None, "name": "x"}], emit_state=False),
            None,
            id="full_refresh_only",
        ),
        # `validate_primary_keys_data_type: false` stays available at high strictness, as in CAT.
        pytest.param(
            _scenario(validate_primary_keys_data_type=False, test_strictness_level="high"),
            _Source([{"id": None, "name": "x"}]),
            None,
            id="high_strictness_allows_primary_key_opt_out",
        ),
    ],
)
def test_basic_read(
    tmp_path: Path,
    scenario: ConnectorTestScenario,
    source: _Source,
    failure_match: str | None,
) -> None:
    suite = _suite(tmp_path, source)
    if failure_match is None:
        suite.test_basic_read(scenario)
        return
    with pytest.raises(AssertionError, match=failure_match):
        suite.test_basic_read(scenario)


# --- Stream statuses (advisory) ----------------------------------------------------------------


def _statuses(
    stream: str, *statuses: AirbyteStreamStatus, namespace: str | None = None
) -> list[AirbyteMessage]:
    descriptor = StreamDescriptor(name=stream, namespace=namespace)
    return [stream_status_message(descriptor, status) for status in statuses]


STARTED, RUNNING, COMPLETE, INCOMPLETE = (
    AirbyteStreamStatus.STARTED,
    AirbyteStreamStatus.RUNNING,
    AirbyteStreamStatus.COMPLETE,
    AirbyteStreamStatus.INCOMPLETE,
)


@pytest.mark.parametrize(
    "messages, expected",
    [
        pytest.param(
            _statuses("a", STARTED, RUNNING, RUNNING, COMPLETE) + _statuses("b", STARTED, COMPLETE),
            [],
            id="complete_with_and_without_running",
        ),
        pytest.param(
            _statuses("a", STARTED, RUNNING, COMPLETE),
            ["1 of 2 configured streams reported no stream status: 'b'."],
            id="stream_without_status",
        ),
        pytest.param(
            _statuses("a", STARTED, COMPLETE)
            + _statuses("b", STARTED, COMPLETE)
            + _statuses("c", STARTED, COMPLETE),
            ["1 streams that are not in the configured catalog reported a stream status: 'c'."],
            id="unconfigured_stream",
        ),
        pytest.param(
            _statuses("a", STARTED, RUNNING) + _statuses("b", STARTED, COMPLETE),
            ["stream 'a' reported STARTED, RUNNING; expected"],
            id="no_complete",
        ),
        pytest.param(
            _statuses("a", STARTED, RUNNING, INCOMPLETE) + _statuses("b", STARTED, COMPLETE),
            ["stream 'a' reported STARTED, RUNNING, INCOMPLETE; expected"],
            id="incomplete",
        ),
        pytest.param(
            _statuses("a", RUNNING, COMPLETE) + _statuses("b", STARTED, COMPLETE),
            ["stream 'a' reported RUNNING, COMPLETE; expected"],
            id="no_started",
        ),
        pytest.param(
            _statuses("a", STARTED, COMPLETE, RUNNING, COMPLETE)
            + _statuses("b", STARTED, COMPLETE),
            ["stream 'a' reported STARTED, COMPLETE, RUNNING, COMPLETE; expected"],
            id="complete_before_the_end",
        ),
        pytest.param(
            _statuses("a", COMPLETE) + _statuses("b", STARTED, COMPLETE),
            ["stream 'a' reported COMPLETE; expected"],
            id="single_status",
        ),
    ],
)
def test_find_stream_status_problems(messages: list[AirbyteMessage], expected: list[str]) -> None:
    problems = find_stream_status_problems(messages, _catalog({"a": None, "b": None}))
    assert len(problems) == len(expected)
    for problem, fragment in zip(problems, expected):
        assert fragment in problem


def test_find_stream_status_problems_ignores_other_messages() -> None:
    messages = [
        _record("a", {"id": 1}),
        final_state("a"),
        _error_trace(message="boom"),
        *_statuses("a", STARTED, COMPLETE),
    ]
    assert find_stream_status_problems(messages, _catalog({"a": None})) == []


def test_find_stream_status_problems_matches_the_namespace() -> None:
    catalog = _catalog({"a": None}, namespace="public")
    assert (
        find_stream_status_problems(_statuses("a", STARTED, COMPLETE, namespace="public"), catalog)
        == []
    )
    problems = find_stream_status_problems(_statuses("a", STARTED, COMPLETE), catalog)
    assert [problem.split(":")[0] for problem in problems] == [
        "- 1 of 1 configured streams reported no stream status",
        "- 1 streams that are not in the configured catalog reported a stream status",
    ]
    assert "'public.a'" in problems[0]


def test_basic_read_warns_on_missing_stream_statuses(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _Source([{"id": 1}], emit_statuses=False))
    with pytest.warns(StreamStatusWarning, match="reported no stream status: 'users'"):
        suite.test_basic_read(_scenario())


def test_basic_read_warns_on_stream_statuses_when_a_check_fails(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _Source([{"id": None, "name": "x"}], emit_statuses=False))
    with pytest.warns(StreamStatusWarning, match="reported no stream status: 'users'"):
        with pytest.raises(AssertionError, match=MSG_PRIMARY_KEY):
            suite.test_basic_read(_scenario())


def test_basic_read_does_not_warn_on_complete_stream_statuses(tmp_path: Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", StreamStatusWarning)
        _suite(tmp_path, _Source([{"id": 1}])).test_basic_read(_scenario())


def test_basic_read_skips_stream_statuses_for_a_full_refresh_only_config(tmp_path: Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", StreamStatusWarning)
        _suite(tmp_path, _Source([{"id": 1}], emit_statuses=False)).test_basic_read(
            _scenario(sections=("full_refresh",))
        )


def test_basic_read_stream_status_warning_can_be_made_an_error(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _Source([{"id": 1}], emit_statuses=False))
    with warnings.catch_warnings():
        warnings.simplefilter("error", StreamStatusWarning)
        with pytest.raises(StreamStatusWarning):
            suite.test_basic_read(_scenario())


def test_fail_read_with_invalid_config_passes_when_the_spec_rejects_it(tmp_path: Path) -> None:
    _suite(tmp_path, _Source([{"id": 1}])).test_fail_read_with_invalid_config()


def test_fail_read_with_invalid_config_fails_when_the_read_succeeds(tmp_path: Path) -> None:
    with pytest.raises(AssertionError) as error:
        _suite(
            tmp_path, _Source([{"id": 1}], behavior="skip_config_check")
        ).test_fail_read_with_invalid_config()
    message = str(error.value)
    assert "The read succeeded with a config that the connector's spec rejects" in message
    assert "`check_config_against_spec = False`" in message
    assert "`expect_trace_message_on_failure: false`" in message


def test_fail_read_with_invalid_config_fails_on_a_malformed_trace(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match=MSG_MALFORMED_TRACE) as error:
        _suite(tmp_path, _Source(behavior="malformed_trace")).test_fail_read_with_invalid_config()
    assert "1 of 2 `ERROR` trace messages" in str(error.value)
    assert SENTINEL not in str(error.value)


def test_fail_read_with_invalid_config_when_the_source_cannot_be_created(tmp_path: Path) -> None:
    _suite(
        tmp_path,
        AirbyteTracedException(
            message="The config is invalid.", failure_type=FailureType.config_error
        ),
    ).test_fail_read_with_invalid_config()
    _suite(tmp_path, KeyError("api_key")).test_fail_read_with_invalid_config()


def test_fail_read_with_invalid_config_skips_without_required_properties(tmp_path: Path) -> None:
    source = _Source(spec={"type": "object", "properties": {}})
    with pytest.raises(pytest.skip.Exception, match="requires no top-level property"):
        _suite(tmp_path, source).test_fail_read_with_invalid_config()


def test_fail_read_with_invalid_config_honours_the_opt_out(tmp_path: Path) -> None:
    (tmp_path / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump(
            {
                "acceptance_tests": {
                    "basic_read": {
                        "tests": [
                            {
                                "config_path": "secrets/config.json",
                                "expect_trace_message_on_failure": False,
                            }
                        ]
                    }
                }
            }
        )
    )
    with pytest.raises(pytest.skip.Exception, match="expect_trace_message_on_failure: false"):
        _suite(
            tmp_path, _Source([{"id": 1}], behavior="skip_config_check")
        ).test_fail_read_with_invalid_config()


def test_fail_read_with_bad_catalog_checks_the_trace(tmp_path: Path) -> None:
    _suite(tmp_path, _Source([{"id": 1}])).test_fail_read_with_bad_catalog(_scenario())


def test_fail_read_with_bad_catalog_fails_on_a_malformed_trace(tmp_path: Path) -> None:
    # Without the stub, the entrypoint rejects the invalid catalog before the source reads.
    source = _Source(behavior="malformed_trace")
    source.read_catalog = lambda catalog_path: ConfiguredAirbyteCatalog(streams=[])  # type: ignore[method-assign]
    with pytest.raises(AssertionError, match=MSG_MALFORMED_TRACE) as error:
        _suite(tmp_path, source).test_fail_read_with_bad_catalog(_scenario())
    assert SENTINEL not in str(error.value)


def test_fail_read_with_bad_catalog_honours_the_opt_out(tmp_path: Path) -> None:
    # As in CAT, the opt-out skips this test for its config.
    with pytest.raises(pytest.skip.Exception, match="expect_trace_message_on_failure: false"):
        _suite(tmp_path, _Source([{"id": 1}])).test_fail_read_with_bad_catalog(
            _scenario(config_path="secrets/config.json", expect_trace_message_on_failure=False)
        )


# --- Opt-outs in `acceptance-test-config.yml` ------------------------------------------------

OPT_OUTS = (
    "validate_primary_keys_data_type",
    "validate_state_messages",
    "expect_trace_message_on_failure",
)


@pytest.mark.parametrize("opt_out", OPT_OUTS)
@pytest.mark.parametrize(
    "values",
    [
        pytest.param((False, True), id="false_then_true"),
        pytest.param((True, False), id="true_then_false"),
    ],
)
def test_opt_outs_are_kept_through_scenario_dedup(
    tmp_path: Path, opt_out: str, values: tuple[bool, bool]
) -> None:
    (tmp_path / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump(
            {
                "acceptance_tests": {
                    "connection": {
                        "tests": [{"config_path": "secrets/config.json", "status": "succeed"}]
                    },
                    "basic_read": {
                        "tests": [
                            {"config_path": "secrets/config.json", opt_out: value}
                            for value in values
                        ]
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
    assert getattr(scenario, opt_out) is False
    assert all(getattr(scenario, other) for other in OPT_OUTS if other != opt_out)
    assert scenario.validate_schema is True
