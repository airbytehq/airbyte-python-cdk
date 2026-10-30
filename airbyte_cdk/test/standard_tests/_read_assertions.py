# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Record assertions for the `read` standard tests.

These port the record checks of the Connector Acceptance Tests (CAT) `TestBasicRead.test_read`,
which the standard tests did not reproduce:

- every configured stream returns at least one record (CAT `_validate_empty_streams`);
- every record conforms to its stream's JSON schema (CAT `_validate_schema`), with the same
  validator CAT ran: Draft 7 with strict integers and a lenient `date-time` format check, and
  without failing on extra columns;
- every record of a stream with a `source_defined_primary_key` has a usable primary key: no key
  field holds an object or an array, and not every key field is missing or null (CAT
  `_validate_primary_keys_data_type`);
- every configured stream emits a `STREAM` (or `GLOBAL`) state message carrying `sourceStats`
  (the per-stream form of CAT `_validate_state_messages`).

`find_stream_status_problems` ports CAT `_validate_stream_statuses`: every configured stream
reports `STARTED`, then only `RUNNING`, and ends with `COMPLETE`. It is advisory for now: the
`read` tests report its problems as a `StreamStatusWarning` and do not fail.

They also include the check of CAT's `TestBasicRead.test_airbyte_trace_message_on_failure`: a
read that fails emits a well-formed `TRACE` message of type `ERROR` (`assert_error_trace_messages`).

They take the read output and the configured catalog rather than a scenario, so the in-process
and the Docker read tests can share them.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from jsonschema import Draft7Validator, FormatChecker, validators
from jsonschema.exceptions import FormatError, UnknownType, ValidationError, best_match
from referencing.exceptions import Unresolvable

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateType,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    TraceType,
    Type,
)
from airbyte_cdk.utils.datetime_helpers import ab_datetime_try_parse

MAX_SCHEMA_ERRORS_PER_STREAM = 5
"""How many distinct schema errors are reported per stream. The total is always reported."""

MAX_PRIMARY_KEY_ERRORS_PER_STREAM = 5
"""How many distinct primary key errors are reported per stream. The total is always reported."""

_MAX_MESSAGE_CHARS = 300
_MAX_STREAM_NAMES = 10
_MAX_CONSTRAINT_CHARS = 80

# CAT's shape check for `date-time` values: a date, a ' ' or 'T' separator, a time, then anything
# (fraction, offset, zone name). The value must also parse as a datetime.
_DATETIME_SHAPE = re.compile(r"^\d{4}-\d?\d-\d?\d(\s|T)\d?\d:\d?\d:\d?\d(.\d+)?.*$")

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_strict_integer(_checker: object, instance: object) -> bool:
    # JSON Schema treats `1.0` as an integer; destinations do not, so neither do we. Booleans are
    # `int` subclasses in Python but not integers in JSON.
    return isinstance(instance, int) and not isinstance(instance, bool)


StrictIntegerDraft7Validator = validators.extend(
    Draft7Validator,
    type_checker=Draft7Validator.TYPE_CHECKER.redefine("integer", _is_strict_integer),
)
"""Draft 7 validator that only accepts `int` values as `integer`."""


class RecordFormatChecker(FormatChecker):
    """Format checker for records: CAT's lenient `date-time` check, defaults for other formats.

    The default `date-time` check requires strict RFC 3339. Sources emit many shapes that
    destinations accept (a ' ' separator, no offset, a zone name), so `date-time` values only
    need a date-and-time shape that parses as a datetime.
    """

    def check(self, instance: object, format: str) -> None:
        if format == "date-time":
            if isinstance(instance, str) and not _is_valid_datetime(instance):
                raise FormatError("is not a valid 'date-time'")
            return

        super().check(instance, format)


def _is_valid_datetime(value: str) -> bool:
    return bool(_DATETIME_SHAPE.match(value)) and ab_datetime_try_parse(value) is not None


def format_record_path(path: Iterable[Any]) -> str:
    """Render a path inside a record as a JSONPath, e.g. `$.items[0]['first name']`."""
    rendered = "$"
    for part in path:
        if isinstance(part, int):
            rendered += f"[{part}]"
        elif _IDENTIFIER.match(str(part)):
            rendered += f".{part}"
        else:
            rendered += f"[{str(part)!r}]"
    return rendered


_JSON_TYPE_NAMES: tuple[tuple[type, str], ...] = (
    (bool, "boolean"),  # before `int`: `bool` is an `int` subclass
    (int, "integer"),
    (float, "number"),
    (str, "string"),
    (list, "array"),
    (Mapping, "object"),
)


def _json_type_name(value: object) -> str:
    if value is None:
        return "null"
    for python_type, name in _JSON_TYPE_NAMES:
        if isinstance(value, python_type):
            return name
    return type(value).__name__


def _describe_error(error: ValidationError) -> str:
    """Describe a schema error without printing any part of the record.

    jsonschema's own messages embed the failing value (for an `anyOf` the whole nested object),
    and these messages end up in public CI logs. So only the validator keyword, the schema's own
    constraint and the JSON type of the value are reported.
    """
    keyword = error.validator
    got = _json_type_name(error.instance)
    constraint = error.validator_value

    if keyword == "type":
        expected_types = [constraint] if isinstance(constraint, str) else list(constraint)
        return f"expected type {' or '.join(repr(name) for name in expected_types)}, got {got}"

    if keyword == "required" and isinstance(error.instance, Mapping):
        # The names come from the schema, not from the record.
        missing = [name for name in constraint if name not in error.instance]
        return f"missing required field(s) {', '.join(repr(name) for name in missing)}"

    description = f"fails `{keyword}`"
    if isinstance(constraint, (str, int, float, bool)):
        # A scalar constraint (`format`, `pattern`, `maxLength`, `const`...) is schema content.
        description += f" {_truncate(repr(constraint), _MAX_CONSTRAINT_CHARS)}"
    description += f", got {got}"

    if error.context:
        # `anyOf`/`oneOf`: name the closest option's failure, described the same way. An option
        # whose type the value does not even have says the least, so it is the last resort.
        candidates = [
            option_error
            for option_error in error.context
            if not (option_error.validator == "type" and not option_error.relative_path)
        ]
        closest = best_match(candidates or error.context)
        if closest is not None:
            description += (
                f"; closest option fails at `{format_record_path(closest.absolute_path)}`: "
                f"{_describe_error(closest)}"
            )
    return description


def _truncate(text: str, limit: int = _MAX_MESSAGE_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _resolve_local_ref(root: Mapping[str, Any], ref: str) -> Any:
    if not ref.startswith("#/"):
        return None
    node: Any = root
    for part in ref[2:].split("/"):
        if not isinstance(node, Mapping) or part not in node:
            return None
        node = node[part]
    return node


def get_expected_schema_paths(schema: Mapping[str, Any]) -> set[str]:
    """Return the leaf property paths a record of this schema is expected to have.

    Paths look like `/a`, `/a/b` and `/list/[]/c`. A `oneOf`/`anyOf` contributes the paths of
    every option. An object without `properties` is a leaf. Local `$ref`s are resolved, and a
    recursive reference stops the descent.

    Ported from CAT's `get_expected_schema_structure`.
    """
    paths: set[str] = set()
    on_stack: set[int] = set()

    def _scan(subschema: Any, path: str) -> None:
        if not isinstance(subschema, Mapping):
            if path:
                paths.add(path)
            return
        if "$ref" in subschema:
            target = _resolve_local_ref(schema, subschema["$ref"])
            if target is None or id(target) in on_stack:
                if path:
                    paths.add(path)
                return
            on_stack.add(id(target))
            _scan(target, path)
            on_stack.discard(id(target))
            return

        options = subschema.get("oneOf") or subschema.get("anyOf")
        if options:
            for option in options:
                _scan({"type": "object", **option} if isinstance(option, Mapping) else option, path)
            return

        schema_type = subschema.get("type", ["object", "null"])
        if not isinstance(schema_type, list):
            schema_type = [schema_type]
        if "object" in schema_type and subschema.get("properties"):
            for name, property_schema in subschema["properties"].items():
                _scan(property_schema, f"{path}/{name}")
            return
        if "array" in schema_type and "object" not in schema_type:
            _scan(subschema.get("items", {}), f"{path}/[]")
            return
        if path:
            paths.add(path)

    _scan(schema, "")
    return paths


def get_record_paths(data: Any) -> set[str]:
    """Return every key path in a record, with list items represented by their first element.

    Ported from CAT's `get_object_structure`.
    """
    paths: set[str] = set()

    def _traverse(value: Any, path: str) -> None:
        if path:
            paths.add(path)
        if isinstance(value, dict):
            for key, nested in value.items():
                _traverse(nested, f"{path}/{key}")
        elif isinstance(value, list) and value:
            _traverse(value[0], f"{path}/[]")

    _traverse(data, "")
    return paths


def _describe_schema_problem(error: Exception) -> str:
    """Describe an error the validator raised on a broken schema, without the record value.

    The exceptions' own messages print the instance being checked (`While checking instance:`),
    so only the schema part of the problem is reported.
    """
    if isinstance(error, UnknownType):
        return f"unknown type {_truncate(repr(error.type), _MAX_CONSTRAINT_CHARS)}"
    if isinstance(error, Unresolvable):
        return f"unresolvable `$ref` {_truncate(repr(error.ref), _MAX_CONSTRAINT_CHARS)}"
    if isinstance(error, re.error):
        pattern = error.pattern if isinstance(error.pattern, str) else None
        return f"invalid `pattern` {_truncate(repr(pattern), _MAX_CONSTRAINT_CHARS)}: {error.msg}"
    return f"the validator raised {type(error).__name__}"


@dataclass
class _StreamSchemaReport:
    """Schema problems found in one stream's records."""

    records_checked: int = 0
    invalid_records: int = 0
    # Distinct errors keyed by the schema rule that failed, first occurrence wins.
    errors: dict[tuple[Any, ...], str] = field(default_factory=dict)
    unmatched_records: int = 0
    first_unmatched: str | None = None
    # Set when the schema itself cannot be applied; the stream's records are not checked further.
    invalid_schema: str | None = None


class _StreamSchemaChecker:
    def __init__(self, stream_name: str, json_schema: Mapping[str, Any]) -> None:
        self._stream_name = stream_name
        self._validator = StrictIntegerDraft7Validator(
            json_schema, format_checker=RecordFormatChecker()
        )
        self._expected_paths = get_expected_schema_paths(json_schema)
        self.report = _StreamSchemaReport()

    def check(self, data: Mapping[str, Any]) -> None:
        report = self.report
        if report.invalid_schema is not None:
            return
        record_number = report.records_checked
        report.records_checked += 1

        try:
            errors: list[ValidationError] = list(self._validator.iter_errors(data))
        except Exception as schema_problem:  # noqa: BLE001  # Any error here is a schema problem.
            report.invalid_schema = _describe_schema_problem(schema_problem)
            return
        if errors:
            report.invalid_records += 1
            for error in errors:
                schema_rule = tuple(error.schema_path)
                if schema_rule not in report.errors:
                    report.errors[schema_rule] = (
                        f"record #{record_number} at `{format_record_path(error.absolute_path)}`: "
                        f"{_truncate(_describe_error(error))} "
                        f"(schema rule: `{'/'.join(str(part) for part in schema_rule)}`)"
                    )

        # A schema with `additionalProperties` and no `required` accepts any object, so also
        # require each record to share at least one field with the schema.
        if self._expected_paths and not (get_record_paths(data) & self._expected_paths):
            report.unmatched_records += 1
            if report.first_unmatched is None:
                record_fields = sorted(data)[:10] if isinstance(data, Mapping) else []
                schema_fields = sorted(self._expected_paths)[:10]
                report.first_unmatched = (
                    f"record #{record_number} has top-level fields {record_fields}, none of which "
                    f"match the schema's fields (first 10: {schema_fields})"
                )

    def describe_failures(self) -> str | None:
        report = self.report
        lines: list[str] = []
        if report.invalid_schema is not None:
            lines.append(
                f"- Stream '{self._stream_name}': the stream's JSON schema is invalid "
                f"({report.invalid_schema}), so its records cannot be validated. Fix the schema."
            )
        if report.invalid_records:
            lines.append(
                f"- Stream '{self._stream_name}': {report.invalid_records} of "
                f"{report.records_checked} records do not match the stream's JSON schema."
            )
            shown = list(report.errors.values())[:MAX_SCHEMA_ERRORS_PER_STREAM]
            lines.extend(f"    - {error}" for error in shown)
            hidden = len(report.errors) - len(shown)
            if hidden:
                lines.append(f"    - ...and {hidden} more distinct schema error(s).")
        if report.unmatched_records:
            lines.append(
                f"- Stream '{self._stream_name}': {report.unmatched_records} of "
                f"{report.records_checked} records share no field with the stream's JSON schema; "
                f"{report.first_unmatched}."
            )
        return "\n".join(lines) if lines else None


_MISSING = object()
"""The value of a key field that is absent from the record."""


def _key_field_value(data: Any, key_path: Sequence[str]) -> Any:
    """Return the value at `key_path` in `data`, or `_MISSING` if the path does not exist."""
    node = data
    for part in key_path:
        if not isinstance(node, Mapping) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _format_primary_key(primary_key: Sequence[Sequence[str]]) -> str:
    return ", ".join(f"`{format_record_path(key_path)}`" for key_path in primary_key)


class _StreamPrimaryKeyChecker:
    """Checks the primary key of each record of one stream, as CAT's
    `_validate_primary_keys_data_type` did.

    A key field must not hold an object or an array, and at least one key field must be present
    and not null. As in CAT, a composite key may have some of its fields null. Booleans count
    as scalars, as in CAT; they are named `boolean`, never `integer`.
    """

    def __init__(self, stream_name: str, primary_key: Sequence[Sequence[str]]) -> None:
        self._stream_name = stream_name
        self._primary_key = [list(key_path) for key_path in primary_key if key_path]
        self.records_checked = 0
        self.invalid_records = 0
        # Distinct problems, keyed by the key field and the kind of problem; first record wins.
        self.errors: dict[tuple[Any, ...], str] = {}

    def check(self, data: Any, record_number: int) -> None:
        if not self._primary_key:
            return
        self.records_checked += 1
        problems: dict[tuple[Any, ...], str] = {}
        null_fields: list[str] = []
        for key_path in self._primary_key:
            value = _key_field_value(data, key_path)
            rendered_path = format_record_path(key_path)
            if value is _MISSING or value is None:
                null_fields.append(
                    f"`{rendered_path}` is {'missing' if value is _MISSING else 'null'}"
                )
            elif isinstance(value, (Mapping, list)):
                got = _json_type_name(value)
                problems[(tuple(key_path), got)] = (
                    f"record #{record_number}: key field `{rendered_path}` is an {got}; a primary "
                    "key field must hold a scalar (a string, a number or a boolean)"
                )
        if len(null_fields) == len(self._primary_key):
            problems[("every key field missing or null", *null_fields)] = (
                f"record #{record_number}: every key field is missing or null "
                f"({', '.join(null_fields)})"
            )
        if problems:
            self.invalid_records += 1
            for problem, description in problems.items():
                self.errors.setdefault(problem, description)

    def describe_failures(self) -> str | None:
        if not self.invalid_records:
            return None
        lines = [
            f"- Stream '{self._stream_name}' (primary key {_format_primary_key(self._primary_key)}): "
            f"{self.invalid_records} of {self.records_checked} records have an invalid primary key."
        ]
        shown = list(self.errors.values())[:MAX_PRIMARY_KEY_ERRORS_PER_STREAM]
        lines.extend(f"    - {error}" for error in shown)
        hidden = len(self.errors) - len(shown)
        if hidden:
            lines.append(f"    - ...and {hidden} more distinct primary key error(s).")
        return "\n".join(lines)


def _find_state_message_failures(
    state_messages: Iterable[AirbyteMessage],
    configured_catalog: ConfiguredAirbyteCatalog,
) -> list[str]:
    """Check the state messages of a full-refresh read, as CAT's `_validate_state_messages` did.

    CAT asserted that the read emitted at least as many state messages as there are configured
    streams, that none was of the deprecated `LEGACY` type, and that each carried `sourceStats`.
    State is per stream in the current protocol, so the count is checked per stream instead: a
    stream that emits several state messages cannot hide one that emits none.
    """
    configured = {
        (configured_stream.stream.namespace, configured_stream.stream.name)
        for configured_stream in configured_catalog.streams
    }
    covered: set[tuple[str | None, str]] = set()
    state_count = 0
    legacy_states: list[int] = []
    states_without_object: list[int] = []
    states_without_stats: list[int] = []
    for message in state_messages:
        state = message.state
        if message.type != Type.STATE or state is None:
            continue
        state_number = state_count
        state_count += 1
        if state.type == AirbyteStateType.STREAM and state.stream is not None:
            descriptor = state.stream.stream_descriptor
            covered.add((descriptor.namespace, descriptor.name))
        elif state.type == AirbyteStateType.GLOBAL and state.global_ is not None:
            covered.update(
                (stream_state.stream_descriptor.namespace, stream_state.stream_descriptor.name)
                for stream_state in state.global_.stream_states or []
            )
        elif state.type in (AirbyteStateType.STREAM, AirbyteStateType.GLOBAL):
            states_without_object.append(state_number)
        else:
            legacy_states.append(state_number)
        if state.sourceStats is None or state.sourceStats.recordCount is None:
            states_without_stats.append(state_number)

    def _numbers(numbers: list[int]) -> str:
        shown = ", ".join(f"#{number}" for number in numbers[:5])
        return shown + (f" and {len(numbers) - 5} more" if len(numbers) > 5 else "")

    failures: list[str] = []
    streams_without_state = sorted(
        name if namespace is None else f"{namespace}.{name}"
        for namespace, name in configured - covered
    )
    if streams_without_state:
        failures.append(
            f"- {len(streams_without_state)} of {len(configured)} configured streams emitted no "
            "`STREAM` or `GLOBAL` state message: "
            f"{', '.join(repr(name) for name in streams_without_state)}. A stream must emit a "
            "state message once it has been read in full; the CDK emits one when a stream "
            "completes."
        )
    if legacy_states:
        failures.append(
            f"- {len(legacy_states)} of {state_count} state messages use the deprecated `LEGACY` "
            f"state type (or no type): state message(s) {_numbers(legacy_states)}. Emit "
            "per-stream `STREAM` state (or `GLOBAL` state) instead."
        )
    if states_without_object:
        failures.append(
            f"- {len(states_without_object)} of {state_count} state messages are of type "
            "`STREAM` without a `stream` object, or `GLOBAL` without a `global` object: state "
            f"message(s) {_numbers(states_without_object)}. Emit the object that the type names."
        )
    if states_without_stats:
        failures.append(
            f"- {len(states_without_stats)} of {state_count} state messages carry no "
            f"`sourceStats.recordCount`: state message(s) {_numbers(states_without_stats)}. The "
            "CDK entrypoint sets it on every state message it emits."
        )
    return failures


class StreamStatusWarning(UserWarning):
    """A stream status problem in a `read` that the standard tests report but do not enforce yet.

    To make these problems fail the test, turn the warning into an error, either with
    `pytest -W error::airbyte_cdk.test.standard_tests.StreamStatusWarning` or with
    `airbyte-cdk connector test --pytest-arg=-Werror::airbyte_cdk.test.standard_tests.StreamStatusWarning`.
    """


def _stream_label(namespace: str | None, name: str) -> str:
    return name if namespace is None else f"{namespace}.{name}"


def _stream_list(labels: Iterable[str]) -> str:
    names = sorted(labels)
    shown = ", ".join(repr(name) for name in names[:_MAX_STREAM_NAMES])
    hidden = len(names) - _MAX_STREAM_NAMES
    return shown + (f" and {hidden} more" if hidden > 0 else "")


def find_stream_status_problems(
    trace_messages: Iterable[AirbyteMessage],
    configured_catalog: ConfiguredAirbyteCatalog,
) -> list[str]:
    """Check the stream status trace messages of a successful read, as CAT did.

    Ported from CAT's `_validate_stream_statuses`: every configured stream, and no other stream,
    must report its status, and each stream's statuses must be `STARTED`, then only `RUNNING`,
    then `COMPLETE`. CAT ran this check by default for certified connectors only. Here it is
    advisory: `test_basic_read` reports the problems as a `StreamStatusWarning`.
    """
    configured = {
        (configured_stream.stream.namespace, configured_stream.stream.name)
        for configured_stream in configured_catalog.streams
    }
    statuses: dict[tuple[str | None, str], list[AirbyteStreamStatus]] = {}
    for message in trace_messages:
        trace = message.trace
        if (
            message.type != Type.TRACE
            or trace is None
            or trace.type != TraceType.STREAM_STATUS
            or trace.stream_status is None
        ):
            continue
        descriptor = trace.stream_status.stream_descriptor
        statuses.setdefault((descriptor.namespace, descriptor.name), []).append(
            trace.stream_status.status
        )

    problems: list[str] = []
    silent = [_stream_label(*stream) for stream in configured - statuses.keys()]
    if silent:
        problems.append(
            f"- {len(silent)} of {len(configured)} configured streams reported no stream status: "
            f"{_stream_list(silent)}."
        )
    unexpected = [_stream_label(*stream) for stream in statuses.keys() - configured]
    if unexpected:
        problems.append(
            f"- {len(unexpected)} streams that are not in the configured catalog reported a "
            f"stream status: {_stream_list(unexpected)}."
        )
    for stream in sorted(statuses.keys() & configured, key=lambda key: (key[0] or "", key[1])):
        sequence = statuses[stream]
        if (
            len(sequence) < 2
            or sequence[0] != AirbyteStreamStatus.STARTED
            or sequence[-1] != AirbyteStreamStatus.COMPLETE
            or any(status != AirbyteStreamStatus.RUNNING for status in sequence[1:-1])
        ):
            shown = ", ".join(status.value for status in sequence[:6])
            if len(sequence) > 6:
                shown += f" and {len(sequence) - 6} more"
            problems.append(
                f"- stream {_stream_label(*stream)!r} reported {shown}; expected `STARTED`, "
                "then only `RUNNING`, then `COMPLETE`."
            )
    return problems


def assert_error_trace_messages(messages: Iterable[AirbyteMessage]) -> None:
    """Assert that a failed command emitted at least one well-formed `ERROR` trace message.

    Ported from CAT's `TestBasicRead.test_airbyte_trace_message_on_failure`, which only required
    one `ERROR` trace. A trace is also well-formed here: it carries the `error` object, a
    non-blank `error.message` (what the platform shows the user), an `error.failure_type` (what
    the platform uses to tell a configuration problem from a system error) and a positive
    `emitted_at`. A `TRACE` line that does not parse as an Airbyte message never counts.

    The failure message names the trace messages by number and the fields they lack. It never
    prints their text, which may hold record or config values.
    """
    error_traces = [
        message.trace
        for message in messages
        if message.type == Type.TRACE
        and message.trace is not None
        and message.trace.type == TraceType.ERROR
    ]
    if not error_traces:
        raise AssertionError(
            "The failed read emitted no `TRACE` message of type `ERROR`. A connector must report "
            "why it fails as an error trace message: the CDK entrypoint does so for any "
            "exception that escapes `read`, so a connector without one bypasses the entrypoint "
            "or exits without raising."
        )

    problems: list[str] = []
    for number, trace in enumerate(error_traces):
        lacking: list[str] = []
        error = trace.error
        if error is None:
            lacking.append("the `error` object")
        else:
            if not isinstance(error.message, str) or not error.message.strip():
                lacking.append("a non-blank `error.message`")
            if error.failure_type is None:
                lacking.append("an `error.failure_type`")
        emitted_at = trace.emitted_at
        if (
            not isinstance(emitted_at, (int, float))
            or isinstance(emitted_at, bool)
            or emitted_at <= 0
        ):
            lacking.append("a positive `emitted_at`")
        if lacking:
            problems.append(f"- error trace #{number} lacks {', '.join(lacking)}.")
    if problems:
        raise AssertionError(
            f"{len(problems)} of {len(error_traces)} `ERROR` trace messages of the failed read "
            "are not well-formed:\n" + "\n".join(problems)
        )


INVALID_CONFIG_MARKER = "__airbyte_standard_tests_invalid_config"
"""The only key of the config that `make_invalid_config` returns."""


def make_invalid_config(connection_specification: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return a config that the connector's spec rejects, or `None` if none can be built.

    The config holds none of the spec's required top-level properties, and no value from any
    real config, so a connector that validates its config fails before it sends a request, and
    nothing secret can leak from it. `None` means the spec requires no top-level property, so
    an arbitrary config may be valid.
    """
    required = connection_specification.get("required")
    if not isinstance(required, list) or not required:
        return None
    return {INVALID_CONFIG_MARKER: True}


def assert_schema_validation_opt_out_allowed(
    *, validate_schema: bool, test_strictness_level: str | None
) -> None:
    """Reject `validate_schema: false` for a connector at `test_strictness_level: high`.

    CAT refused the opt-out at high strictness (`TestBasicRead.test_read`), so certified
    connectors cannot drop schema validation by config.
    """
    if validate_schema or test_strictness_level != "high":
        return
    raise AssertionError(
        "`validate_schema: false` is not allowed for a connector with "
        "`test_strictness_level: high` in `acceptance-test-config.yml` (CAT refused it too). Fix "
        "the stream schemas (or the records) so the records validate, and remove "
        "`validate_schema: false` from the config's `basic_read` entry; or lower the connector's "
        "`test_strictness_level`."
    )


def assert_read_records(
    *,
    records: Iterable[AirbyteMessage],
    configured_catalog: ConfiguredAirbyteCatalog,
    require_records_per_stream: bool,
    validate_schema: bool,
    validate_primary_keys: bool = False,
    state_messages: Iterable[AirbyteMessage] | None = None,
) -> None:
    """Assert the records a `read` returned against the configured catalog.

    The read must return at least one record, as any `read` expected to succeed must. The
    records are consumed once, so a lazy iterator over a large read is fine.

    Args:
        records: The `RECORD` messages from the read. Other messages are ignored.
        configured_catalog: The catalog the read ran with. Streams declared in `empty_streams`
            are expected to be excluded from it already.
        require_records_per_stream: Require at least one record from every configured stream.
        validate_schema: Validate every record against its stream's JSON schema.
        validate_primary_keys: Check the primary key of every record of a stream with a
            `source_defined_primary_key`: no key field holds an object or an array, and not
            every key field is missing or null.
        state_messages: The `STATE` messages from the read, to check that every configured
            stream emitted per-stream state with `sourceStats`; `None` skips the check.

    Raises:
        AssertionError: Listing every failure, by stream, with the path of the offending value
            inside the record. Record values are never printed.
    """
    record_counts: Counter[str] = Counter()
    checkers: dict[str, _StreamSchemaChecker] = {}
    if validate_schema:
        checkers = {
            configured_stream.stream.name: _StreamSchemaChecker(
                configured_stream.stream.name, configured_stream.stream.json_schema
            )
            for configured_stream in configured_catalog.streams
        }
    key_checkers: dict[str, _StreamPrimaryKeyChecker] = {}
    if validate_primary_keys:
        key_checkers = {
            configured_stream.stream.name: _StreamPrimaryKeyChecker(
                configured_stream.stream.name, configured_stream.stream.source_defined_primary_key
            )
            for configured_stream in configured_catalog.streams
            if configured_stream.stream.source_defined_primary_key
        }

    for message in records:
        record = message.record
        if message.type != Type.RECORD or record is None:
            continue
        record_number = record_counts[record.stream]
        record_counts[record.stream] += 1
        checker = checkers.get(record.stream)
        if checker is not None:
            checker.check(record.data)
        key_checker = key_checkers.get(record.stream)
        if key_checker is not None:
            key_checker.check(record.data, record_number)

    failures: list[str] = []
    if not record_counts:
        failures.append("Expected records but got none.")
    if require_records_per_stream:
        streams_without_records = [
            configured_stream.stream.name
            for configured_stream in configured_catalog.streams
            if not record_counts[configured_stream.stream.name]
        ]
        if streams_without_records:
            failures.append(
                "Every configured stream must return at least one record, but these returned "
                f"none: {', '.join(repr(name) for name in sorted(streams_without_records))}. "
                "If a stream is expected to be empty for this config, list it under "
                "`empty_streams` (with a `bypass_reason`) in the config's `basic_read` entry of "
                "`acceptance-test-config.yml`."
            )

    schema_failures = [
        description
        for checker in checkers.values()
        if (description := checker.describe_failures()) is not None
    ]
    if schema_failures:
        failures.append(
            "Records do not match their stream's JSON schema. Fix the schema or the records; to "
            "skip this check for a config, set `validate_schema: false` (with a reason) in its "
            "`basic_read` entry of `acceptance-test-config.yml`.\n" + "\n".join(schema_failures)
        )

    key_failures = [
        description
        for key_checker in key_checkers.values()
        if (description := key_checker.describe_failures()) is not None
    ]
    if key_failures:
        failures.append(
            "Records have invalid primary keys. Destinations deduplicate on the primary key, so "
            "every record needs a scalar value in at least one key field, and none in an object "
            "or an array. Fix the stream's `primary_key` or the records; to skip this check for "
            "a config, set `validate_primary_keys_data_type: false` (with a reason) in its "
            "`basic_read` entry of `acceptance-test-config.yml`.\n" + "\n".join(key_failures)
        )

    if state_messages is not None:
        state_failures = _find_state_message_failures(state_messages, configured_catalog)
        if state_failures:
            failures.append(
                "The read's state messages are incomplete. To skip this check for a config, set "
                "`validate_state_messages: false` (with a reason) in its `basic_read` entry of "
                "`acceptance-test-config.yml`.\n" + "\n".join(state_failures)
            )

    if failures:
        raise AssertionError("\n\n".join(failures))
