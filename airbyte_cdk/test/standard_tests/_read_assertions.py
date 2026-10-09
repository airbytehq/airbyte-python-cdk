# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Record assertions for the `read` standard tests.

These port the record checks of the Connector Acceptance Tests (CAT) `TestBasicRead.test_read`,
which the standard tests did not reproduce:

- every configured stream returns at least one record (CAT `_validate_empty_streams`);
- every record conforms to its stream's JSON schema (CAT `_validate_schema`), with the same
  validator CAT ran: Draft 7 with strict integers and a lenient `date-time` format check, and
  without failing on extra columns.

They take the read output and the configured catalog rather than a scenario, so the in-process
and the Docker read tests can share them.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from jsonschema import Draft7Validator, FormatChecker, validators
from jsonschema.exceptions import FormatError, UnknownType, ValidationError, best_match
from referencing.exceptions import Unresolvable

from airbyte_cdk.models import AirbyteMessage, ConfiguredAirbyteCatalog
from airbyte_cdk.utils.datetime_helpers import ab_datetime_try_parse

MAX_SCHEMA_ERRORS_PER_STREAM = 5
"""How many distinct schema errors are reported per stream. The total is always reported."""

_MAX_MESSAGE_CHARS = 300
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


def assert_read_records(
    *,
    records: Iterable[AirbyteMessage],
    configured_catalog: ConfiguredAirbyteCatalog,
    require_records_per_stream: bool,
    validate_schema: bool,
) -> None:
    """Assert the records a `read` returned against the configured catalog.

    Args:
        records: The `RECORD` messages from the read.
        configured_catalog: The catalog the read ran with. Streams declared in `empty_streams`
            are expected to be excluded from it already.
        require_records_per_stream: Require at least one record from every configured stream.
        validate_schema: Validate every record against its stream's JSON schema.

    Raises:
        AssertionError: Listing every failure, by stream, with the path of the offending value
            inside the record.
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

    for message in records:
        record = message.record
        if record is None:
            continue
        record_counts[record.stream] += 1
        checker = checkers.get(record.stream)
        if checker is not None:
            checker.check(record.data)

    failures: list[str] = []
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

    if failures:
        raise AssertionError("\n\n".join(failures))
