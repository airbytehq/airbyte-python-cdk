# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Assertions on the catalog a source returns from `discover`.

These restore the `TestDiscovery` assertions of the retired Connector Acceptance Tests (CAT).
Each check takes the discovered catalog and returns a list of problems, one string per
offending stream or field, so that a single test run reports everything that is wrong.

Checks marked as not enforced report their problems as a `DiscoveredCatalogWarning` instead of
failing the test. They are not enforced yet because connectors in the fleet still violate them.
Run pytest with `-W error::airbyte_cdk.test.standard_tests.DiscoveredCatalogWarning` to make them
fail locally.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import jsonschema
from jsonschema.protocols import Validator

from airbyte_cdk.models import AirbyteCatalog

# Keywords whose value maps names to subschemas.
_SCHEMA_MAP_KEYWORDS = frozenset(
    {"properties", "patternProperties", "definitions", "$defs", "dependentSchemas"}
)
# Keywords whose value is a list of subschemas.
_SCHEMA_LIST_KEYWORDS = frozenset({"allOf", "anyOf", "oneOf", "prefixItems"})
# Keywords whose value is a single subschema. `items` may also hold a list of subschemas.
_SCHEMA_KEYWORDS = frozenset(
    {
        "additionalItems",
        "additionalProperties",
        "contains",
        "else",
        "if",
        "items",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)

UNSUPPORTED_KEYWORDS = ("allOf", "not")
"""JSON Schema keywords that Airbyte destinations do not support in stream schemas."""

SCALAR_PRIMARY_KEY_FORBIDDEN_TYPES = frozenset({"object", "array"})

SUPPORTED_AIRBYTE_TYPES: Mapping[str, frozenset[str]] = {
    "timestamp_with_timezone": frozenset({"string"}),
    "timestamp_without_timezone": frozenset({"string"}),
    "time_with_timezone": frozenset({"string"}),
    "time_without_timezone": frozenset({"string"}),
    "integer": frozenset({"integer", "number"}),
}
"""Each `airbyte_type` from the Airbyte type system, mapped to the JSON types it can annotate.

See https://docs.airbyte.com/platform/understanding-airbyte/supported-data-types
"""

TEMPORAL_FORMATS = frozenset({"date", "date-time", "time"})
"""String formats that the Airbyte type system maps to temporal types."""


class DiscoveredCatalogWarning(UserWarning):
    """A problem in the discovered catalog that the standard tests report but do not enforce.

    To make these problems fail the test, turn the warning into an error, either with
    `pytest -W error::airbyte_cdk.test.standard_tests.DiscoveredCatalogWarning` or with
    `airbyte-cdk connector test --pytest-arg=-Werror::airbyte_cdk.test.standard_tests.DiscoveredCatalogWarning`.
    """


def _pointer(path: Sequence[str]) -> str:
    """Render a schema path as a JSON pointer fragment, for example `#/properties/id`."""
    escaped = (segment.replace("~", "~0").replace("/", "~1") for segment in path)
    return "#/" + "/".join(escaped) if path else "#"


def _iter_subschemas(
    schema: Any,
    path: tuple[str, ...] = (),
) -> Iterator[tuple[tuple[str, ...], Mapping[str, Any]]]:
    """Yield every subschema of `schema` with its path, starting with `schema` itself.

    Only keywords that hold subschemas are followed. Property names, and values such as
    `default`, `examples` and `enum`, are never mistaken for keywords.
    """
    if not isinstance(schema, Mapping):
        # Boolean schemas have no keywords.
        return

    yield path, schema
    for keyword, value in schema.items():
        if keyword in _SCHEMA_MAP_KEYWORDS and isinstance(value, Mapping):
            for name, subschema in value.items():
                yield from _iter_subschemas(subschema, (*path, keyword, str(name)))
        elif keyword == "dependencies" and isinstance(value, Mapping):
            # Values are either subschemas or lists of property names.
            for name, subschema in value.items():
                yield from _iter_subschemas(subschema, (*path, keyword, str(name)))
        elif isinstance(value, list) and (keyword in _SCHEMA_LIST_KEYWORDS or keyword == "items"):
            for index, subschema in enumerate(value):
                yield from _iter_subschemas(subschema, (*path, keyword, str(index)))
        elif keyword in _SCHEMA_KEYWORDS:
            yield from _iter_subschemas(value, (*path, keyword))


def _json_types(schema: Mapping[str, Any]) -> set[str]:
    """Return the JSON types a schema declares, as a set."""
    declared = schema.get("type")
    if isinstance(declared, str):
        return {declared}
    if isinstance(declared, list):
        return {item for item in declared if isinstance(item, str)}
    return set()


def _json_types_with_alternatives(schema: Mapping[str, Any]) -> set[str]:
    """Return the JSON types a schema declares, including those of its `anyOf`/`oneOf` branches.

    A value typed as `{"anyOf": [{"type": "string"}, {"type": "null"}]}` can be any of the
    branch types, so they are added to the types declared on the node itself.
    """
    types = _json_types(schema)
    for keyword in ("anyOf", "oneOf"):
        branches = schema.get(keyword)
        if isinstance(branches, list):
            for branch in branches:
                if isinstance(branch, Mapping):
                    types |= _json_types_with_alternatives(branch)
    return types


def _meta_validator(schema: Mapping[str, Any]) -> Validator:
    """Return a validator for the meta-schema of the draft that `schema` declares in `$schema`.

    Draft 7 is used when `$schema` is absent, not a string, or names an unknown draft.
    """
    validator_cls: type[Validator] = jsonschema.Draft7Validator
    if isinstance(schema.get("$schema"), str):
        validator_cls = jsonschema.validators.validator_for(schema, default=validator_cls)
    return validator_cls(validator_cls.META_SCHEMA)


_MISSING = object()


def _find_field(schema: Mapping[str, Any], field_path: Sequence[str]) -> Any:
    """Return the subschema of a (possibly nested) field, or `_MISSING` if the schema lacks it."""
    node: Any = schema
    for segment in field_path:
        properties = node.get("properties") if isinstance(node, Mapping) else None
        if not isinstance(properties, Mapping) or segment not in properties:
            return _MISSING
        node = properties[segment]
    return node


def _field_name(field_path: Sequence[str]) -> str:
    return ".".join(field_path)


def check_catalog_has_streams(catalog: AirbyteCatalog) -> list[str]:
    """The catalog declares at least one stream."""
    if catalog.streams:
        return []
    return ["The catalog does not contain any streams."]


def check_stream_names_are_unique(catalog: AirbyteCatalog) -> list[str]:
    """No two streams share the same namespace and name."""
    counts = Counter((stream.namespace, stream.name) for stream in catalog.streams)
    return [
        f"Stream '{name}'"
        + (f" in namespace '{namespace}'" if namespace else "")
        + f" is declared {count} times."
        for (namespace, name), count in counts.items()
        if count > 1
    ]


def check_streams_declare_sync_modes(catalog: AirbyteCatalog) -> list[str]:
    """Every stream declares at least one supported sync mode."""
    return [
        f"Stream '{stream.name}' does not declare any supported sync modes."
        for stream in catalog.streams
        if not stream.supported_sync_modes
    ]


def check_schemas_are_valid_json_schema(catalog: AirbyteCatalog) -> list[str]:
    """Every stream schema is a valid JSON Schema.

    Each schema is validated against the meta-schema of the draft it declares in `$schema`,
    or Draft 7 when it declares none. Every violation is reported, ordered by location, rather
    than the single best match that `check_schema` raises, so that fixing one does not reveal
    another on the next run.
    """
    problems: list[str] = []
    for stream in catalog.streams:
        errors = sorted(
            _meta_validator(stream.json_schema).iter_errors(stream.json_schema),
            key=lambda error: [str(part) for part in error.absolute_path],
        )
        problems.extend(
            f"Stream '{stream.name}' has an invalid JSON schema at "
            f"{_pointer([str(part) for part in error.absolute_path])}: {error.message}"
            for error in errors
        )
    return problems


def check_cursor_fields_exist_in_schema(catalog: AirbyteCatalog) -> list[str]:
    """A declared default cursor field exists in the stream schema."""
    problems = []
    for stream in catalog.streams:
        if not stream.default_cursor_field:
            continue
        if _find_field(stream.json_schema, stream.default_cursor_field) is _MISSING:
            problems.append(
                f"Stream '{stream.name}' declares cursor field "
                f"'{_field_name(stream.default_cursor_field)}', "
                "which is not a property in the stream schema."
            )
    return problems


def check_primary_keys_exist_in_schema(catalog: AirbyteCatalog) -> list[str]:
    """Every field of a declared primary key exists in the stream schema."""
    problems = []
    for stream in catalog.streams:
        for key_path in stream.source_defined_primary_key or []:
            if _find_field(stream.json_schema, key_path) is _MISSING:
                problems.append(
                    f"Stream '{stream.name}' declares primary key field "
                    f"'{_field_name(key_path)}', which is not a property in the stream schema."
                )
    return problems


def check_primary_keys_have_scalar_types(catalog: AirbyteCatalog) -> list[str]:
    """No field of a declared primary key is typed as an object or an array.

    Fields missing from the schema are reported by `check_primary_keys_exist_in_schema`.
    """
    problems = []
    for stream in catalog.streams:
        for key_path in stream.source_defined_primary_key or []:
            field_schema = _find_field(stream.json_schema, key_path)
            if not isinstance(field_schema, Mapping):
                continue
            forbidden = _json_types(field_schema) & SCALAR_PRIMARY_KEY_FORBIDDEN_TYPES
            if forbidden:
                problems.append(
                    f"Stream '{stream.name}' declares primary key field "
                    f"'{_field_name(key_path)}' with type {sorted(_json_types(field_schema))}. "
                    "Primary key fields must not be objects or arrays."
                )
    return problems


def check_refs_are_resolved(catalog: AirbyteCatalog) -> list[str]:
    """Stream schemas contain no unresolved `$ref`."""
    return [
        f"Stream '{stream.name}' has an unresolved $ref '{subschema['$ref']}' at {_pointer(path)}."
        for stream in catalog.streams
        for path, subschema in _iter_subschemas(stream.json_schema)
        if "$ref" in subschema
    ]


def check_no_unsupported_keywords(catalog: AirbyteCatalog) -> list[str]:
    """Stream schemas use none of the keywords in `UNSUPPORTED_KEYWORDS`."""
    return [
        f"Stream '{stream.name}' uses the unsupported keyword '{keyword}' at {_pointer(path)}."
        for stream in catalog.streams
        for path, subschema in _iter_subschemas(stream.json_schema)
        for keyword in UNSUPPORTED_KEYWORDS
        if keyword in subschema
    ]


def check_additional_properties_not_false(catalog: AirbyteCatalog) -> list[str]:
    """No stream schema sets `additionalProperties` to false.

    A closed schema turns the removal of a property into a breaking change for existing
    connections. See https://github.com/airbytehq/airbyte/issues/14196.
    """
    return [
        f"Stream '{stream.name}' sets additionalProperties to false at {_pointer(path)}."
        for stream in catalog.streams
        for path, subschema in _iter_subschemas(stream.json_schema)
        if subschema.get("additionalProperties") is False
    ]


def check_supported_data_types(catalog: AirbyteCatalog) -> list[str]:
    """Stream schemas stay within the Airbyte type system.

    The top level is an object, every `airbyte_type` is a known one and annotates a compatible
    JSON type, and temporal formats annotate strings. The types of `anyOf`/`oneOf` branches
    count as types of the annotated node, and an annotation on a node that declares no type at
    all is not checked. Unknown JSON type names are already rejected by
    `check_schemas_are_valid_json_schema`.
    """
    problems = []
    for stream in catalog.streams:
        top_level_types = _json_types(stream.json_schema)
        if "object" not in top_level_types:
            problems.append(
                f"Stream '{stream.name}' has a top-level schema of type "
                f"{sorted(top_level_types)}; it must be an object."
            )
        for path, subschema in _iter_subschemas(stream.json_schema):
            json_types = _json_types_with_alternatives(subschema)
            airbyte_type = subschema.get("airbyte_type")
            if airbyte_type is not None:
                compatible_types = (
                    SUPPORTED_AIRBYTE_TYPES.get(airbyte_type)
                    if isinstance(airbyte_type, str)
                    else None
                )
                if compatible_types is None:
                    problems.append(
                        f"Stream '{stream.name}' uses the unknown airbyte_type "
                        f"'{airbyte_type}' at {_pointer(path)}."
                    )
                elif json_types and not json_types & compatible_types:
                    problems.append(
                        f"Stream '{stream.name}' uses airbyte_type '{airbyte_type}' on type "
                        f"{sorted(json_types)} at {_pointer(path)}; it requires one of "
                        f"{sorted(compatible_types)}."
                    )
            string_format = subschema.get("format")
            if (
                isinstance(string_format, str)
                and string_format in TEMPORAL_FORMATS
                and json_types
                and "string" not in json_types
            ):
                problems.append(
                    f"Stream '{stream.name}' uses format '{string_format}' on type "
                    f"{sorted(json_types)} at {_pointer(path)}; it requires a string."
                )
    return problems


@dataclass(frozen=True)
class CatalogCheck:
    """A named assertion on the discovered catalog."""

    name: str
    run: Callable[[AirbyteCatalog], list[str]]
    enforced: bool = True


CATALOG_CHECKS: tuple[CatalogCheck, ...] = (
    CatalogCheck("catalog has streams", check_catalog_has_streams),
    CatalogCheck("stream names are unique", check_stream_names_are_unique),
    CatalogCheck("streams declare sync modes", check_streams_declare_sync_modes),
    CatalogCheck("refs are resolved", check_refs_are_resolved),
    CatalogCheck("no unsupported keywords", check_no_unsupported_keywords),
    CatalogCheck("additionalProperties is not false", check_additional_properties_not_false),
    # Not enforced until the manifest-only connectors that violate them are fixed.
    CatalogCheck(
        "schemas are valid JSON schema", check_schemas_are_valid_json_schema, enforced=False
    ),
    CatalogCheck(
        "cursor fields exist in schema", check_cursor_fields_exist_in_schema, enforced=False
    ),
    CatalogCheck(
        "primary keys exist in schema", check_primary_keys_exist_in_schema, enforced=False
    ),
    CatalogCheck(
        "primary keys have scalar types", check_primary_keys_have_scalar_types, enforced=False
    ),
    CatalogCheck("supported data types", check_supported_data_types, enforced=False),
)


def find_catalog_problems(
    catalog: AirbyteCatalog,
    checks: Sequence[CatalogCheck] = CATALOG_CHECKS,
) -> dict[CatalogCheck, list[str]]:
    """Run each check against the catalog and return the problems of every failing check.

    A check that raises is reported as a problem of that check, so that a check which is not
    enforced yet cannot fail the test by crashing on a schema it did not anticipate.
    """
    results: dict[CatalogCheck, list[str]] = {}
    for check in checks:
        try:
            problems = check.run(catalog)
        except Exception as error:  # Reported, not raised; see the docstring.
            problems = [f"The check could not run: {type(error).__name__}: {error}"]
        if problems:
            results[check] = problems
    return results


def format_catalog_problems(problems: Mapping[CatalogCheck, list[str]]) -> str:
    """Render problems grouped by check, for an assertion or warning message."""
    sections = [
        f"[{check.name}]\n" + "\n".join(f"  - {problem}" for problem in check_problems)
        for check, check_problems in problems.items()
    ]
    return "\n".join(sections)
