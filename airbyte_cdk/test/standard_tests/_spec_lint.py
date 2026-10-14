# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Credential-free assertions on a connector's spec.

These checks need only the connector's SPEC message, plus a scenario's config and the
connector's output for the secret leakage check. That lets them run in both standard-test
paths: in-process, and against the connector image.
"""

from __future__ import annotations

import json
import re
import warnings
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteMessageSerializer,
    ConnectorSpecification,
    ConnectorSpecificationSerializer,
    Type,
)
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.standard_tests._catalog_checks import _iter_subschemas, _meta_validator

SECRET_PROPERTY_NAMES = frozenset(
    {
        "access_token",
        "api_token",
        "certificate",
        "client_secret",
        "client_token",
        "credentials",
        "jwt",
        "key",
        "password",
        "refresh_token",
        "secret",
        "service_account",
        "service_account_info",
        "token",
    }
)
"""Property names that always denote a secret.

Ported from the connector acceptance tests (CAT), minus `tenant_id`, `app_id` and `appid`:
those identify an account or an app rather than authenticate it.
"""

SECRET_PROPERTY_NAME_SUFFIXES = (
    "_token",
    "_secret",
    "password",
    "api_key",
    "apikey",
    "secret_key",
    "private_key",
    "access_key",
)
"""Name suffixes that denote a secret, such as `api_key` or `personal_access_token`.

CAT matched exact names only, so it never flagged an unmarked `api_key`, the most common
secret property name in the connector fleet.

An `access_key` that sits next to a `secret` or `secret_key` property is exempt: it is the
public half of a key pair, like an AWS access key ID (see `is_secret_property_name`).
"""

_ACCESS_KEY_SUFFIX = "access_key"
_KEY_PAIR_SECRET_SUFFIXES = ("_secret", "secret_key")

_SECRET_CAPABLE_TYPES = {"string", "integer", "number"}

_COMBINATORS = ("oneOf", "anyOf", "allOf")

_UNMASKED_COMBINATORS = ("anyOf", "allOf")
"""Combinators that the CDK's runtime secret filter does not look through.

`airbyte_cdk.utils.airbyte_secrets_utils.get_secret_paths` drops only `properties` and `oneOf`
from the schema path of an `airbyte_secret` flag and looks the rest up in the config. A flag
under `anyOf` or `allOf` therefore resolves to no config value, and the secret is never masked
in logs. `get_secrets` also starts at the top-level `properties`, so no combinator at the root
of the spec is looked through either.
"""

_MIN_LEAK_CHECK_LENGTH = 8
"""Secret values shorter than this are not searched for in the output.

Short values, such as `123` in a test config, occur in ordinary output by coincidence.
"""


def _normalize_property_name(name: str) -> str:
    """Return `name` in snake case: `clientSecret` and `client-secret` become `client_secret`."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower().replace("-", "_")


def _is_key_pair_secret_name(normalized_name: str) -> bool:
    return normalized_name == "secret" or normalized_name.endswith(_KEY_PAIR_SECRET_SUFFIXES)


def is_secret_property_name(name: str, sibling_names: Iterable[str] = ()) -> bool:
    """Return whether a spec property with this name is expected to hold a secret.

    Names are compared in snake case, so `clientSecret`, `client-secret` and `Client_Secret`
    all match `client_secret`.

    `sibling_names` are the names of the other properties of the same object. An
    `*access_key` with a sibling such as `app_secret` or `secret_key` is the public half of
    a key pair: it identifies the key rather than authenticates with it, so it is not
    expected to be secret. An `*access_key` on its own is the credential, as with the many
    APIs that take a single `access_key` parameter.
    """
    normalized_name = _normalize_property_name(name)
    if normalized_name in SECRET_PROPERTY_NAMES:
        return True
    if not normalized_name.endswith(SECRET_PROPERTY_NAME_SUFFIXES):
        return False
    if normalized_name.endswith(_ACCESS_KEY_SUFFIX) and not normalized_name.endswith(
        f"secret_{_ACCESS_KEY_SUFFIX}"
    ):
        return not any(
            _is_key_pair_secret_name(_normalize_property_name(sibling))
            for sibling in sibling_names
            if sibling != name
        )
    return True


def _variants(schema: Mapping[str, Any], keywords: Iterable[str] = _COMBINATORS) -> Iterator[Any]:
    for keyword in keywords:
        variants = schema.get(keyword)
        if isinstance(variants, list):
            yield from (variant for variant in variants if isinstance(variant, Mapping))


def _declared_types(schema: Mapping[str, Any]) -> set[str]:
    """Return the types `schema` declares, itself or through its combinator variants.

    A pydantic-v2 `Optional[str]` field declares its types only under `anyOf`.
    """
    property_type = schema.get("type")
    if isinstance(property_type, str):
        return {property_type}
    if isinstance(property_type, list):
        return {str(item) for item in property_type}
    return set().union(*(_declared_types(variant) for variant in _variants(schema)))


def _can_hold_secret(schema: Mapping[str, Any]) -> bool:
    """Return whether a property can hold a secret value.

    Booleans and nulls cannot, and the UI cannot render secret objects or arrays. A property
    with a `const` value cannot either, since every config holds the same value. A property
    that declares its types only under a combinator can hold a secret if any variant can.
    """
    if "const" in schema:
        return False
    if "type" in schema:
        return bool(_declared_types(schema) & _SECRET_CAPABLE_TYPES)
    return any(_can_hold_secret(variant) for variant in _variants(schema))


def _is_marked_secret(schema: Mapping[str, Any]) -> bool:
    """Return whether the runtime secret filter masks the value of this property.

    The flag counts on the property itself, or on a `oneOf` variant of it, because the
    filter drops `oneOf` from the schema path. A flag under `anyOf` or `allOf` does not count
    (see `_UNMASKED_COMBINATORS`).
    """
    if schema.get("airbyte_secret") is True:
        return True
    return any(_is_marked_secret(variant) for variant in _variants(schema, ("oneOf",)))


@dataclass(frozen=True)
class _SpecProperty:
    pointer: str
    """JSON pointer of this schema in the connection specification."""
    name: str
    """Name of the property this schema belongs to."""
    sibling_names: frozenset[str]
    """Names of every property of the object that declares the property, itself included."""
    schema: Mapping[str, Any]
    property_pointer: str
    """JSON pointer of the property itself, which differs from `pointer` for a variant."""
    is_variant: bool
    """Whether this schema is a `oneOf`/`anyOf`/`allOf` variant of the property."""
    unmasked_under: str | None
    """The first combinator on the path that the runtime secret filter does not look through."""
    unmasked_at_root: bool
    """Whether `unmasked_under` sits at the root of the spec, which the filter never reads."""


def _iter_spec_properties(
    schema: Mapping[str, Any],
    *,
    pointer: str = "",
    name: str | None = None,
    sibling_names: frozenset[str] = frozenset(),
    property_pointer: str = "",
    is_variant: bool = False,
    unmasked_under: str | None = None,
    unmasked_at_root: bool = False,
) -> Iterator[_SpecProperty]:
    """Yield every property of `schema`, and every combinator variant of each property.

    The `items` of an array are yielded as the property itself, under its name and siblings,
    since an array of secret strings marks its items.
    """
    if name is not None:
        yield _SpecProperty(
            pointer,
            name,
            sibling_names,
            schema,
            property_pointer,
            is_variant,
            unmasked_under,
            unmasked_at_root,
        )

    properties = schema.get("properties")
    if isinstance(properties, Mapping):
        property_names = frozenset(properties)
        for property_name, property_schema in properties.items():
            if isinstance(property_schema, Mapping):
                child_pointer = f"{pointer}/properties/{property_name}"
                yield from _iter_spec_properties(
                    property_schema,
                    pointer=child_pointer,
                    name=property_name,
                    sibling_names=property_names,
                    property_pointer=child_pointer,
                    unmasked_under=unmasked_under,
                    unmasked_at_root=unmasked_at_root,
                )

    for keyword in _COMBINATORS:
        variants = schema.get(keyword)
        if not isinstance(variants, list):
            continue
        at_root = name is None
        hides_secrets = unmasked_under is None and (keyword in _UNMASKED_COMBINATORS or at_root)
        for index, variant in enumerate(variants):
            if isinstance(variant, Mapping):
                yield from _iter_spec_properties(
                    variant,
                    pointer=f"{pointer}/{keyword}/{index}",
                    name=name,
                    sibling_names=sibling_names,
                    property_pointer=property_pointer,
                    is_variant=not at_root,
                    unmasked_under=keyword if hides_secrets else unmasked_under,
                    unmasked_at_root=at_root if hides_secrets else unmasked_at_root,
                )

    items = schema.get("items")
    if isinstance(items, Mapping):
        yield from _iter_spec_properties(
            items,
            pointer=f"{pointer}/items",
            name=name,
            sibling_names=sibling_names,
            property_pointer=f"{pointer}/items",
            unmasked_under=unmasked_under,
            unmasked_at_root=unmasked_at_root,
        )


def _describe_types(schema: Mapping[str, Any]) -> str:
    return ", ".join(sorted(_declared_types(schema))) or "none"


def find_secret_marking_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per property whose `airbyte_secret` flag is wrong.

    Any `airbyte_secret` value must be a boolean: the CDK's secret filter only masks values
    whose flag is `True`, so a string such as `"true"` leaves the value unmasked. The flag
    must also sit where the filter finds it, so not under `anyOf` or `allOf` (see
    `_UNMASKED_COMBINATORS`).

    A property whose name denotes a secret (see `is_secret_property_name`) and that can hold
    a secret value must set `airbyte_secret: true`, on itself or on a `oneOf` variant. One
    that cannot hold a secret value, such as a boolean or an object, must not set it.

    A property that sets `airbyte_secret: false` explicitly is not reported as an unmarked
    secret: that is how a spec records that a secret-looking name, such as a sort `key`,
    holds no secret.
    """
    errors: list[str] = []
    for spec_property in _iter_spec_properties(connection_specification):
        pointer = spec_property.pointer
        property_schema = spec_property.schema
        marking = property_schema.get("airbyte_secret")
        if marking is not None and not isinstance(marking, bool):
            errors.append(
                f"`{pointer}` sets `airbyte_secret` to `{json.dumps(marking)}`, which is not a "
                "boolean, so the CDK does not treat the value as a secret."
            )
            continue
        if marking is True and spec_property.unmasked_under:
            if spec_property.is_variant:
                fix = f"Set it on `{spec_property.property_pointer}` instead."
            elif spec_property.unmasked_at_root:
                fix = "The filter reads only the top-level `properties` of the spec."
            else:
                combinator = spec_property.unmasked_under
                fix = f"Declare the variants with `oneOf` instead of `{combinator}`."
            errors.append(
                f"`{pointer}` sets `airbyte_secret: true` under "
                f"`{spec_property.unmasked_under}`, where the CDK's secret filter does not look, "
                f"so the value is not masked in logs. {fix}"
            )
            continue
        if spec_property.is_variant:
            # The property that declares the variant is checked as a whole.
            continue
        if not _declared_types(property_schema) or not is_secret_property_name(
            spec_property.name, spec_property.sibling_names
        ):
            continue

        marked_as_secret = _is_marked_secret(property_schema)
        can_hold_secret = _can_hold_secret(property_schema)
        if can_hold_secret and not marked_as_secret:
            if marking is False or _has_unmasked_variant_marking(property_schema):
                # An explicit `false` is a reviewed decision. A flag under `anyOf` is reported
                # on the variant above.
                continue
            errors.append(
                f"`{pointer}` looks like a secret but is not marked `airbyte_secret: true`. "
                "If it holds no secret, set `airbyte_secret: false` on it explicitly."
            )
        elif marked_as_secret and not can_hold_secret:
            errors.append(
                f"`{pointer}` is marked `airbyte_secret: true` but its type "
                f"`{_describe_types(property_schema)}` cannot hold a secret value."
            )
    return errors


def _has_unmasked_variant_marking(schema: Mapping[str, Any], under_unmasked: bool = False) -> bool:
    """Return whether a variant of `schema` sets the flag under `anyOf` or `allOf`."""
    for keyword in _COMBINATORS:
        unmasked = under_unmasked or keyword in _UNMASKED_COMBINATORS
        for variant in _variants(schema, (keyword,)):
            if unmasked and variant.get("airbyte_secret") is True:
                return True
            if _has_unmasked_variant_marking(variant, unmasked):
                return True
    return False


# Structural, presentational and OAuth lints, ported from CAT's `TestSpec`.


class SpecLintWarning(UserWarning):
    """A spec problem that the standard tests report but do not enforce yet.

    To make these problems fail the test, turn the warning into an error, either with
    `pytest -W error::airbyte_cdk.test.standard_tests.SpecLintWarning` or with
    `airbyte-cdk connector test --pytest-arg=-Werror::airbyte_cdk.test.standard_tests.SpecLintWarning`.
    """


SPEC_REFERENCE_URL = (
    "https://docs.airbyte.com/platform/connector-development/connector-specification-reference"
)

FORBIDDEN_SPEC_KEYWORDS = (
    "not",
    "anyOf",
    "patternProperties",
    "prefixItems",
    "allOf",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "dependentRequired",
)
"""JSON Schema keywords that the connector setup form cannot render."""

ARRAY_ITEM_TYPES = frozenset({"object", "string", "number", "integer"})
"""Item types the connector setup form can render as an array field, besides an `enum`."""

DISPLAY_TYPES = frozenset({"dropdown", "radio"})
"""Values `display_type` may take on a field with `oneOf` variants."""


def _schema_nodes(schema: Any) -> Iterator[tuple[tuple[str, ...], Mapping[str, Any]]]:
    """Yield `(path, subschema)` for `schema` and every subschema in it.

    Only keywords that hold subschemas are followed, so a property named `type`, `enum` or
    `not` is never mistaken for a keyword, and values such as `default` or `examples` are
    never read as schemas.
    """
    yield from _iter_subschemas(schema)


def _spec_pointer(path: Iterable[str]) -> str:
    """Render a schema path as the JSON pointer used in failure messages, `` `/` `` at the root."""
    return "`/" + "/".join(_pointer_token(segment) for segment in path) + "`"


def _is_property(path: tuple[str, ...]) -> bool:
    return len(path) >= 2 and path[-2] == "properties"


def _is_single_valued(schema: Any) -> bool:
    """Return whether a property can hold one value only: a `const`, or an `enum` of one item."""
    if not isinstance(schema, Mapping):
        return False
    enum = schema.get("enum")
    return "const" in schema or (isinstance(enum, list) and len(enum) == 1)


def find_oneof_usage_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per `oneOf` that the setup form cannot render as a choice of variants.

    Ported from CAT's `test_oneof_usage`. The schema that declares `oneOf` must be an object,
    and every variant must be an object with `properties`. The variants must share exactly one
    discriminator: a property that sets `const` in every variant or, failing that, one that
    sets a single-item `enum` in every variant. The setup form picks the variant by that
    value, so a `const` discriminator may set `default` and `enum` only to the same value, an
    `enum` discriminator must set its one item as `default` (the form reads it as the
    `const`), and no two variants may share a value.

    Unlike CAT, a property whose `enum` has several items is not a discriminator candidate,
    since it cannot identify a variant (such as a `compression_codec` next to `format_type`).
    """
    errors: list[str] = []
    for path, schema in _schema_nodes(connection_specification):
        variants = schema.get("oneOf")
        if not isinstance(variants, list):
            continue
        where = _spec_pointer((*path, "oneOf"))
        if schema.get("type") != "object":
            errors.append(
                f"{where} is declared on a schema that is not `type: object`; the setup form "
                "renders `oneOf` only as a choice between object variants."
            )
        variant_properties: list[Mapping[str, Any]] = []
        for index, variant in enumerate(variants):
            properties = variant.get("properties") if isinstance(variant, Mapping) else None
            if not isinstance(properties, Mapping):
                errors.append(
                    f"{_spec_pointer((*path, 'oneOf', str(index)))} has no `properties`; every "
                    "`oneOf` variant must be an object with properties."
                )
                continue
            variant_properties.append(properties)
        if not variants or len(variant_properties) != len(variants):
            continue

        common = set.intersection(*(set(properties) for properties in variant_properties))
        candidates = sorted(
            name
            for name in common
            if all(_is_single_valued(properties[name]) for properties in variant_properties)
        )
        const_candidates = [
            name
            for name in candidates
            if all("const" in properties[name] for properties in variant_properties)
        ]
        if len(const_candidates) == 1:
            discriminator = const_candidates[0]
        elif not const_candidates and len(candidates) == 1:
            discriminator = candidates[0]
        else:
            found = const_candidates or candidates
            detail = ", ".join(f"`{name}`" for name in found) if found else "none"
            errors.append(
                f"{where} variants must share exactly one property that sets `const` in every "
                f"variant, so the setup form can tell them apart (found: {detail})."
            )
            continue

        values: list[Any] = []
        for index, properties in enumerate(variant_properties):
            discriminator_schema = properties[discriminator]
            discriminator_where = _spec_pointer(
                (*path, "oneOf", str(index), "properties", discriminator)
            )
            if "const" in discriminator_schema:
                value = discriminator_schema["const"]
                if "default" in discriminator_schema and not _same_json(
                    discriminator_schema["default"], value
                ):
                    errors.append(
                        f"{discriminator_where} sets a `default` that differs from its `const`."
                    )
                if "enum" in discriminator_schema and not _same_json(
                    discriminator_schema["enum"], [value]
                ):
                    errors.append(
                        f"{discriminator_where} sets an `enum` other than a single item equal "
                        "to its `const`."
                    )
            elif "default" in discriminator_schema and _same_json(
                discriminator_schema["enum"], [discriminator_schema["default"]]
            ):
                value = discriminator_schema["default"]
            else:
                errors.append(
                    f"{discriminator_where} must set `const`, or set its single `enum` item as "
                    "its `default`."
                )
                continue
            values.append(value)
        if any(_count_json(values, value) > 1 for value in values):
            errors.append(
                f"{where} variants share a value of the discriminator `{discriminator}`, so "
                "the setup form cannot tell them apart."
            )
    return errors


def _same_json(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True) == json.dumps(right, sort_keys=True)


def _count_json(values: list[Any], value: Any) -> int:
    return sum(_same_json(item, value) for item in values)


def find_enum_usage_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per `enum` that lists the same value more than once.

    Ported from CAT's `test_enum_usage`.
    """
    errors: list[str] = []
    for path, schema in _schema_nodes(connection_specification):
        enum = schema.get("enum")
        if not isinstance(enum, list):
            continue
        seen: dict[str, int] = {}
        for index, value in enumerate(enum):
            key = json.dumps(value, sort_keys=True)
            if key in seen:
                errors.append(
                    f"{_spec_pointer((*path, 'enum'))} lists the same value at positions "
                    f"{seen[key]} and {index}."
                )
            else:
                seen[key] = index
    return errors


def find_forbidden_keyword_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per JSON Schema keyword that the setup form cannot render.

    Ported from CAT's `test_forbidden_complex_types`: see `FORBIDDEN_SPEC_KEYWORDS`.
    """
    return [
        f"{_spec_pointer((*path, keyword))} uses `{keyword}`, which the connector setup form "
        "does not support."
        for path, schema in _schema_nodes(connection_specification)
        for keyword in FORBIDDEN_SPEC_KEYWORDS
        if keyword in schema
    ]


def find_array_type_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per array whose items the setup form cannot render.

    Ported from CAT's `test_array_type`: `items` must be a single schema, of type `object`,
    `string`, `number` or `integer`, or with an `enum`.
    """
    errors: list[str] = []
    for path, schema in _schema_nodes(connection_specification):
        if schema.get("type") != "array" or "items" not in schema:
            continue
        items = schema["items"]
        where = _spec_pointer((*path, "items"))
        if not isinstance(items, Mapping):
            errors.append(f"{where} must be a single schema, not a list of item schemas.")
        elif "enum" in items:
            continue
        elif not isinstance(items.get("type"), str) or items["type"] not in ARRAY_ITEM_TYPES:
            errors.append(
                f"{where} must be of type `object`, `string`, `number` or `integer`, or set "
                "an `enum`."
            )
    return errors


def find_empty_object_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per object that declares no properties and no `oneOf` variants.

    Ported from CAT's `test_object_not_empty`: the setup form shows nothing for such an object.
    The root schema may be an empty object, for a connector that takes no configuration.
    """
    return [
        f"{_spec_pointer(path)} is an object with no `properties` and no `oneOf`, so the setup "
        "form cannot show it. Add its properties or remove it."
        for path, schema in _schema_nodes(connection_specification)
        if path
        and schema.get("type") == "object"
        and "oneOf" not in schema
        and not schema.get("properties")
    ]


def find_property_type_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per property whose `type` lists types the setup form cannot render.

    Ported from CAT's `test_property_type_is_not_array`: a list of types must hold one type,
    or one type followed by `null`, such as `[string, null]`.
    """
    errors: list[str] = []
    for path, schema in _schema_nodes(connection_specification):
        types = schema.get("type")
        if not _is_property(path) or not isinstance(types, list):
            continue
        if len(types) not in (1, 2) or (len(types) == 2 and types[1] != "null"):
            errors.append(
                f"{_spec_pointer((*path, 'type'))} must be a single type, or a type followed by "
                "`null`, such as `[string, null]`."
            )
    return errors


def _iter_keys(value: Any, path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], str]]:
    """Yield `(path, key)` for every mapping key anywhere in a JSON-like value."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield path, str(key)
            yield from _iter_keys(item, (*path, str(key)))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _iter_keys(item, (*path, str(index)))


def find_unresolved_ref_errors(spec: Mapping[str, Any]) -> list[str]:
    """Return one error per `$ref` left in the emitted spec.

    Ported from CAT's `test_defined_refs_exist_in_json_spec_file`. The platform does not
    resolve references in a spec, so the connector must emit it with every `$ref` resolved.
    `spec` is the whole SPEC message payload, so `advanced_auth` is checked too.
    """
    return [
        f"{_spec_pointer((*path, key))} is an unresolved reference; emit the spec with every "
        "`$ref` resolved."
        for path, key in _iter_keys(spec)
        if key == "$ref"
    ]


def find_additional_properties_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per `additionalProperties` that is set to anything but `true`.

    Ported from CAT's `test_additional_properties_is_true`. With `false` or a schema, removing
    a property in a later version makes every existing config that sets it invalid.
    """
    return [
        f"{_spec_pointer((*path, 'additionalProperties'))} must be `true` when set; any other "
        "value makes existing configs invalid as soon as a property is removed."
        for path, schema in _schema_nodes(connection_specification)
        if "additionalProperties" in schema and schema["additionalProperties"] is not True
    ]


def find_display_type_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per misused `display_type`.

    Ported from CAT's `test_display_type`: `display_type` belongs on a field with `oneOf`
    variants, and must be `dropdown` or `radio`.
    """
    errors: list[str] = []
    for path, schema in _schema_nodes(connection_specification):
        if "display_type" not in schema:
            continue
        where = _spec_pointer((*path, "display_type"))
        if "oneOf" not in schema:
            errors.append(f"{where} is set on a field without `oneOf` variants.")
        display_type = schema["display_type"]
        if not isinstance(display_type, str) or display_type not in DISPLAY_TYPES:
            errors.append(f"{where} must be `dropdown` or `radio`.")
    return errors


def find_duplicate_order_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per `order` value used twice among the properties of one object.

    Ported from CAT's `test_duplicate_order`. Properties in different `group`s may share an
    `order`.
    """
    errors: list[str] = []
    for path, schema in _schema_nodes(connection_specification):
        properties = schema.get("properties")
        if schema.get("type") != "object" or not isinstance(properties, Mapping):
            continue
        used: dict[tuple[str, str], str] = {}
        for name, prop in properties.items():
            if not isinstance(prop, Mapping) or "order" not in prop:
                continue
            key = (str(prop.get("group", "")), json.dumps(prop["order"]))
            if key in used:
                errors.append(
                    f"{_spec_pointer((*path, 'properties', name, 'order'))} repeats the "
                    f"`order` of `{used[key]}`" + (f" in group `{key[0]}`." if key[0] else ".")
                )
            else:
                used[key] = name
    return errors


def find_nested_group_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per `group` set below the top-level properties.

    Ported from CAT's `test_nested_group`: the setup form groups top-level fields only.
    """
    return [
        f"{_spec_pointer((*path, 'group'))} is set below the top level; only top-level "
        "properties can be grouped."
        for path, schema in _schema_nodes(connection_specification)
        if "group" in schema and path and not (len(path) == 2 and path[0] == "properties")
    ]


def _config_paths(schema: Any, prefix: tuple[str, ...] = ()) -> Iterator[tuple[str, ...]]:
    """Yield the path of every property a config can set, through `oneOf`/`anyOf` variants.

    An array's items add a `[]` segment, as in CAT's `get_expected_schema_structure`.
    """
    if not isinstance(schema, Mapping):
        return
    for keyword in _COMBINATORS:
        for variant in _variants(schema, (keyword,)):
            yield from _config_paths(variant, prefix)
    properties = schema.get("properties")
    if isinstance(properties, Mapping):
        for name, prop in properties.items():
            yield (*prefix, str(name))
            yield from _config_paths(prop, (*prefix, str(name)))
    items = schema.get("items")
    if isinstance(items, Mapping):
        yield from _config_paths(items, (*prefix, "[]"))


def find_oauth_parameter_errors(spec: Mapping[str, Any]) -> list[str]:
    """Return one error per `advanced_auth` config path that the connection spec lacks.

    Ported from CAT's `test_oauth_flow_parameters`. `predicate_key` and every
    `path_in_connector_config` in the OAuth input and output specifications must name a
    property of `connectionSpecification`, or the platform reads or writes a config field
    that does not exist.
    """
    advanced_auth = spec.get("advanced_auth")
    if not isinstance(advanced_auth, Mapping):
        return []
    known = set(_config_paths(spec.get("connectionSpecification") or {}))
    referenced: list[tuple[str, list[Any]]] = []
    if isinstance(advanced_auth.get("predicate_key"), list):
        referenced.append(("`/advanced_auth/predicate_key`", advanced_auth["predicate_key"]))
    oauth = advanced_auth.get("oauth_config_specification")
    if isinstance(oauth, Mapping):
        for section in (
            "oauth_user_input_from_connector_config_specification",
            "complete_oauth_output_specification",
            "complete_oauth_server_output_specification",
        ):
            properties = (oauth.get(section) or {}).get("properties")
            if not isinstance(properties, Mapping):
                continue
            for name, prop in properties.items():
                if isinstance(prop, Mapping) and isinstance(
                    prop.get("path_in_connector_config"), list
                ):
                    referenced.append(
                        (
                            _spec_pointer(
                                (
                                    "advanced_auth",
                                    "oauth_config_specification",
                                    section,
                                    "properties",
                                    name,
                                    "path_in_connector_config",
                                )
                            ),
                            prop["path_in_connector_config"],
                        )
                    )
    return [
        f"{where} names the config path `/{'/'.join(str(part) for part in config_path)}`, "
        "which is not a property of `connectionSpecification`."
        for where, config_path in referenced
        if tuple(str(part) for part in config_path) not in known
    ]


def find_invalid_schema_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per place where the spec is not a valid JSON Schema.

    The spec is checked against the meta-schema of the draft it declares in `$schema`, draft 7
    by default, and draft 7 when `$schema` is not a string (its meta-schema then reports
    `$schema`). CAT only found this through `test_config_match_spec`, when a scenario config
    was validated against an invalid spec.
    """
    return [
        f"{_spec_pointer(map(str, error.path))} is not valid JSON Schema: it fails the "
        f"`{error.validator}` rule of the meta-schema."
        for error in _meta_validator(connection_specification).iter_errors(connection_specification)
    ]


@dataclass(frozen=True)
class SpecLint:
    """A named credential-free assertion on a connector spec."""

    name: str
    rule: str
    """What the lint requires and why, printed above its problems."""
    run: Callable[[Mapping[str, Any]], list[str]]
    """Takes the whole SPEC payload (`connectionSpecification`, `advanced_auth`, ...)."""
    enforced: bool = True


def _on_connection_specification(
    check: Callable[[Mapping[str, Any]], list[str]],
) -> Callable[[Mapping[str, Any]], list[str]]:
    def run(spec: Mapping[str, Any]) -> list[str]:
        connection_specification = spec.get("connectionSpecification")
        if not isinstance(connection_specification, Mapping):
            return []
        return check(connection_specification)

    return run


SPEC_LINTS: tuple[SpecLint, ...] = (
    SpecLint(
        "secret marking",
        "A property that holds a credential must set `airbyte_secret: true` so the platform "
        "masks it in the UI and logs and stores it in the secrets manager.",
        _on_connection_specification(find_secret_marking_errors),
    ),
    SpecLint(
        "valid JSON Schema",
        "The spec must be a valid JSON Schema, or the platform cannot validate configs with it.",
        _on_connection_specification(find_invalid_schema_errors),
    ),
    SpecLint(
        "refs resolved",
        "The platform does not resolve `$ref`, so the emitted spec must contain none.",
        find_unresolved_ref_errors,
    ),
    SpecLint(
        "additionalProperties is true",
        "`additionalProperties` must be `true` when set, so that removing a property later "
        "does not invalidate saved configs.",
        _on_connection_specification(find_additional_properties_errors),
    ),
    SpecLint(
        "oneOf usage",
        "The setup form renders `oneOf` as a choice of object variants told apart by one "
        "shared `const` property.",
        _on_connection_specification(find_oneof_usage_errors),
    ),
    SpecLint(
        "enum usage",
        "An `enum` must not list the same value twice.",
        _on_connection_specification(find_enum_usage_errors),
    ),
    SpecLint(
        "array items",
        "The setup form renders arrays of objects, strings, numbers, integers or enum values only.",
        _on_connection_specification(find_array_type_errors),
    ),
    SpecLint(
        "property types",
        "The setup form renders a property's first type, so a list of types must be one type, "
        "optionally followed by `null`.",
        _on_connection_specification(find_property_type_errors),
    ),
    SpecLint(
        "display_type",
        "`display_type` belongs on a field with `oneOf` variants and must be `dropdown` or "
        "`radio`.",
        _on_connection_specification(find_display_type_errors),
    ),
    SpecLint(
        "duplicate order",
        "The setup form orders fields by `order`, which must be unique within one object and "
        "group.",
        _on_connection_specification(find_duplicate_order_errors),
    ),
    SpecLint(
        "nested group",
        "The setup form groups top-level fields only.",
        _on_connection_specification(find_nested_group_errors),
    ),
    SpecLint(
        "OAuth config paths",
        "Every config path in `advanced_auth` must name a property of the connection "
        "specification.",
        find_oauth_parameter_errors,
    ),
    # Not enforced yet. `source-facebook-marketing` (a single-item `allOf` that pydantic emits
    # for an enum field) and `destination-ragie` (pydantic-v2 `Optional` fields as `anyOf`)
    # build their specs in code. Inlining those schemas accepts exactly the same configs, but the
    # spec backward-compatibility check reports it as breaking.
    SpecLint(
        "forbidden keywords",
        "The connector setup form cannot render these JSON Schema keywords.",
        _on_connection_specification(find_forbidden_keyword_errors),
        enforced=False,
    ),
    # Not enforced yet: `source-smoke-test` builds its spec in PyAirbyte, outside the monorepo,
    # and its free-form `json_schema` and `records` objects cannot gain properties compatibly.
    SpecLint(
        "object not empty",
        "The setup form cannot show an object with no `properties` and no `oneOf`.",
        _on_connection_specification(find_empty_object_errors),
        enforced=False,
    ),
)
"""The lints `assert_spec_is_valid` runs, in the order their problems are reported."""


def find_spec_problems(
    spec: Mapping[str, Any],
    lints: Iterable[SpecLint] = SPEC_LINTS,
) -> dict[SpecLint, list[str]]:
    """Run each lint on a SPEC payload and return the problems of every failing lint.

    `spec` is the SPEC message's `spec` object as plain JSON. A lint that raises is reported
    as a problem of that lint, so that a lint which is not enforced cannot fail the test by
    crashing on a spec it did not anticipate.
    """
    results: dict[SpecLint, list[str]] = {}
    for lint in lints:
        try:
            problems = lint.run(spec)
        except Exception as error:  # Reported, not raised; see the docstring.
            problems = [f"The lint could not run: {type(error).__name__}."]
        if problems:
            results[lint] = problems
    return results


def format_spec_problems(problems: Mapping[SpecLint, list[str]]) -> str:
    """Render problems grouped by lint, for an assertion or warning message."""
    return "\n".join(
        f"[{lint.name}] {lint.rule}\n" + "\n".join(f"  - {problem}" for problem in found)
        for lint, found in problems.items()
    )


def get_single_spec(result: EntrypointOutput, *, connector_name: str) -> ConnectorSpecification:
    """Assert that `spec` emitted exactly one SPEC message, and return its specification."""
    spec_messages = result.spec_messages
    assert len(spec_messages) == 1, (
        f"`spec` for connector '{connector_name}' emitted {len(spec_messages)} SPEC messages, "
        f"expected exactly 1. Logs: {result.logs}"
    )
    spec = spec_messages[0].spec
    assert spec is not None, (
        f"`spec` for connector '{connector_name}' emitted an empty SPEC message."
    )
    return spec


def assert_spec_is_valid(spec: ConnectorSpecification, *, connector_name: str) -> None:
    """Assert that a connector spec passes every enforced credential-free lint.

    Problems found by lints that are not enforced yet (see `SPEC_LINTS`) are reported as a
    `SpecLintWarning` instead. No lint prints a config value: a spec holds none.
    """
    problems = find_spec_problems(ConnectorSpecificationSerializer.dump(spec))
    advisory = {lint: found for lint, found in problems.items() if not lint.enforced}
    if advisory:
        warnings.warn(
            f"The spec of connector '{connector_name}' has problems that will fail this test in "
            f"a future CDK release:\n{format_spec_problems(advisory)}",
            category=SpecLintWarning,
            stacklevel=2,
        )
    enforced = {lint: found for lint, found in problems.items() if lint.enforced}
    assert not enforced, (
        f"The spec of connector '{connector_name}' fails the spec lint. See "
        f"{SPEC_REFERENCE_URL} for the rules.\n{format_spec_problems(enforced)}"
    )


def _pointer_token(name: str) -> str:
    return name.replace("~", "~0").replace("/", "~1")


def _scalars(value: Any) -> Iterator[Any]:
    """Yield every scalar inside a JSON-like value.

    Mapping keys are not yielded: they name the fields of a secret object, and the runtime
    secret filter does not mask them either.
    """
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _scalars(item)
    elif isinstance(value, list):
        for item in value:
            yield from _scalars(item)
    elif value is not None:
        yield value


def _discriminators(variant: Mapping[str, Any]) -> dict[str, Any]:
    """Return the properties of a `oneOf` variant that have one fixed value, such as `auth_type`."""
    properties = variant.get("properties")
    if not isinstance(properties, Mapping):
        return {}
    discriminators: dict[str, Any] = {}
    for name, property_schema in properties.items():
        if not isinstance(property_schema, Mapping):
            continue
        enum = property_schema.get("enum")
        if "const" in property_schema:
            discriminators[name] = property_schema["const"]
        elif isinstance(enum, list) and len(enum) == 1:
            discriminators[name] = enum[0]
    return discriminators


def _selected_variants(variants: list[Any], config: Any) -> list[Mapping[str, Any]]:
    """Return the `oneOf` variants that `config` selects through their discriminators.

    A value that is secret only in a variant the user did not choose is not a secret of this
    config. If no variant's discriminators match the config, for example because no variant
    has any, every variant is returned.
    """
    mappings = [variant for variant in variants if isinstance(variant, Mapping)]
    if not isinstance(config, Mapping):
        return mappings
    selected = [
        variant
        for variant in mappings
        if (discriminators := _discriminators(variant))
        and all(name in config and config[name] == value for name, value in discriminators.items())
    ]
    return selected or mappings


def _iter_config_secrets(
    schema: Any,
    config: Any,
    pointer: str = "",
) -> Iterator[tuple[str, Any]]:
    """Yield `(config_pointer, value)` for each value in `config` that `schema` marks secret.

    The pointer is built from spec property names and array indices only, never from config
    keys or values, so it is safe to print. Every scalar inside a secret object or array is
    yielded under the pointer of that object or array.
    """
    if not isinstance(schema, Mapping):
        return
    if schema.get("airbyte_secret") is True:
        for value in _scalars(config):
            yield pointer, value

    properties = schema.get("properties")
    if isinstance(properties, Mapping) and isinstance(config, Mapping):
        for name, property_schema in properties.items():
            if name in config:
                yield from _iter_config_secrets(
                    property_schema, config[name], f"{pointer}/{_pointer_token(name)}"
                )

    for keyword in _COMBINATORS:
        variants = schema.get(keyword)
        if isinstance(variants, list):
            if keyword == "oneOf":
                variants = _selected_variants(variants, config)
            for variant in variants:
                yield from _iter_config_secrets(variant, config, pointer)

    items = schema.get("items")
    if isinstance(items, Mapping) and isinstance(config, list):
        for index, item in enumerate(config):
            yield from _iter_config_secrets(items, item, f"{pointer}/{index}")


def find_config_secrets(
    connection_specification: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[tuple[str, Any]]:
    """Return `(config_pointer, value)` for every `airbyte_secret` value in `config`.

    Unlike `airbyte_cdk.utils.airbyte_secrets_utils.get_secrets`, which the CDK uses to mask
    logs at runtime, this also walks array `items` and `anyOf`/`allOf` variants, so it finds a
    secret in an array of objects, such as one API key per account. Of a `oneOf`, it walks only
    the variants the config selects (see `_selected_variants`). For the variant the config
    selects, it finds every value `get_secrets` finds.
    """
    return list(dict.fromkeys(_iter_config_secrets(connection_specification, config)))


def _is_searchable(value: str) -> bool:
    return len(value) >= _MIN_LEAK_CHECK_LENGTH and re.search(r"\d", value) is not None


def _secret_variants(secret: Any) -> set[str]:
    """Return every form of `secret` worth searching for in the output.

    Besides the literal value, a secret shows up JSON-escaped when a connector dumps its
    config, `repr`-escaped when it formats a Python object, and URL-encoded in a logged URL.
    Each line of a multi-line secret, such as a PEM private key, is searched on its own too.
    Only values of at least `_MIN_LEAK_CHECK_LENGTH` characters that contain a digit are
    kept: shorter or word-like values, such as the `invalid_api_key` placeholders found in
    test configs, appear in ordinary output by coincidence.
    """
    if isinstance(secret, bool) or not isinstance(secret, (str, int, float)):
        return set()
    value = str(secret)
    if not _is_searchable(value):
        return set()
    variants = {value, json.dumps(value)[1:-1], repr(value)[1:-1], quote(value, safe="")}
    variants.update(line for line in re.split(r"\r?\n|\\n", value) if _is_searchable(line))
    return variants


def _stream_name(message: Mapping[str, Any]) -> str | None:
    """Return the name of the stream a serialized message is about, if any."""
    record = message.get("record")
    if isinstance(record, Mapping) and isinstance(record.get("stream"), str):
        return str(record["stream"])
    for value in message.values():
        if isinstance(value, Mapping):
            descriptor = value.get("stream_descriptor")
            if isinstance(descriptor, Mapping) and isinstance(descriptor.get("name"), str):
                return str(descriptor["name"])
            nested_name = _stream_name(value)
            if nested_name is not None:
                return nested_name
    return None


def _string_values(value: Any) -> Iterator[str]:
    """Yield every string found in a JSON-like value, recursively.

    Numbers are yielded as text too, so a numeric secret emitted as a JSON number is found.
    """
    if isinstance(value, str):
        yield value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        yield str(value)
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _string_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _string_values(item)


_CONFIG_VALIDATION_ERROR_PREFIX = "Config validation error:"
"""How `check_config_against_spec_or_exit` starts the message of a failed config validation."""

_CONFIG_VALIDATION_LABEL = "built by the CDK's config validation"


def _is_config_validation_status(message: AirbyteMessage) -> bool:
    status = message.connectionStatus
    return (
        message.type == Type.CONNECTION_STATUS
        and status is not None
        and (status.message or "").startswith(_CONFIG_VALIDATION_ERROR_PREFIX)
    )


def find_leaked_secrets(
    output: EntrypointOutput,
    secrets: Iterable[tuple[str, Any]],
) -> list[str]:
    """Return one line per place in `output` that contains a secret from `secrets`.

    `secrets` holds `(config_pointer, value)` pairs, as `find_config_secrets` returns. A line
    names the pointers of the secrets found and where they were found: the message number (on
    the Docker path, the stdout line number), its type and stream, or the stderr line number.
    It never contains any text from the output, so it cannot print a secret in any encoding.
    The stream name is left out if it overlaps any searched form of a secret.

    A CONNECTION_STATUS that the CDK builds from a failed config validation is labeled as
    such, since the connector's own code never ran: the fix is a newer CDK, which masks
    secrets in that message, or a scenario config that passes the spec.

    CONTROL messages are skipped: a connector config update carries the full config,
    including its secrets, by design.
    """
    secrets = list(secrets)
    variants_by_pointer: dict[str, set[str]] = {}
    for pointer, secret in secrets:
        if variants := _secret_variants(secret):
            variants_by_pointer.setdefault(pointer, set()).update(variants)
    if not variants_by_pointer:
        return []
    every_secret_form = set().union(*variants_by_pointer.values())

    def leaked_pointers(text: str) -> list[str]:
        return [
            pointer
            for pointer, variants in variants_by_pointer.items()
            if any(variant in text for variant in variants)
        ]

    def describe(pointers: Iterable[str]) -> str:
        return ", ".join(f"`{pointer}`" for pointer in sorted(set(pointers)))

    leaks: list[str] = []
    for number, message in enumerate(output.get_message_iterator(), start=1):
        if message.type == Type.CONTROL:
            continue
        serialized_message = AirbyteMessageSerializer.dump(message)
        pointers = [
            pointer
            for text in _string_values(serialized_message)
            for pointer in leaked_pointers(text)
        ]
        if not pointers:
            continue
        location = message.type.value
        stream = _stream_name(serialized_message)
        if stream and not any(form in stream or stream in form for form in every_secret_form):
            location += f", stream `{stream}`"
        if _is_config_validation_status(message):
            location += f", {_CONFIG_VALIDATION_LABEL}"
        leaks.append(f"{describe(pointers)} in message #{number} ({location})")

    stderr_lines = (output.stderr or "").splitlines()
    for number, line in enumerate(stderr_lines, start=1):
        pointers = leaked_pointers(line)
        if pointers:
            leaks.append(f"{describe(pointers)} in stderr line {number}")
    return leaks


def assert_no_secrets_in_output(
    output: EntrypointOutput,
    *,
    spec: ConnectorSpecification,
    config: Mapping[str, Any],
    verb: str,
    connector_name: str,
) -> None:
    """Assert that no `airbyte_secret` value from `config` appears in the connector's output.

    This searches every message the connector emitted and, on the Docker path, its raw
    stdout and stderr. In-process, a `print()` to stdout is not captured: only the messages
    the connector yields and the records of the root logger are searched.

    Run this before any other assertion on `output`, since those print raw messages.
    """
    secrets = find_config_secrets(spec.connectionSpecification, config)
    leaks = find_leaked_secrets(output, secrets)
    hint = ""
    if any(_CONFIG_VALIDATION_LABEL in leak for leak in leaks):
        hint = (
            "\nA message built by the CDK's config validation comes from the CDK, not from "
            "connector code: the CDK in use predates masking secrets in it, or the secret sits "
            "where the CDK's secret filter does not look (the spec lint reports that). Upgrade "
            "`airbyte-cdk`, or make the scenario config pass the spec."
        )
    assert not leaks, (
        f"`{verb}` for connector '{connector_name}' printed secret values from its config. "
        "Secrets must never appear in logs, traces or connection status messages. The values "
        "at these config paths were found in the output:\n" + "\n".join(leaks) + hint
    )
