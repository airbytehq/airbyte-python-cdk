# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Credential-free assertions on a connector's spec.

These checks need only the connector's SPEC message, plus a scenario's config and the
connector's output for the secret leakage check. That lets them run in both standard-test
paths: in-process, and against the connector image.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from airbyte_cdk.models import AirbyteMessageSerializer, ConnectorSpecification, Type
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.utils.airbyte_secrets_utils import get_secrets

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
"""

_SECRET_CAPABLE_TYPES = {"string", "integer", "number"}

_MASK = "****"
_EXCERPT_CONTEXT = 150

_MIN_LEAK_CHECK_LENGTH = 8
"""Secret values shorter than this are not searched for in the output.

Short values, such as `123` in a test config, occur in ordinary output by coincidence.
"""


def is_secret_property_name(name: str) -> bool:
    """Return whether a spec property with this name is expected to hold a secret."""
    normalized_name = name.lower().replace("-", "_")
    return normalized_name in SECRET_PROPERTY_NAMES or normalized_name.endswith(
        SECRET_PROPERTY_NAME_SUFFIXES
    )


def _can_hold_secret(property_schema: Mapping[str, Any]) -> bool:
    """Return whether a property can hold a secret value.

    Booleans and nulls cannot, and the UI cannot render secret objects or arrays. A property
    with a `const` value cannot either, since every config holds the same value.
    """
    property_type = property_schema.get("type")
    if isinstance(property_type, str):
        types = {property_type}
    elif isinstance(property_type, list):
        types = set(property_type)
    else:
        return False
    return bool(types & _SECRET_CAPABLE_TYPES) and "const" not in property_schema


def _iter_named_properties(
    schema: Mapping[str, Any],
    pointer: str = "",
    name: str | None = None,
) -> Iterator[tuple[str, str, Mapping[str, Any]]]:
    """Yield `(json_pointer, property_name, property_schema)` for each property in a schema.

    Variants of a `oneOf`/`anyOf`/`allOf` and the `items` of an array are yielded under the
    name of the property that declares them.
    """
    if name is not None:
        yield pointer, name, schema

    properties = schema.get("properties")
    if isinstance(properties, Mapping):
        for property_name, property_schema in properties.items():
            if isinstance(property_schema, Mapping):
                yield from _iter_named_properties(
                    property_schema,
                    f"{pointer}/properties/{property_name}",
                    property_name,
                )

    for keyword in ("oneOf", "anyOf", "allOf"):
        variants = schema.get(keyword)
        if isinstance(variants, list):
            for index, variant in enumerate(variants):
                if isinstance(variant, Mapping):
                    yield from _iter_named_properties(variant, f"{pointer}/{keyword}/{index}", name)

    items = schema.get("items")
    if isinstance(items, Mapping):
        yield from _iter_named_properties(items, f"{pointer}/items", name)


def find_secret_marking_errors(connection_specification: Mapping[str, Any]) -> list[str]:
    """Return one error per property whose `airbyte_secret` flag is wrong.

    Any `airbyte_secret` value must be a boolean: the CDK's secret filter only masks values
    whose flag is `True`, so a string such as `"true"` leaves the value unmasked. A property
    whose name denotes a secret (see `is_secret_property_name`) and that can hold a secret
    value must set `airbyte_secret: true`. One that cannot hold a secret value, such as a
    boolean or an object, must not set it.
    """
    errors: list[str] = []
    for pointer, name, property_schema in _iter_named_properties(connection_specification):
        marking = property_schema.get("airbyte_secret")
        if marking is not None and not isinstance(marking, bool):
            errors.append(
                f"`{pointer}` sets `airbyte_secret` to `{json.dumps(marking)}`, which is not a "
                "boolean, so the CDK does not treat the value as a secret."
            )
            continue
        if "type" not in property_schema or not is_secret_property_name(name):
            continue

        marked_as_secret = marking is True
        can_hold_secret = _can_hold_secret(property_schema)
        if can_hold_secret and not marked_as_secret:
            errors.append(
                f"`{pointer}` looks like a secret but is not marked `airbyte_secret: true`."
            )
        elif marked_as_secret and not can_hold_secret:
            errors.append(
                f"`{pointer}` is marked `airbyte_secret: true` but its type "
                f"`{property_schema.get('type')}` cannot hold a secret value."
            )
    return errors


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
    """Assert that a connector spec passes every credential-free lint."""
    errors = find_secret_marking_errors(spec.connectionSpecification)
    assert not errors, (
        f"The spec of connector '{connector_name}' marks secrets incorrectly. A property that "
        "holds a credential must set `airbyte_secret: true` so the platform masks it in the UI "
        "and logs and stores it in the secrets manager:\n" + "\n".join(errors)
    )


def _string_values(value: Any) -> Iterator[str]:
    """Yield every string found in a JSON-like value, recursively."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _string_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _string_values(item)


def _searchable_secrets(secrets: Iterable[Any]) -> list[str]:
    """Return the secret values worth searching for in the output.

    Only values of at least `_MIN_LEAK_CHECK_LENGTH` characters that contain a digit are
    kept. Shorter or word-like values, such as the `invalid_api_key` placeholders found in
    test configs, appear in ordinary output by coincidence.
    """
    searchable: list[str] = []
    for secret in secrets:
        if isinstance(secret, bool) or not isinstance(secret, (str, int, float)):
            continue
        value = str(secret)
        if len(value) >= _MIN_LEAK_CHECK_LENGTH and re.search(r"\d", value):
            searchable.append(value)
    return searchable


def _leak_excerpt(text: str, secrets: Iterable[Any]) -> str:
    """Return an excerpt of `text` around its first secret, with every secret masked.

    Secrets are masked longest first so that a secret containing another one is masked whole,
    and before the excerpt is cut so that the cut cannot expose part of a secret.
    """
    secret_values = sorted(
        {str(secret) for secret in secrets if secret and not isinstance(secret, bool)},
        key=len,
        reverse=True,
    )
    for secret in secret_values:
        text = text.replace(secret, _MASK)
    start = max(text.find(_MASK) - _EXCERPT_CONTEXT, 0)
    return text[start : start + 2 * _EXCERPT_CONTEXT]


def find_leaked_secrets(output: EntrypointOutput, secrets: Iterable[Any]) -> list[str]:
    """Return a masked excerpt of each message in `output` that contains one of `secrets`.

    CONTROL messages are skipped: a connector config update carries the full config,
    including its secrets, by design.
    """
    secrets = list(secrets)
    searchable_secrets = _searchable_secrets(secrets)
    if not searchable_secrets:
        return []

    leaks: list[str] = []
    for message in output.get_message_iterator():
        if message.type == Type.CONTROL:
            continue
        for text in _string_values(AirbyteMessageSerializer.dump(message)):
            if any(secret in text for secret in searchable_secrets):
                leaks.append(f"{message.type.value} message: {_leak_excerpt(text, secrets)}")
                break
    return leaks


def assert_no_secrets_in_output(
    output: EntrypointOutput,
    *,
    spec: ConnectorSpecification,
    config: Mapping[str, Any],
    verb: str,
    connector_name: str,
) -> None:
    """Assert that no `airbyte_secret` value from `config` appears in the connector's output."""
    secrets = get_secrets(spec.connectionSpecification, config)
    leaks = find_leaked_secrets(output, secrets)
    assert not leaks, (
        f"`{verb}` for connector '{connector_name}' printed secret values from its config. "
        "Secrets must never appear in logs, traces or connection status messages:\n"
        + "\n".join(leaks)
    )
