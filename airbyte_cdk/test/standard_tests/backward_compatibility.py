# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Backward-compatibility checks between a connector and its published version.

A connector's spec is the contract with every config that users have already saved. When a new
version is rolled out, the platform validates those saved configs against the new spec, so a
change that rejects a config the previous version accepted breaks existing connections at
upgrade time. `compare_specs` classifies every difference between two specs as breaking or
compatible, and the helpers below fetch the previously published spec from the connector
registry and decide whether a breaking change has been declared through the breaking-change
process.

The comparison rules are ported from the spec comparator of the regression tests in
`airbyte-ops-mcp`. Two differences keep this check, which blocks a merge, focused on what existing
connections depend on: only the config-relevant parts of the spec are compared (see
`COMPARED_SPEC_KEYS`), and changes to the OAuth consent flow are compatible, because they apply
to new authorizations only.
"""

from __future__ import annotations

from collections.abc import Hashable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

import requests
from packaging.version import InvalidVersion, Version
from requests.adapters import HTTPAdapter, Retry

DeploymentMode = Literal["oss", "cloud"]

REGISTRY_ENTRY_URL_TEMPLATE = (
    "https://connectors.airbyte.com/files/metadata/{docker_repository}/{version}/{registry}.json"
)
"""Public URL of a published registry entry. `version` is a version tag or `latest`."""

REGISTRY_TIMEOUT_SECONDS = (5.0, 10.0)
"""Connect and read timeouts of one registry request.

With the retries of `registry_retry`, an unreachable registry fails the test in about 30 seconds
rather than holding every image-test run for minutes.
"""

BREAKING_CHANGES_DOCS_URL = (
    "https://docs.airbyte.com/platform/connector-development/connector-breaking-changes"
)

DEPLOYMENT_MODE_ENV_VARS: dict[DeploymentMode, dict[str, str]] = {
    "oss": {"DEPLOYMENT_MODE": "oss", "AIRBYTE_EDITION": "COMMUNITY"},
    "cloud": {"DEPLOYMENT_MODE": "cloud", "AIRBYTE_EDITION": "CLOUD"},
}
"""Environment the registry publisher sets when it runs `spec` for each registry.

Legacy Java and Python CDKs read `DEPLOYMENT_MODE`; the bulk CDK reads `AIRBYTE_EDITION`.
Running the current image with the same variables makes the two specs comparable.
"""

COMPARED_SPEC_KEYS = (
    "connectionSpecification",
    "advanced_auth",
    "supported_destination_sync_modes",
)
"""The parts of a spec that existing connections depend on.

`connectionSpecification` validates saved configs, `advanced_auth` says where the platform reads
and writes OAuth values in a config, and `supported_destination_sync_modes` lists the modes that
existing connections may use. The other keys are documentation or legacy flags.
"""

MAX_REPORTED_CHANGES = 20
"""How many changes of one kind `format_spec_change_summary` lists before it truncates."""


@dataclass
class SpecComparison:
    """The differences between two specs, split by whether they break saved configs."""

    breaking: list[str] = field(default_factory=list)
    compatible: list[str] = field(default_factory=list)

    @property
    def is_backward_compatible(self) -> bool:
        return not self.breaking


@dataclass(frozen=True)
class PublishedSpec:
    """A spec as the connector registry published it."""

    version: str
    spec: dict[str, Any]
    url: str


def compare_specs(previous: Mapping[str, Any], current: Mapping[str, Any]) -> SpecComparison:
    """Compare two connector specs, allowing backward-compatible changes.

    The rule is compatibility, not equality: a new optional property passes, because no saved
    config becomes invalid, while a removed property, a narrowed type, a tightened constraint or
    a property that has become required fails, because saved configs can. Only JSON Schema
    validation keywords and the protocol keys that locate values in a config can break one;
    every other key (titles, descriptions, `order`, `airbyte_hidden`, `$schema` and so on) is an
    annotation, and changing it is compatible. Default changes are compatible too.

    Args:
        previous: The `spec` object the published version emitted, as plain JSON.
        current: The `spec` object the version under test emits, as plain JSON.
    """
    comparison = SpecComparison()
    _diff_node(_compared_part(previous), _compared_part(current), "", None, False, comparison)
    return comparison


def _compared_part(spec: Mapping[str, Any]) -> dict[str, Any]:
    return {key: spec[key] for key in COMPARED_SPEC_KEYS if spec.get(key) is not None}


# Keys whose value maps config field names to schemas. Their keys are names a connector chose,
# so a field called `description` is a field, not documentation.
_PROPERTY_MAP_KEYS = frozenset({"$defs", "definitions", "patternProperties", "properties"})

# Keys that constrain what a config may contain. Adding one narrows the set of valid configs;
# removing one only widens it.
_CONSTRAINT_KEYS = frozenset(
    {
        "additionalProperties",
        "const",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "format",
        "maxContains",
        "maxItems",
        "maxLength",
        "maxProperties",
        "maximum",
        "minContains",
        "minItems",
        "minLength",
        "minProperties",
        "minimum",
        "multipleOf",
        "pattern",
        "required",
        "type",
        "uniqueItems",
    }
)

# The value a keyword takes when it is absent. A keyword set to one of these accepts exactly what
# its absence accepts, so adding or removing it changes nothing, and moving a value keyword to one
# of these relaxes it. For a boolean keyword, any other value is the strict one. The draft-04
# boolean form of `exclusiveMaximum`/`exclusiveMinimum` defaults to false.
_KEYWORD_DEFAULTS: dict[str, tuple[Any, ...]] = {
    "additionalItems": (True, {}),
    "additionalProperties": (True, {}),
    "exclusiveMaximum": (False,),
    "exclusiveMinimum": (False,),
    "items": (True, {}),
    "minContains": (1,),
    "minItems": (0,),
    "minLength": (0,),
    "minProperties": (0,),
    "patternProperties": ({},),
    "properties": ({},),
    "required": ([],),
    "uniqueItems": (False,),
}

# Keywords with a default whose value describes fields rather than constrains a value.
_FIELD_KEYWORDS = frozenset({"items", "patternProperties", "properties"})

_BOUNDS_RELAXED_BY_GROWING = frozenset(
    {"exclusiveMaximum", "maxContains", "maxItems", "maxLength", "maxProperties", "maximum"}
)
_BOUNDS_RELAXED_BY_SHRINKING = frozenset(
    {"exclusiveMinimum", "minContains", "minItems", "minLength", "minProperties", "minimum"}
)

# Keys that decide the shape of a node rather than constrain a value. Removing one drops the
# fields it describes, so it is breaking like a removed property.
_STRUCTURE_KEYS = frozenset({"$ref", "allOf", "anyOf", "items", "oneOf"})

# Validation keywords this check does not model. Removing one only widens what a config may set;
# adding or changing one may narrow it in ways that are not compared here, so it is breaking.
# None of them appears in the connection specs of the connector fleet today.
_UNMODELED_VALIDATION_KEYS = frozenset(
    {
        "additionalItems",
        "contains",
        "dependencies",
        "dependentRequired",
        "dependentSchemas",
        "else",
        "if",
        "not",
        "prefixItems",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)

# Protocol keys that say where the platform reads and writes values in a saved config. Moving or
# removing one breaks existing connections even though no JSON Schema keyword changed.
_PROTOCOL_KEYS = frozenset(
    {
        *COMPARED_SPEC_KEYS,
        "auth_flow_type",
        "complete_oauth_output_specification",
        "complete_oauth_server_input_specification",
        "complete_oauth_server_output_specification",
        "oauth_config_specification",
        "oauth_user_input_from_connector_config_specification",
        "path_in_connector_config",
        "path_in_oauth_response",
        "predicate_key",
        "predicate_value",
    }
)

# Marks a field whose value the platform stores in its secret store. Marking more fields is
# compatible; unmarking one changes how values that were already saved as secrets are handled.
_SECRET_KEY = "airbyte_secret"

# Keys of `advanced_auth` that define how a new OAuth consent is obtained (consent and token URLs,
# scopes, which outputs to extract). Saved configs and their tokens do not depend on them.
_OAUTH_CONSENT_FLOW_KEYS = frozenset({"oauth_connector_input_specification"})

_CONFIG_RELEVANT_KEYS = frozenset(
    {
        *_PROPERTY_MAP_KEYS,
        *_CONSTRAINT_KEYS,
        *_STRUCTURE_KEYS,
        *_UNMODELED_VALIDATION_KEYS,
        *_PROTOCOL_KEYS,
        *_OAUTH_CONSENT_FLOW_KEYS,
        _SECRET_KEY,
        "default",
    }
)
"""Every key whose change can matter to a saved config. Any other key is an annotation."""

# Keys whose list value is a set of allowed values. Every other list is positional: in
# `path_in_connector_config`, `["credentials", "client_id"]` is a different location from
# `["client_id"]`, not a wider one.
_SET_VALUED_KEYS = frozenset({"enum", "supported_destination_sync_modes"})

# Keys holding alternative shapes for the same node. The platform picks a branch by its
# discriminating `const`, not by its index, so branches are matched by what identifies them.
_BRANCH_KEYS = frozenset({"anyOf", "oneOf"})

_MAX_VALUE_CHARS = 120


def _is_annotation(key: str) -> bool:
    return key not in _CONFIG_RELEVANT_KEYS


def _is_keyword_default(key: str, value: Any) -> bool:
    return any(_same_json(value, default) for default in _KEYWORD_DEFAULTS.get(key, ()))


def _same_json(left: Any, right: Any) -> bool:
    """JSON equality, which unlike Python's does not equate `false` with `0`."""
    return isinstance(left, bool) == isinstance(right, bool) and left == right


def _is_secret(value: Any) -> bool:
    # The platform reads the flag leniently, so the string "true" also marks a secret.
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def _diff_node(
    previous: Any,
    current: Any,
    path: str,
    key: str | None,
    is_property_map: bool,
    comparison: SpecComparison,
) -> None:
    if previous == current:
        return

    if isinstance(previous, dict) and isinstance(current, dict):
        _diff_object(previous, current, path, is_property_map, comparison)
        return

    if isinstance(previous, list) and isinstance(current, list):
        _diff_list(previous, current, path, key, comparison)
        return

    comparison.breaking.append(
        f"{_label(path)} changed from {_brief(previous)} to {_brief(current)}"
    )


def _diff_object(
    previous: dict[str, Any],
    current: dict[str, Any],
    path: str,
    is_property_map: bool,
    comparison: SpecComparison,
) -> None:
    required_handled = not is_property_map and _diff_required(
        previous.get("required"), current.get("required"), path, comparison
    )

    for key in previous:
        if required_handled and key == "required":
            continue

        child_path = _child_path(path, key)
        if key not in current:
            _record_removed_key(key, previous[key], child_path, is_property_map, comparison)
            continue

        if previous[key] != current[key]:
            _diff_member(key, previous[key], current[key], child_path, is_property_map, comparison)

    for key in current:
        if key in previous or (required_handled and key == "required"):
            continue
        _record_added_key(key, current[key], _child_path(path, key), is_property_map, comparison)


def _record_removed_key(
    key: str,
    value: Any,
    child_path: str,
    is_property_map: bool,
    comparison: SpecComparison,
) -> None:
    label = _label(child_path)

    if is_property_map:
        comparison.breaking.append(f"{label} was removed")
    elif key in _OAUTH_CONSENT_FLOW_KEYS:
        comparison.compatible.append(f"{label} was removed (OAuth consent flow)")
    elif key == _SECRET_KEY:
        _diff_secret(value, None, label, comparison)
    elif _is_annotation(key):
        comparison.compatible.append(f"{label} was removed (annotation)")
    elif key == "default":
        comparison.compatible.append(f"{label} was removed (changes behavior, not validity)")
    elif _is_keyword_default(key, value):
        comparison.compatible.append(f"{label} was removed; it was at its default value")
    elif key in _CONSTRAINT_KEYS or key in _UNMODELED_VALIDATION_KEYS:
        comparison.compatible.append(f"{label} was removed, widening what a config may set")
    else:
        comparison.breaking.append(f"{label} was removed")


def _record_added_key(
    key: str,
    value: Any,
    child_path: str,
    is_property_map: bool,
    comparison: SpecComparison,
) -> None:
    label = _label(child_path)

    if is_property_map or key in _PROPERTY_MAP_KEYS:
        comparison.compatible.append(f"{label} was added")
    elif key == _SECRET_KEY:
        comparison.compatible.append(f"{label} was added")
    elif _is_annotation(key):
        comparison.compatible.append(f"{label} was added (annotation)")
    elif key == "default":
        comparison.compatible.append(f"{label} was added (changes behavior, not validity)")
    elif _is_keyword_default(key, value):
        comparison.compatible.append(
            f"{label} was added at its default value {_brief(value)}, which allows the same configs"
        )
    elif key in _CONSTRAINT_KEYS:
        comparison.breaking.append(f"{label} was added, narrowing what a config may set")
    elif key in _STRUCTURE_KEYS or key in _UNMODELED_VALIDATION_KEYS:
        comparison.breaking.append(f"{label} was added, changing the shape of this node")
    else:
        comparison.compatible.append(f"{label} was added")


def _diff_member(
    key: str,
    previous_value: Any,
    current_value: Any,
    child_path: str,
    is_property_map: bool,
    comparison: SpecComparison,
) -> None:
    label = _label(child_path)

    if not is_property_map:
        if key in _OAUTH_CONSENT_FLOW_KEYS:
            comparison.compatible.append(
                f"{label} changed (OAuth consent flow, applies to new authorizations only)"
            )
            return

        if key == _SECRET_KEY:
            _diff_secret(previous_value, current_value, label, comparison)
            return

        if _is_annotation(key):
            comparison.compatible.append(f"{label} changed (annotation)")
            return

        if key == "default":
            comparison.compatible.append(
                f"{label} changed from {_brief(previous_value)} to {_brief(current_value)} "
                "(changes behavior, not validity)"
            )
            return

        if key == "type":
            _diff_type(previous_value, current_value, label, comparison)
            return

        if key in _UNMODELED_VALIDATION_KEYS and not _is_keyword_default(key, current_value):
            comparison.breaking.append(
                f"{label} changed; this check does not model `{key}`, so any change may narrow "
                "what a config may set"
            )
            return

        if key in _KEYWORD_DEFAULTS:
            # Emptying `properties` or `items` drops the fields they describe, which the
            # recursive comparison reports, so only value keywords relax to their default here.
            if _is_keyword_default(key, current_value) and key not in _FIELD_KEYWORDS:
                comparison.compatible.append(
                    f"{label} relaxed to its default {_brief(current_value)}"
                )
                return
            if isinstance(previous_value, bool) and isinstance(current_value, bool):
                comparison.breaking.append(f"{label} tightened to {current_value!r}")
                return

        if _is_number(previous_value) and _is_number(current_value):
            if _diff_bound(key, previous_value, current_value, label, comparison):
                return

    _diff_node(
        previous_value,
        current_value,
        child_path,
        key,
        not is_property_map and key in _PROPERTY_MAP_KEYS,
        comparison,
    )


def _diff_secret(
    previous_value: Any,
    current_value: Any,
    label: str,
    comparison: SpecComparison,
) -> None:
    """Compare `airbyte_secret` by direction. `current_value` is `None` when the key was removed.

    Values saved while a field was secret live in the platform's secret store, so a field that
    stops being secret changes how those saved values are read and shown. A field that becomes
    secret, or a flag rewritten without changing its meaning (`"true"` to `true`), is compatible.
    """
    was_secret = _is_secret(previous_value)
    is_secret = _is_secret(current_value)
    change = "was removed" if current_value is None else "changed"

    if was_secret and not is_secret:
        comparison.breaking.append(
            f"{label} {change}, so the field is no longer a secret: values saved as secrets may "
            "be exposed or no longer read from the secret store"
        )
    elif is_secret and not was_secret:
        comparison.compatible.append(f"{label} {change}, so the field is now a secret")
    else:
        comparison.compatible.append(
            f"{label} {change} from {_brief(previous_value)} to {_brief(current_value)} "
            "(same meaning)"
        )


def _diff_type(
    previous_value: Any,
    current_value: Any,
    label: str,
    comparison: SpecComparison,
) -> None:
    """Compare two `type` declarations as the sets of values they allow.

    `"string"` becoming `["null", "string"]` accepts strictly more than before, and so does
    `"integer"` becoming `"number"`, so comparing the raw values would wrongly call either a break.
    """
    previous_types = _as_type_set(previous_value)
    current_types = _as_type_set(current_value)

    removed = previous_types - current_types
    added = current_types - previous_types
    if "integer" in removed and "number" in current_types:
        removed.discard("integer")
        added.discard("number")
        comparison.compatible.append(f"{label} widened from integer to number")

    if removed:
        comparison.breaking.append(f"{label} no longer allows {', '.join(sorted(removed))}")
    if added:
        comparison.compatible.append(f"{label} also allows {', '.join(sorted(added))}")


def _as_type_set(value: Any) -> set[str]:
    if isinstance(value, list):
        return {str(entry) for entry in value}
    return {str(value)}


def _diff_bound(
    key: str,
    previous_value: float,
    current_value: float,
    label: str,
    comparison: SpecComparison,
) -> bool:
    """Classify a moved numeric bound. Returns whether `key` is a bound at all."""
    if key in _BOUNDS_RELAXED_BY_GROWING:
        relaxed = current_value > previous_value
    elif key in _BOUNDS_RELAXED_BY_SHRINKING:
        relaxed = current_value < previous_value
    else:
        return False

    verb = "relaxed" if relaxed else "tightened"
    message = f"{label} {verb} from {previous_value!r} to {current_value!r}"
    (comparison.compatible if relaxed else comparison.breaking).append(message)
    return True


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _diff_list(
    previous: list[Any],
    current: list[Any],
    path: str,
    key: str | None,
    comparison: SpecComparison,
) -> None:
    if key in _BRANCH_KEYS:
        _diff_branches(previous, current, path, comparison)
        return

    if key in _SET_VALUED_KEYS:
        for entry in previous:
            if entry not in current:
                comparison.breaking.append(f"{_label(path)} no longer allows {_brief(entry)}")
        for entry in current:
            if entry not in previous:
                comparison.compatible.append(f"{_label(path)} also allows {_brief(entry)}")
        return

    for index, previous_entry in enumerate(previous):
        child_path = f"{path}[{index}]"
        if index >= len(current):
            comparison.breaking.append(f"{_label(child_path)} was removed")
            continue
        _diff_node(previous_entry, current[index], child_path, key, False, comparison)

    for index in range(len(previous), len(current)):
        comparison.breaking.append(
            f"{_label(f'{path}[{index}]')} was added, changing an ordered list"
        )


def _diff_branches(
    previous: list[Any],
    current: list[Any],
    path: str,
    comparison: SpecComparison,
) -> None:
    """Compare `oneOf`/`anyOf` branches, matched by what identifies them.

    Reordering auth methods is a common, harmless edit; compared by position it would read as
    every field of both branches being replaced. A previous branch without a partner is reported
    as removed, and a current one as added. Paths follow each version's own ordering.
    """
    partners = _match_branches(previous, current)

    for index, branch in enumerate(previous):
        partner = partners.get(index)
        if partner is None:
            comparison.breaking.append(f"{_label(f'{path}[{index}]')} was removed")
            continue
        _diff_node(branch, current[partner], f"{path}[{index}]", None, False, comparison)

    matched = set(partners.values())
    for index in range(len(current)):
        if index not in matched:
            comparison.compatible.append(f"{_label(f'{path}[{index}]')} was added")


def _match_branches(previous: list[Any], current: list[Any]) -> dict[int, int]:
    """Pair each previous branch with the current branch that is most likely the same one.

    Candidate pairs are ranked by, in order: being identical; the discriminating `const` values
    they share, minus those they disagree on; the same title; how many fields and types they
    share; and how close their positions are. Pairs are then taken greedily from the best. Two
    branches that disagree on a discriminator and share none are different auth methods, never
    a pair, so a renamed discriminator reads as one branch removed and one added.
    """
    ranked: list[tuple[tuple[Any, ...], int, int]] = []
    for previous_index, previous_branch in enumerate(previous):
        for current_index, current_branch in enumerate(current):
            rank = _branch_pair_rank(
                previous_branch, current_branch, abs(previous_index - current_index)
            )
            if rank is not None:
                ranked.append((rank, previous_index, current_index))

    ranked.sort(key=lambda item: (item[0], -item[1], -item[2]), reverse=True)

    partners: dict[int, int] = {}
    matched_current: set[int] = set()
    for _, previous_index, current_index in ranked:
        if previous_index in partners or current_index in matched_current:
            continue
        partners[previous_index] = current_index
        matched_current.add(current_index)
    return partners


def _branch_pair_rank(previous: Any, current: Any, distance: int) -> tuple[Any, ...] | None:
    previous_discriminators = _discriminators(previous)
    current_discriminators = _discriminators(current)
    shared_names = previous_discriminators.keys() & current_discriminators.keys()
    agreeing = sum(
        previous_discriminators[name] == current_discriminators[name] for name in shared_names
    )
    disagreeing = len(shared_names) - agreeing
    if disagreeing and not agreeing:
        return None

    previous_title = previous.get("title") if isinstance(previous, dict) else None
    current_title = current.get("title") if isinstance(current, dict) else None
    same_title = isinstance(previous_title, str) and previous_title == current_title

    return (
        previous == current,
        agreeing - disagreeing,
        same_title,
        _similarity(_branch_features(previous), _branch_features(current)),
        -distance,
    )


def _discriminators(branch: Any) -> dict[str, Hashable]:
    """The single-valued properties of a branch, such as `auth_type: {const: "oauth2.0"}`."""
    if not isinstance(branch, dict) or not isinstance(branch.get("properties"), dict):
        return {}
    return {
        str(name): value
        for name, schema in branch["properties"].items()
        if isinstance(schema, dict)
        for value in (_single_valued(schema),)
        if value is not None
    }


def _single_valued(schema: dict[str, Any]) -> Hashable | None:
    value = schema.get("const")
    if value is None:
        enum = schema.get("enum")
        value = enum[0] if isinstance(enum, list) and len(enum) == 1 else None
    return value if isinstance(value, Hashable) else None


def _branch_features(branch: Any) -> set[str]:
    if not isinstance(branch, dict):
        return set()
    properties = branch.get("properties")
    features = (
        {f"property:{name}" for name in properties} if isinstance(properties, dict) else set()
    )
    return features | {f"type:{name}" for name in _as_type_set(branch.get("type"))}


def _similarity(left: set[str], right: set[str]) -> float:
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def _diff_required(
    previous_required: Any,
    current_required: Any,
    path: str,
    comparison: SpecComparison,
) -> bool:
    """Compare the `required` list of one schema node. Returns whether it was compared here.

    Requiring a property that was optional invalidates every saved config that omitted it, even
    when the property is new and even when the node had no `required` list before. A `required`
    value that is not a list of names is left to the generic comparison.
    """
    if previous_required is None and current_required is None:
        return False

    previous_names = [] if previous_required is None else previous_required
    current_names = [] if current_required is None else current_required
    if not _is_name_list(previous_names) or not _is_name_list(current_names):
        return False

    node = _label(path)
    for name in current_names:
        if name not in previous_names:
            comparison.breaking.append(f"{node}: `{name}` is now required")
    for name in previous_names:
        if name not in current_names:
            comparison.compatible.append(f"{node}: `{name}` is no longer required")
    return True


def _is_name_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(entry, str) for entry in value)


def _child_path(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _label(path: str) -> str:
    return f"`{path}`" if path else "the spec"


def _brief(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= _MAX_VALUE_CHARS else f"{text[:_MAX_VALUE_CHARS]}…"


def registry_retry() -> Retry:
    """The retry policy of registry requests.

    Transient server errors and rate limits are retried twice with a short backoff. A
    `Retry-After` header is not honored, so a rate-limited registry cannot stall the test.
    """
    return Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=False,
    )


def fetch_published_spec(
    docker_repository: str,
    registry: DeploymentMode,
    version: str = "latest",
) -> PublishedSpec | None:
    """Fetch a published spec from the public connector registry.

    Args:
        docker_repository: The connector's `dockerRepository`, e.g. `airbyte/source-faker`.
        registry: Which registry entry to read. Each deployment mode has its own entry, because
            a connector may emit a different spec in each.
        version: A published version tag, or `latest` for the version most users run. During a
            progressive rollout `latest` stays on the previous release until the rollout ends.

    Returns:
        The published spec, or `None` if no entry was published for this registry, which is the
        case for a connector that was never released or is disabled in that registry.

    Raises:
        requests.RequestException: If the registry cannot be reached after retries.
    """
    url = REGISTRY_ENTRY_URL_TEMPLATE.format(
        docker_repository=docker_repository,
        version=version,
        registry=registry,
    )
    with requests.Session() as session:
        adapter = HTTPAdapter(max_retries=registry_retry())
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        response = session.get(url, timeout=REGISTRY_TIMEOUT_SECONDS)

    if response.status_code == 404:
        return None
    response.raise_for_status()

    entry = response.json()
    return PublishedSpec(version=str(entry["dockerImageTag"]), spec=entry["spec"], url=url)


def is_newer_version(version: str, than: str) -> bool:
    """Whether `version` is a later release than `than`. Unparsable versions are never newer.

    A connector whose `dockerImageTag` is older than the published `latest`, such as a branch
    that has not merged a release from the base branch yet, has no earlier spec to compare
    against: the published spec contains changes the branch has never seen.
    """
    try:
        return Version(version) > Version(than)
    except InvalidVersion:
        return False


def declared_breaking_changes(
    metadata: Mapping[str, Any],
    previous_version: str,
    current_version: str,
) -> list[str]:
    """List the breaking changes declared after `previous_version`, up to `current_version`.

    A declared breaking change covers every later version until the release that carries it
    reaches `latest`, which matters during a progressive rollout: a patch on top of a major that
    is still rolling out is compared against the version before that major. A pre-release such
    as `2.0.0-rc.1` is covered by the breaking change declared for the release it precedes.

    Args:
        metadata: The `data` section of the connector's `metadata.yaml`.
        previous_version: The published version the spec is compared against.
        current_version: The connector's `dockerImageTag`.
    """
    releases = metadata.get("releases") or {}
    breaking_changes = releases.get("breakingChanges") or {}

    try:
        lower = Version(previous_version)
        upper = Version(current_version)
    except InvalidVersion:
        return [current_version] if current_version in breaking_changes else []
    if upper.is_prerelease or upper.is_devrelease:
        upper = Version(upper.base_version)

    declared: list[str] = []
    for version in breaking_changes:
        try:
            if lower < Version(str(version)) <= upper:
                declared.append(str(version))
        except InvalidVersion:
            continue
    return declared


def format_breaking_spec_changes(
    *,
    connector_name: str,
    current_version: str,
    registry: DeploymentMode,
    published: PublishedSpec,
    comparison: SpecComparison,
) -> str:
    """Explain a failed spec comparison, and how to resolve it through the breaking-change process."""
    findings = "\n".join(f"  - {finding}" for finding in comparison.breaking)
    return (
        f"The {registry.upper()} spec of `{connector_name}` {current_version} is not backward "
        f"compatible with the published version {published.version} ({published.url}):\n"
        f"{findings}\n\n"
        "Existing connections keep their saved configs when they upgrade, and those configs are "
        "validated against the new spec, so each change above can break an existing connection.\n"
        "To resolve this, do one of the following:\n"
        "  - Make the change backward compatible, for example by keeping the old field or value, "
        "or by making a new field optional.\n"
        "  - If the change is intentional, release it as a breaking change: bump the major "
        "version and declare it under `releases.breakingChanges` in metadata.yaml, with a "
        f"migration guide. See {BREAKING_CHANGES_DOCS_URL}\n"
        "  - If a config migration rewrites existing configs into the new shape, or a finding is "
        "a false positive, waive this check until the next release by adding "
        "`backward_compatibility_tests_config: {disable_for_version: "
        f'"{published.version}"}}` to an entry of the `spec` section in '
        "acceptance-test-config.yml. The waiver expires when a newer version is published."
    )


def format_spec_change_summary(
    *,
    registry: DeploymentMode,
    published: PublishedSpec,
    comparison: SpecComparison,
    waiver: str | None = None,
) -> str:
    """Summarize a passed or waived spec comparison, so a reviewer can see what was judged safe.

    Each list is capped at `MAX_REPORTED_CHANGES` entries.

    Args:
        waiver: Why the breaking changes, if any, were waived.
    """
    lines = [
        f"{registry.upper()} spec compared with the published version {published.version}: "
        f"{len(comparison.breaking)} breaking, {len(comparison.compatible)} compatible change(s)."
    ]
    if comparison.breaking:
        lines.append(f"Breaking changes, waived ({waiver}):" if waiver else "Breaking changes:")
        lines.extend(_capped(comparison.breaking))
    if comparison.compatible:
        lines.append("Compatible changes:")
        lines.extend(_capped(comparison.compatible))
    return "\n".join(lines)


def _capped(changes: list[str]) -> list[str]:
    lines = [f"  - {change}" for change in changes[:MAX_REPORTED_CHANGES]]
    if len(changes) > MAX_REPORTED_CHANGES:
        lines.append(f"  … and {len(changes) - MAX_REPORTED_CHANGES} more")
    return lines


def disabled_for_version(acceptance_test_config: Mapping[str, Any]) -> list[str]:
    """Return the versions the spec backward-compatibility test is disabled for.

    Reads `backward_compatibility_tests_config.disable_for_version` from the entries of the
    `spec` section of `acceptance-test-config.yml`, the same setting the legacy acceptance tests
    honored. The test is skipped only while the published `latest` version equals one of these,
    so the waiver expires on its own once the new version is released.
    """
    spec_section = (acceptance_test_config.get("acceptance_tests") or {}).get("spec") or {}
    versions: list[str] = []
    for test in spec_section.get("tests") or []:
        if not isinstance(test, dict):
            continue
        config = test.get("backward_compatibility_tests_config") or {}
        version = config.get("disable_for_version")
        if version is not None:
            versions.append(str(version))
    return versions
