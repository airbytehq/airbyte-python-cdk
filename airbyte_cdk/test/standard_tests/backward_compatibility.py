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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DeploymentMode = Literal["oss", "cloud"]

REGISTRY_ENTRY_URL_TEMPLATE = (
    "https://connectors.airbyte.com/files/metadata/{docker_repository}/{version}/{registry}.json"
)
"""Public URL of a published registry entry. `version` is a version tag or `latest`."""

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
    a property that has become required fails, because saved configs can. Documentation changes
    (titles, descriptions, examples, ordering) and default changes pass and are reported as
    compatible.

    Args:
        previous: The `spec` object the published version emitted, as plain JSON.
        current: The `spec` object the version under test emits, as plain JSON.
    """
    comparison = SpecComparison()
    _diff_node(_compared_part(previous), _compared_part(current), "", None, False, comparison)
    return comparison


def _compared_part(spec: Mapping[str, Any]) -> dict[str, Any]:
    return {key: spec[key] for key in COMPARED_SPEC_KEYS if spec.get(key) is not None}


# Keys whose value documents a property rather than constraining it. A change under one of
# these cannot invalidate a saved config.
_DOC_KEYS = frozenset(
    {
        "always_show",
        "changelogUrl",
        "description",
        "display_type",
        "documentationUrl",
        "examples",
        "group",
        "order",
        "pattern_descriptor",
        "title",
    }
)

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
        "maxItems",
        "maxLength",
        "maxProperties",
        "maximum",
        "minItems",
        "minLength",
        "minProperties",
        "minimum",
        "multipleOf",
        "pattern",
        "type",
        "uniqueItems",
    }
)

# Boolean constraints, and the value that is the strict one. `pattern` and `format` have no such
# direction (one regex is not comparable to another), so any change to them is breaking.
_STRICT_BOOLEAN = {"additionalProperties": False, "uniqueItems": True}

_BOUNDS_RELAXED_BY_GROWING = frozenset(
    {"exclusiveMaximum", "maxItems", "maxLength", "maxProperties", "maximum"}
)
_BOUNDS_RELAXED_BY_SHRINKING = frozenset(
    {"exclusiveMinimum", "minItems", "minLength", "minProperties", "minimum"}
)

# Keys that decide the shape of a node rather than constrain a value.
_STRUCTURE_KEYS = frozenset({"$ref", "allOf", "anyOf", "items", "oneOf"})

# Keys whose list value is a set of allowed values. Every other list is positional: in
# `path_in_connector_config`, `["credentials", "client_id"]` is a different location from
# `["client_id"]`, not a wider one.
_SET_VALUED_KEYS = frozenset({"enum", "supported_destination_sync_modes"})

# Keys holding alternative shapes for the same node. The platform picks a branch by its
# discriminating `const`, not by its index, so branches are matched by discriminator.
_BRANCH_KEYS = frozenset({"anyOf", "oneOf"})

# Keys of `advanced_auth` that define how a new OAuth consent is obtained (consent and token URLs,
# scopes, which outputs to extract). Saved configs and their tokens do not depend on them.
_OAUTH_CONSENT_FLOW_KEYS = frozenset({"oauth_connector_input_specification"})

_MAX_VALUE_CHARS = 120


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
            _record_removed_key(key, child_path, is_property_map, comparison)
            continue

        if previous[key] != current[key]:
            _diff_member(key, previous[key], current[key], child_path, is_property_map, comparison)

    for key in current:
        if key in previous or (required_handled and key == "required"):
            continue
        _record_added_key(key, _child_path(path, key), is_property_map, comparison)


def _record_removed_key(
    key: str,
    child_path: str,
    is_property_map: bool,
    comparison: SpecComparison,
) -> None:
    label = _label(child_path)

    if is_property_map:
        comparison.breaking.append(f"{label} was removed")
    elif key in _OAUTH_CONSENT_FLOW_KEYS:
        comparison.compatible.append(f"{label} was removed (OAuth consent flow)")
    elif key in _DOC_KEYS:
        comparison.compatible.append(f"{label} was removed (documentation)")
    elif key == "default":
        comparison.compatible.append(f"{label} was removed (changes behavior, not validity)")
    elif key in _CONSTRAINT_KEYS:
        comparison.compatible.append(f"{label} was removed, widening what a config may set")
    else:
        comparison.breaking.append(f"{label} was removed")


def _record_added_key(
    key: str,
    child_path: str,
    is_property_map: bool,
    comparison: SpecComparison,
) -> None:
    label = _label(child_path)

    if is_property_map or key in _DOC_KEYS or key in _PROPERTY_MAP_KEYS:
        comparison.compatible.append(f"{label} was added")
    elif key == "default":
        comparison.compatible.append(f"{label} was added (changes behavior, not validity)")
    elif key in _CONSTRAINT_KEYS:
        comparison.breaking.append(f"{label} was added, narrowing what a config may set")
    elif key in _STRUCTURE_KEYS:
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
        if key in _DOC_KEYS:
            comparison.compatible.append(f"{label} changed (documentation)")
            return

        if key in _OAUTH_CONSENT_FLOW_KEYS:
            comparison.compatible.append(
                f"{label} changed (OAuth consent flow, applies to new authorizations only)"
            )
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

        if isinstance(previous_value, bool) and isinstance(current_value, bool):
            strict_value = _STRICT_BOOLEAN.get(key)
            if strict_value is not None:
                tightened = current_value == strict_value
                verb = "tightened to" if tightened else "relaxed to"
                message = f"{label} {verb} {current_value!r}"
                (comparison.breaking if tightened else comparison.compatible).append(message)
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


def _diff_type(
    previous_value: Any,
    current_value: Any,
    label: str,
    comparison: SpecComparison,
) -> None:
    """Compare two `type` declarations as the sets of types they allow.

    `"string"` becoming `["null", "string"]` accepts strictly more than before, so comparing the
    raw values would wrongly call it a break.
    """
    previous_types = _as_type_set(previous_value)
    current_types = _as_type_set(current_value)

    removed = sorted(previous_types - current_types)
    added = sorted(current_types - previous_types)
    if removed:
        comparison.breaking.append(f"{label} no longer allows {', '.join(removed)}")
    if added:
        comparison.compatible.append(f"{label} also allows {', '.join(added)}")


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
    every field of both branches being replaced. A branch with a discriminator and no partner is
    reported as removed or added. Only branches with nothing to identify them fall back to their
    position. Paths follow the previous version's ordering.
    """
    current_by_key = _branches_by_key(current)
    matched_current: set[int] = set()
    unkeyed_previous: list[tuple[int, Any]] = []

    for index, branch in enumerate(previous):
        branch_key = _branch_key(branch)
        if branch_key is None:
            unkeyed_previous.append((index, branch))
            continue

        current_index = current_by_key.get(branch_key)
        if current_index is None:
            comparison.breaking.append(f"{_label(f'{path}[{index}]')} was removed")
            continue

        matched_current.add(current_index)
        _diff_node(branch, current[current_index], f"{path}[{index}]", None, False, comparison)

    unkeyed_current: list[tuple[int, Any]] = []
    for index, branch in enumerate(current):
        if index in matched_current:
            continue
        if _branch_key(branch) is not None:
            comparison.compatible.append(f"{_label(f'{path}[{index}]')} was added")
            continue
        unkeyed_current.append((index, branch))

    for (previous_index, previous_branch), (_, current_branch) in zip(
        unkeyed_previous, unkeyed_current
    ):
        _diff_node(
            previous_branch, current_branch, f"{path}[{previous_index}]", None, False, comparison
        )

    for previous_index, _ in unkeyed_previous[len(unkeyed_current) :]:
        comparison.breaking.append(f"{_label(f'{path}[{previous_index}]')} was removed")

    for current_index, _ in unkeyed_current[len(unkeyed_previous) :]:
        comparison.compatible.append(f"{_label(f'{path}[{current_index}]')} was added")


def _branches_by_key(branches: list[Any]) -> dict[tuple[str, Any], int]:
    """Index branches by discriminator, dropping keys that more than one branch shares."""
    indexed: dict[tuple[str, Any], int] = {}
    duplicated: set[tuple[str, Any]] = set()

    for index, branch in enumerate(branches):
        branch_key = _branch_key(branch)
        if branch_key is None:
            continue
        if branch_key in indexed:
            duplicated.add(branch_key)
            continue
        indexed[branch_key] = index

    for branch_key in duplicated:
        del indexed[branch_key]

    return indexed


def _branch_key(branch: Any) -> tuple[str, Any] | None:
    """What identifies a branch across versions, if anything does.

    A single-valued property such as `auth_type: {const: "oauth2.0"}` is what the platform
    discriminates on; a title is the next best thing. When a branch has several discriminators,
    the smallest property name wins, so the key does not depend on declaration order.
    """
    if not isinstance(branch, dict):
        return None

    properties = branch.get("properties")
    if isinstance(properties, dict):
        discriminators = sorted(
            (str(name), value)
            for name, schema in properties.items()
            if isinstance(schema, dict)
            for value in (_single_valued(schema),)
            if value is not None
        )
        if discriminators:
            return discriminators[0]

    title = branch.get("title")
    return ("title", title) if isinstance(title, str) else None


def _single_valued(schema: dict[str, Any]) -> Any | None:
    value = schema.get("const")
    if value is None:
        enum = schema.get("enum")
        value = enum[0] if isinstance(enum, list) and len(enum) == 1 else None
    return value if isinstance(value, Hashable) else None


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


def fetch_published_spec(
    docker_repository: str,
    registry: DeploymentMode,
    version: str = "latest",
    *,
    timeout_seconds: float = 30,
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
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get(url, timeout=timeout_seconds)

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
    is still rolling out is compared against the version before that major.

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
