#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
"""
Section-aware CHECK for the stepped setup form.

A connector whose specification declares `sections` (each listing the `groups` of properties it shows) can be checked
one section at a time. The form order is the webapp's: sections with a numeric `order` come first, sorted by it, then
the others in list order. A section which declares `"check": true` tells the platform to run a CHECK when
the user completes it; the configuration JSON then carries a top-level `check-section` key naming the section, and only
holds the fields of that section and of the sections before it. The entrypoint validates that much of the configuration
against the specification, runs the full check for the last section (the configuration is complete by then) and
delegates every other section to the connector's `check_section`. A section without `"check": true` is never checked
on its own; the last one is covered by the full check the platform runs before creating the source. Without the key,
CHECK validates the complete configuration, exactly as before.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from airbyte_cdk.models import ConnectorSpecification, FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

CHECK_SECTION_KEY = "check-section"
"""The optional top-level key of the configuration JSON object which turns a CHECK into a section check."""


def _form_position(section: Mapping[str, Any]) -> Tuple[int, float]:
    """Sections with a numeric `order` come first, sorted by it; the others keep their list order after them."""
    order = section.get("order")
    if isinstance(order, (int, float)) and not isinstance(order, bool):
        return (0, order)
    return (1, 0)


def _sections(spec: ConnectorSpecification) -> List[Mapping[str, Any]]:
    """
    The `sections` of the specification which have a string `id`, in form order: the webapp shows the sections with a
    numeric `order` first, sorted by it, then the others in list order, so the list position alone is not the order.
    """
    return sorted(
        (
            section
            for section in spec.connectionSpecification.get("sections") or []
            if isinstance(section, Mapping) and isinstance(section.get("id"), str)
        ),
        key=_form_position,
    )


def declared_sections(spec: ConnectorSpecification) -> List[str]:
    """The `sections[].id` values of the specification, in form order; empty when the specification declares none."""
    return [section["id"] for section in _sections(spec)]


def checkable_sections(spec: ConnectorSpecification) -> List[str]:
    """The ids of the sections which declare `"check": true`, the only ones a `check-section` may name."""
    return [section["id"] for section in _sections(spec) if section.get("check") is True]


def requested_section(config: Mapping[str, Any], spec: ConnectorSpecification) -> Optional[str]:
    """
    The section named by `check-section` in the config, or None for a full check.

    Raises a config error when the key is present but not a non-empty string (a null value included), when the
    specification declares no sections, when the section is not one of the declared ones, or when the section does not
    declare `"check": true`.
    """
    if CHECK_SECTION_KEY not in config:
        return None
    section = config[CHECK_SECTION_KEY]
    declared = declared_sections(spec)
    if not isinstance(section, str) or not section.strip():
        message = f"'{CHECK_SECTION_KEY}' must be a non-empty string."
    elif not declared:
        message = f"'{CHECK_SECTION_KEY}' is not supported: the connector specification declares no sections."
    elif section not in declared:
        message = (
            f"Unknown {CHECK_SECTION_KEY} '{section}'; "
            f"the connector specification declares: {', '.join(declared)}."
        )
    elif section not in checkable_sections(spec):
        message = (
            f"Section '{section}' has no check: "
            f'the connector specification does not declare "check": true for it.'
        )
    else:
        return section
    raise AirbyteTracedException(
        internal_message=message, message=message, failure_type=FailureType.config_error
    )


def is_last_section(spec: ConnectorSpecification, section: str) -> bool:
    """Whether `section` is the last one declared, which completes the configuration."""
    declared = declared_sections(spec)
    return bool(declared) and section == declared[-1]


def strip_check_section(config: Mapping[str, Any]) -> Dict[str, Any]:
    """The config without the `check-section` key, which is not a connector field."""
    return {key: value for key, value in config.items() if key != CHECK_SECTION_KEY}


CROSS_PROPERTY_KEYWORDS = (
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "if",
    "then",
    "else",
    "dependencies",
    "dependentRequired",
    "dependentSchemas",
)
"""
Root-level JSON Schema keywords which can require properties of later sections (for instance a `dependentRequired`
tying a connection field to a credentials field). A section check drops them, see `section_spec`.
"""


def section_spec(spec: ConnectorSpecification, section: str) -> ConnectorSpecification:
    """
    The specification narrowed to what the form has collected by `section`:
    - `required` only keeps the properties whose `group` belongs to that section or to an earlier one;
    - the root-level cross-property keywords (`CROSS_PROPERTY_KEYWORDS`) are dropped, since they can require
      properties of later sections.
    Every property keeps its own schema, so the fields which are present are still validated in full, and the dropped
    constraints apply again in the full check. The last section completes the configuration, so its specification is
    the full one.
    """
    if is_last_section(spec, section):
        return spec
    schema = copy.deepcopy(spec.connectionSpecification)
    for keyword in CROSS_PROPERTY_KEYWORDS:
        schema.pop(keyword, None)
    collected_groups: Set[str] = set()
    for declared_section in _sections(spec):
        collected_groups.update(declared_section.get("groups") or [])
        if declared_section["id"] == section:
            break
    properties = schema.get("properties") or {}
    schema["required"] = [
        name
        for name in schema.get("required") or []
        if isinstance(properties.get(name), Mapping)
        and properties[name].get("group") in collected_groups
    ]
    narrowed = copy.copy(spec)
    narrowed.connectionSpecification = schema
    return narrowed
