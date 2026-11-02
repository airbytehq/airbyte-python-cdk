#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.models import ConnectorSpecification, FailureType
from airbyte_cdk.sources.utils.check_section import (
    CHECK_SECTION_KEY,
    CROSS_PROPERTY_KEYWORDS,
    checkable_sections,
    declared_sections,
    is_last_section,
    requested_section,
    section_spec,
    strip_check_section,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

SECTIONED_SCHEMA = {
    "type": "object",
    "properties": {
        "host": {"type": "string", "group": "db"},
        "port": {"type": "integer", "group": "db"},
        "ssl": {"type": "boolean", "group": "security"},
        "username": {"type": "string", "group": "auth"},
        "password": {"type": "string", "group": "auth"},
        "legacy": {"type": "string", "airbyte_hidden": True},
    },
    "required": ["host", "username", "password", "legacy"],
    "groups": [{"id": "db"}, {"id": "security", "title": "Security"}, {"id": "auth"}],
    "sections": [
        {
            "id": "connection",
            "title": "Connection",
            "order": 0,
            "groups": ["db", "security"],
            "check": True,
        },
        {
            "id": "credentials",
            "title": "Credentials",
            "order": 1,
            "groups": ["auth"],
            "check": True,
        },
        {"id": "review", "title": "Review & set up", "order": 2, "groups": []},
    ],
}
SECTIONED_SPEC = ConnectorSpecification(connectionSpecification=SECTIONED_SCHEMA)
FLAT_SPEC = ConnectorSpecification(
    connectionSpecification={"type": "object", "properties": {"host": {"type": "string"}}}
)


def test_declared_sections_are_in_form_order():
    assert declared_sections(SECTIONED_SPEC) == ["connection", "credentials", "review"]


# The same form as SECTIONED_SCHEMA, with the sections listed out of their `order` and the review section without one:
# the webapp sorts the sections with an `order` first and keeps the others in list order after them.
SHUFFLED_SCHEMA = {
    **SECTIONED_SCHEMA,
    "sections": [
        {"id": "review", "title": "Review & set up", "groups": []},
        {
            "id": "credentials",
            "title": "Credentials",
            "order": 1,
            "groups": ["auth"],
            "check": True,
        },
        {
            "id": "connection",
            "title": "Connection",
            "order": 0,
            "groups": ["db", "security"],
            "check": True,
        },
    ],
}
SHUFFLED_SPEC = ConnectorSpecification(connectionSpecification=SHUFFLED_SCHEMA)


def test_declared_sections_follow_order_not_list_position():
    assert declared_sections(SHUFFLED_SPEC) == ["connection", "credentials", "review"]


def test_sections_without_order_keep_their_list_position_after_the_ordered_ones():
    spec = ConnectorSpecification(
        connectionSpecification={
            "sections": [
                {"id": "c", "groups": []},
                {"id": "b", "order": 5, "groups": []},
                {"id": "d", "groups": []},
                {"id": "a", "order": 2, "groups": []},
            ]
        }
    )

    assert declared_sections(spec) == ["a", "b", "c", "d"]


def test_is_last_section_follows_order_not_list_position():
    assert is_last_section(SHUFFLED_SPEC, "review")
    assert not is_last_section(SHUFFLED_SPEC, "connection")
    assert not is_last_section(SHUFFLED_SPEC, "credentials")


def test_section_spec_for_the_first_section_in_form_order_requires_its_own_fields_only():
    """The webapp sends the first step's fields alone, even when the spec lists that section later."""
    narrowed = section_spec(SHUFFLED_SPEC, "connection").connectionSpecification

    assert narrowed["required"] == ["host"]


def test_section_spec_for_a_later_section_in_form_order_requires_the_earlier_fields_too():
    narrowed = section_spec(SHUFFLED_SPEC, "credentials").connectionSpecification

    assert narrowed["required"] == ["host", "username", "password"]


def test_section_spec_for_the_last_section_in_form_order_is_the_full_spec():
    assert section_spec(SHUFFLED_SPEC, "review") is SHUFFLED_SPEC


def test_checkable_sections_are_the_ones_declaring_check():
    assert checkable_sections(SECTIONED_SPEC) == ["connection", "credentials"]
    assert checkable_sections(FLAT_SPEC) == []


def test_declared_sections_is_empty_without_sections():
    assert declared_sections(FLAT_SPEC) == []
    assert (
        declared_sections(
            ConnectorSpecification(connectionSpecification={"sections": [{"title": "no id"}]})
        )
        == []
    )


def test_requested_section_is_none_without_the_key():
    assert requested_section({"host": "h"}, SECTIONED_SPEC) is None


def test_requested_section_rejects_a_null_section():
    with pytest.raises(AirbyteTracedException) as exc_info:
        requested_section({CHECK_SECTION_KEY: None, "host": "h"}, SECTIONED_SPEC)
    assert exc_info.value.failure_type == FailureType.config_error
    assert exc_info.value.message == "'check-section' must be a non-empty string."


@pytest.mark.parametrize("section", ["connection", "credentials"])
def test_requested_section_resolves_a_checkable_section(section):
    assert requested_section({CHECK_SECTION_KEY: section, "host": "h"}, SECTIONED_SPEC) == section


def test_requested_section_rejects_a_section_without_check():
    with pytest.raises(AirbyteTracedException) as exc_info:
        requested_section({CHECK_SECTION_KEY: "review"}, SECTIONED_SPEC)
    assert exc_info.value.failure_type == FailureType.config_error
    assert (
        exc_info.value.message
        == "Section 'review' has no check: the connector specification does not declare \"check\": true for it."
    )


def test_requested_section_rejects_an_unknown_section():
    with pytest.raises(AirbyteTracedException) as exc_info:
        requested_section({CHECK_SECTION_KEY: "sync"}, SECTIONED_SPEC)
    assert exc_info.value.failure_type == FailureType.config_error
    assert (
        exc_info.value.message
        == "Unknown check-section 'sync'; the connector specification declares: connection, credentials, review."
    )


def test_requested_section_rejects_a_connector_without_sections():
    with pytest.raises(AirbyteTracedException) as exc_info:
        requested_section({CHECK_SECTION_KEY: "connection"}, FLAT_SPEC)
    assert exc_info.value.failure_type == FailureType.config_error
    assert (
        exc_info.value.message
        == "'check-section' is not supported: the connector specification declares no sections."
    )


@pytest.mark.parametrize("value", [1, "", " ", ["connection"], {"id": "connection"}])
def test_requested_section_rejects_a_non_string_section(value):
    with pytest.raises(AirbyteTracedException) as exc_info:
        requested_section({CHECK_SECTION_KEY: value}, SECTIONED_SPEC)
    assert exc_info.value.failure_type == FailureType.config_error
    assert exc_info.value.message == "'check-section' must be a non-empty string."


def test_is_last_section():
    assert is_last_section(SECTIONED_SPEC, "review")
    assert not is_last_section(SECTIONED_SPEC, "connection")
    assert not is_last_section(FLAT_SPEC, "review")


def test_strip_check_section_removes_the_key_only():
    config = {CHECK_SECTION_KEY: "connection", "host": "h", "port": 0}
    assert strip_check_section(config) == {"host": "h", "port": 0}
    assert strip_check_section({"host": "h"}) == {"host": "h"}


def test_section_spec_for_the_first_section_requires_its_own_fields_only():
    narrowed = section_spec(SECTIONED_SPEC, "connection")

    assert narrowed.connectionSpecification["required"] == ["host"]
    assert narrowed.connectionSpecification["properties"] == SECTIONED_SCHEMA["properties"]
    assert narrowed.connectionSpecification["sections"] == SECTIONED_SCHEMA["sections"]
    # The full specification is left untouched.
    assert SECTIONED_SPEC.connectionSpecification["required"] == [
        "host",
        "username",
        "password",
        "legacy",
    ]


def test_section_spec_for_a_middle_section_requires_the_earlier_fields_too():
    narrowed = section_spec(SECTIONED_SPEC, "credentials")

    assert narrowed.connectionSpecification["required"] == ["host", "username", "password"]


def test_section_spec_for_the_last_section_is_the_full_spec():
    assert section_spec(SECTIONED_SPEC, "review") is SECTIONED_SPEC


def test_section_spec_keeps_the_other_spec_fields():
    spec = ConnectorSpecification(
        documentationUrl="https://docs.airbyte.com/integrations/sources/x",
        connectionSpecification=SECTIONED_SCHEMA,
    )

    narrowed = section_spec(spec, "connection")

    assert narrowed.documentationUrl == spec.documentationUrl


CROSS_PROPERTY_SCHEMA = {
    **SECTIONED_SCHEMA,
    "dependentRequired": {"host": ["username"]},
    "dependencies": {"port": ["password"]},
    "allOf": [{"required": ["password"]}],
    "anyOf": [{"required": ["username"]}, {"required": ["password"]}],
    "oneOf": [{"required": ["username"]}],
    "not": {"required": ["ssl"]},
    "if": {"properties": {"host": {"const": "h"}}},
    "then": {"required": ["password"]},
    "else": {"required": ["username"]},
    "dependentSchemas": {"host": {"required": ["password"]}},
}
CROSS_PROPERTY_SPEC = ConnectorSpecification(connectionSpecification=CROSS_PROPERTY_SCHEMA)


def test_section_spec_drops_root_constraints_which_can_require_later_fields():
    from jsonschema import validate

    narrowed = section_spec(CROSS_PROPERTY_SPEC, "connection").connectionSpecification

    assert not set(narrowed) & set(CROSS_PROPERTY_KEYWORDS)
    assert narrowed["required"] == ["host"]
    # The partial config of the first section passes, while its own fields are still validated.
    validate(instance={"host": "h", "port": 5432}, schema=narrowed)
    with pytest.raises(Exception, match="'port'|not of type"):
        validate(instance={"host": "h", "port": "not-a-port"}, schema=narrowed)
    # The full specification keeps every constraint.
    assert all(
        keyword in CROSS_PROPERTY_SPEC.connectionSpecification
        for keyword in CROSS_PROPERTY_KEYWORDS
    )
    assert section_spec(CROSS_PROPERTY_SPEC, "review") is CROSS_PROPERTY_SPEC
