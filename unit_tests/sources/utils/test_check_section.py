#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.models import ConnectorSpecification, FailureType
from airbyte_cdk.sources.utils.check_section import (
    CHECK_SECTION_KEY,
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
        {"id": "connection", "title": "Connection", "order": 0, "groups": ["db", "security"]},
        {"id": "credentials", "title": "Credentials", "order": 1, "groups": ["auth"]},
        {"id": "review", "title": "Review & set up", "order": 2, "groups": []},
    ],
}
SECTIONED_SPEC = ConnectorSpecification(connectionSpecification=SECTIONED_SCHEMA)
FLAT_SPEC = ConnectorSpecification(
    connectionSpecification={"type": "object", "properties": {"host": {"type": "string"}}}
)


def test_declared_sections_are_in_form_order():
    assert declared_sections(SECTIONED_SPEC) == ["connection", "credentials", "review"]


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


@pytest.mark.parametrize("section", ["connection", "credentials", "review"])
def test_requested_section_resolves_a_declared_section(section):
    assert requested_section({CHECK_SECTION_KEY: section, "host": "h"}, SECTIONED_SPEC) == section


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
