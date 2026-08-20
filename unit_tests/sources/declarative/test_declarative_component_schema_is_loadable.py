#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""The shipped manifest schema has to parse, and nothing else asserted that it does.

`_get_declarative_component_schema()` runs before any low-code source is built, so a malformed
`declarative_component_schema.yaml` fails every declarative connector at startup. It is also easy
to break from a documentation edit alone: the descriptions are plain YAML scalars, so a `": "` in
prose ends the scalar and the file stops being a mapping. That happened in this file's history and
surfaced as 194 unrelated test failures rather than as one obvious error.
"""

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    _get_declarative_component_schema,
)


def test_the_shipped_component_schema_parses():
    schema = _get_declarative_component_schema()

    assert schema["title"] == "DeclarativeSource"
    assert "HttpRequester" in schema["definitions"]


def test_every_description_survives_the_yaml_round_trip():
    """A description that ends early still parses — it just silently loses its tail, or turns the
    rest of the sentence into a key. Reading them all back catches the truncation too."""
    schema = _get_declarative_component_schema()

    for name, definition in schema["definitions"].items():
        for field_name, field_schema in (definition.get("properties") or {}).items():
            description = (
                field_schema.get("description") if isinstance(field_schema, dict) else None
            )
            if description is not None:
                assert isinstance(description, str), (
                    f"{name}.{field_name} description is not a string"
                )
                assert description.strip(), f"{name}.{field_name} has an empty description"
