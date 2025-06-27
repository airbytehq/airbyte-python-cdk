#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    _get_declarative_component_schema,
)
from airbyte_cdk.sources.declarative.parsers.manifest_normalizer import (
    ManifestNormalizer,
)
from airbyte_cdk.sources.declarative.parsers.manifest_reference_resolver import (
    ManifestReferenceResolver,
)

resolver = ManifestReferenceResolver()


def test_when_multiple_url_base_are_resolved_and_most_frequent_is_shared(
    manifest_with_multiple_url_base,
    expected_manifest_with_multiple_url_base_normalized,
) -> None:
    """
    This test is to check that the manifest is normalized when multiple url_base are resolved
    and the most frequent one is shared.
    """

    schema = _get_declarative_component_schema()
    resolved_manifest = resolver.preprocess_manifest(manifest_with_multiple_url_base)
    normalized_manifest = ManifestNormalizer(resolved_manifest, schema).normalize()

    assert normalized_manifest == expected_manifest_with_multiple_url_base_normalized


def test_with_shared_definitions_url_base_are_present(
    manifest_with_url_base_linked_definition,
    expected_manifest_with_url_base_linked_definition_normalized,
) -> None:
    """
    This test is to check that the manifest is normalized when the `url_base` is shared
    between the definitions and the `url_base` is present in the manifest.
    """

    schema = _get_declarative_component_schema()
    resolved_manifest = resolver.preprocess_manifest(manifest_with_url_base_linked_definition)
    normalized_manifest = ManifestNormalizer(resolved_manifest, schema).normalize()

    assert normalized_manifest == expected_manifest_with_url_base_linked_definition_normalized


def test_clean_dangling_fields_removes_unreferenced_definitions() -> None:
    """
    Test that unreferenced definitions are removed while referenced ones are kept.
    """
    manifest = {
        "definitions": {
            "referenced": {"type": "object", "properties": {"a": 1}},
            "unreferenced": {"type": "object", "properties": {"b": 2}},
        },
        "streams": [
            {
                "name": "stream1",
                "type": "object",
                "properties": {"def": {"$ref": "#/definitions/referenced"}},
            }
        ],
    }
    expected = {
        "definitions": {"referenced": {"type": "object", "properties": {"a": 1}}},
        "streams": [
            {
                "name": "stream1",
                "type": "object",
                "properties": {"def": {"$ref": "#/definitions/referenced"}},
            }
        ],
    }
    schema = _get_declarative_component_schema()
    normalizer = ManifestNormalizer(manifest, schema)
    normalizer._clean_dangling_fields()
    assert normalizer._normalized_manifest == expected


def test_clean_dangling_fields_removes_unreferenced_schemas() -> None:
    """
    Test that unreferenced schemas are removed while referenced ones are kept.
    """
    manifest = {
        "schemas": {
            "referenced": {"type": "object", "properties": {"a": 1}},
            "unreferenced": {"type": "object", "properties": {"b": 2}},
        },
        "streams": [
            {"name": "stream1", "schema_loader": {"schema": {"$ref": "#/schemas/referenced"}}}
        ],
    }
    expected = {
        "schemas": {"referenced": {"type": "object", "properties": {"a": 1}}},
        "streams": [
            {"name": "stream1", "schema_loader": {"schema": {"$ref": "#/schemas/referenced"}}}
        ],
    }
    schema = _get_declarative_component_schema()
    normalizer = ManifestNormalizer(manifest, schema)
    normalizer._clean_dangling_fields()
    assert normalizer._normalized_manifest == expected


def test_clean_dangling_fields_keeps_parent_paths() -> None:
    """
    Test that parent paths of referenced fields are kept.
    """
    manifest = {
        "definitions": {
            "parent": {"child": {"grandchild": {"type": "object", "properties": {"a": 1}}}}
        },
        "streams": [
            {
                "name": "stream1",
                "type": "object",
                "properties": {"def": {"$ref": "#/definitions/parent/child/grandchild"}},
            }
        ],
    }
    expected = {
        "definitions": {
            "parent": {"child": {"grandchild": {"type": "object", "properties": {"a": 1}}}}
        },
        "streams": [
            {
                "name": "stream1",
                "type": "object",
                "properties": {"def": {"$ref": "#/definitions/parent/child/grandchild"}},
            }
        ],
    }
    schema = _get_declarative_component_schema()
    normalizer = ManifestNormalizer(manifest, schema)
    normalizer._clean_dangling_fields()
    assert normalizer._normalized_manifest == expected


def test_clean_dangling_fields_removes_empty_sections() -> None:
    """
    Test that empty sections are removed after cleaning.
    """
    manifest = {
        "definitions": {"unreferenced": {"type": "object", "properties": {"b": 2}}},
        "schemas": {"unreferenced": {"type": "object", "properties": {"b": 2}}},
        "streams": [{"name": "stream1", "type": "object", "properties": {}}],
    }
    expected = {"streams": [{"name": "stream1", "type": "object", "properties": {}}]}
    schema = _get_declarative_component_schema()
    normalizer = ManifestNormalizer(manifest, schema)
    normalizer._clean_dangling_fields()
    assert normalizer._normalized_manifest == expected


def test_replace_parent_streams_with_refs_replaces_with_ref():
    """
    If a parent_stream_config's stream field matches another stream object, it should be replaced with a $ref to the correct index.
    """
    stream_a = {"name": "A", "type": "DeclarativeStream", "retriever": {}}
    stream_b = {"name": "B", "type": "DeclarativeStream", "retriever": {}}
    manifest = {
        "streams": [
            stream_a,
            stream_b,
            {
                "name": "C",
                "type": "DeclarativeStream",
                "retriever": {
                    "partition_router": {
                        "type": "SubstreamPartitionRouter",
                        "parent_stream_configs": [
                            {
                                "stream": stream_a.copy(),
                                "type": "ParentStreamConfig",
                                "parent_key": "id",
                                "partition_field": "aid",
                            },
                            {
                                "stream": stream_b.copy(),
                                "type": "ParentStreamConfig",
                                "parent_key": "id",
                                "partition_field": "bid",
                            },
                        ],
                    }
                },
            },
        ]
    }
    expected = {
        "streams": [
            stream_a,
            stream_b,
            {
                "name": "C",
                "type": "DeclarativeStream",
                "retriever": {
                    "partition_router": {
                        "type": "SubstreamPartitionRouter",
                        "parent_stream_configs": [
                            {
                                "stream": {"$ref": "#/streams/0"},
                                "type": "ParentStreamConfig",
                                "parent_key": "id",
                                "partition_field": "aid",
                            },
                            {
                                "stream": {"$ref": "#/streams/1"},
                                "type": "ParentStreamConfig",
                                "parent_key": "id",
                                "partition_field": "bid",
                            },
                        ],
                    }
                },
            },
        ]
    }
    schema = _get_declarative_component_schema()
    normalizer = ManifestNormalizer(manifest, schema)
    normalizer._replace_parent_streams_with_refs()
    assert normalizer._normalized_manifest == expected


def test_replace_parent_streams_with_refs_no_match():
    """
    If a parent_stream_config's stream field does not match any stream object, it should not be replaced.
    """
    stream_a = {"name": "A", "type": "DeclarativeStream", "retriever": {}}
    unrelated_stream = {"name": "X", "type": "DeclarativeStream", "retriever": {"foo": 1}}
    manifest = {
        "streams": [
            stream_a,
            {
                "name": "C",
                "type": "DeclarativeStream",
                "retriever": {
                    "partition_router": {
                        "type": "SubstreamPartitionRouter",
                        "parent_stream_configs": [
                            {
                                "stream": unrelated_stream.copy(),
                                "type": "ParentStreamConfig",
                                "parent_key": "id",
                                "partition_field": "aid",
                            },
                        ],
                    }
                },
            },
        ]
    }
    expected = {
        "streams": [
            stream_a,
            {
                "name": "C",
                "type": "DeclarativeStream",
                "retriever": {
                    "partition_router": {
                        "type": "SubstreamPartitionRouter",
                        "parent_stream_configs": [
                            {
                                "stream": unrelated_stream.copy(),
                                "type": "ParentStreamConfig",
                                "parent_key": "id",
                                "partition_field": "aid",
                            },
                        ],
                    }
                },
            },
        ]
    }
    schema = _get_declarative_component_schema()
    normalizer = ManifestNormalizer(manifest, schema)
    normalizer._replace_parent_streams_with_refs()
    assert normalizer._normalized_manifest == expected


def test_replace_parent_streams_with_refs_handles_multiple_partition_routers():
    """
    If there are multiple partition_routers (as a list), all should be checked.
    """
    stream_a = {"name": "A", "type": "DeclarativeStream", "retriever": {}}
    stream_b = {"name": "B", "type": "DeclarativeStream", "retriever": {}}
    manifest = {
        "streams": [
            stream_a,
            stream_b,
            {
                "name": "C",
                "type": "DeclarativeStream",
                "retriever": {
                    "partition_router": [
                        {
                            "type": "SubstreamPartitionRouter",
                            "parent_stream_configs": [
                                {
                                    "stream": stream_a.copy(),
                                    "type": "ParentStreamConfig",
                                    "parent_key": "id",
                                    "partition_field": "aid",
                                },
                            ],
                        },
                        {
                            "type": "SubstreamPartitionRouter",
                            "parent_stream_configs": [
                                {
                                    "stream": stream_b.copy(),
                                    "type": "ParentStreamConfig",
                                    "parent_key": "id",
                                    "partition_field": "bid",
                                },
                            ],
                        },
                    ]
                },
            },
        ]
    }
    expected = {
        "streams": [
            stream_a,
            stream_b,
            {
                "name": "C",
                "type": "DeclarativeStream",
                "retriever": {
                    "partition_router": [
                        {
                            "type": "SubstreamPartitionRouter",
                            "parent_stream_configs": [
                                {
                                    "stream": {"$ref": "#/streams/0"},
                                    "type": "ParentStreamConfig",
                                    "parent_key": "id",
                                    "partition_field": "aid",
                                },
                            ],
                        },
                        {
                            "type": "SubstreamPartitionRouter",
                            "parent_stream_configs": [
                                {
                                    "stream": {"$ref": "#/streams/1"},
                                    "type": "ParentStreamConfig",
                                    "parent_key": "id",
                                    "partition_field": "bid",
                                },
                            ],
                        },
                    ]
                },
            },
        ]
    }
    schema = _get_declarative_component_schema()
    normalizer = ManifestNormalizer(manifest, schema)
    normalizer._replace_parent_streams_with_refs()
    assert normalizer._normalized_manifest == expected
