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


def test_with_linked_definitions_url_base_authenticator_when_multiple_streams_reference_the_same_schema(
    manifest_with_linked_definitions_url_base_authenticator_abnormal_schemas,
    expected_manifest_with_linked_definitions_url_base_authenticator_normalized,
) -> None:
    """
    This test is to check that the manifest is normalized when the `url_base` and the `authenticator` is linked
    between the definitions and the `url_base` is present in the manifest.
    The `authenticator` is not a normal schema, but a reference to another schema.

    The test also verifies the `stream.schema_loader.schema` is properly extracted to
    the `schemas.<stream_name>`.
    """

    schema = _get_declarative_component_schema()
    resolved_manifest = resolver.preprocess_manifest(
        manifest_with_linked_definitions_url_base_authenticator_abnormal_schemas
    )
    normalized_manifest = ManifestNormalizer(resolved_manifest, schema).normalize()

    assert (
        normalized_manifest
        == expected_manifest_with_linked_definitions_url_base_authenticator_normalized
    )
