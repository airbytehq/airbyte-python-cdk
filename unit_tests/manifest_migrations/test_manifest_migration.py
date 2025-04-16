#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.manifest_migrations.migration_handler import (
    ManifestMigrationHandler,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from airbyte_cdk.sources.declarative.parsers.manifest_reference_resolver import (
    ManifestReferenceResolver,
)

resolver = ManifestReferenceResolver()


def test_manifest_resolve_migrate(
    manifest_with_url_base_to_migrate_to_url,
    expected_manifest_with_url_base_migrated_to_url,
) -> None:
    """
    This test is to check that the manifest is migrated and normalized
    when the `url_base` is migrated to `url` and the `path` is joined to `url`.
    """

    resolved_manifest = resolver.preprocess_manifest(manifest_with_url_base_to_migrate_to_url)
    migrated_manifest = ManifestMigrationHandler(dict(resolved_manifest)).apply_migrations()

    assert migrated_manifest == expected_manifest_with_url_base_migrated_to_url


def test_manifest_resolve_do_not_migrate(
    manifest_with_migrated_url_base_and_path_is_joined_to_url,
) -> None:
    """
    This test is to check that the manifest remains migrated already,
    after the `url_base` and `path` is joined to `url`.
    """

    resolved_manifest = resolver.preprocess_manifest(
        manifest_with_migrated_url_base_and_path_is_joined_to_url
    )
    migrated_manifest = ManifestMigrationHandler(dict(resolved_manifest)).apply_migrations()

    # it's expected that the manifest is the same after the processing
    assert migrated_manifest == manifest_with_migrated_url_base_and_path_is_joined_to_url
