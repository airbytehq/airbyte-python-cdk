#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from unittest.mock import patch

from freezegun import freeze_time

from airbyte_cdk.manifest_migrations import migrations_registry
from airbyte_cdk.manifest_migrations.migration_handler import (
    ManifestMigrationHandler,
)
from airbyte_cdk.manifest_migrations.migrations import (
    HttpRequesterPathToUrl,
    HttpRequesterRequestBodyJsonDataToRequestBody,
    HttpRequesterUrlBaseToUrl,
)
from airbyte_cdk.sources.declarative.parsers.manifest_reference_resolver import (
    ManifestReferenceResolver,
)
from unit_tests.manifest_migrations.conftest import DummyMigration

resolver = ManifestReferenceResolver()


@freeze_time("2025-04-01")
@patch.dict(
    migrations_registry.MANIFEST_MIGRATIONS,
    {
        ">=6.48.2,<6.50.0": [
            HttpRequesterUrlBaseToUrl,
            HttpRequesterPathToUrl,
            HttpRequesterRequestBodyJsonDataToRequestBody,
        ]
    },
    clear=True,
)
def test_manifest_resolve_migrate_url_base_and_path_to_url(
    manifest_with_url_base_to_migrate_to_url,
    expected_manifest_with_url_base_migrated_to_url,
) -> None:
    """
    This test is to check that the manifest is migrated and normalized
    when the `url_base` is migrated to `url` and the `path` is joined to `url`.
    """

    resolved_manifest = ManifestReferenceResolver().preprocess_manifest(
        manifest_with_url_base_to_migrate_to_url
    )
    migrated_manifest = ManifestMigrationHandler(dict(resolved_manifest)).apply_migrations()

    assert migrated_manifest == expected_manifest_with_url_base_migrated_to_url


@freeze_time("2025-04-01")
def test_manifest_resolve_migrate_request_body_json_and_data_to_request_body(
    manifest_with_request_body_json_and_data_to_migrate_to_request_body,
    expected_manifest_with_migrated_to_request_body,
) -> None:
    """
    This test is to check that the manifest is migrated correctly,
    after the `request_body_json` and `request_body_data` are migrated to `request_body`.
    """

    resolved_manifest = resolver.preprocess_manifest(
        manifest_with_request_body_json_and_data_to_migrate_to_request_body
    )
    migrated_manifest = ManifestMigrationHandler(dict(resolved_manifest)).apply_migrations()

    assert migrated_manifest == expected_manifest_with_migrated_to_request_body


@freeze_time("2025-04-01")
@patch.dict(migrations_registry.MANIFEST_MIGRATIONS, {"0.0.0": [DummyMigration]}, clear=True)
def test_manifest_resolve_do_not_migrate(
    manifest_with_migrated_url_base_and_path_is_joined_to_url,
) -> None:
    """
    This test is to check that the manifest remains migrated already,
    after the `url_base` and `path` is joined to `url`.
    """

    resolved_manifest = ManifestReferenceResolver().preprocess_manifest(
        manifest_with_migrated_url_base_and_path_is_joined_to_url
    )
    migrated_manifest = ManifestMigrationHandler(dict(resolved_manifest)).apply_migrations()

    # it's expected that the manifest is the same after the processing, because the manifest version is higher.
    assert migrated_manifest == manifest_with_migrated_url_base_and_path_is_joined_to_url
