#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
from typing import Type

from airbyte_cdk.manifest_migrations.exceptions import (
    ManifestMigrationException,
)
from airbyte_cdk.manifest_migrations.manifest_migration import (
    ManifestMigration,
    ManifestType,
)
from airbyte_cdk.manifest_migrations.migrations_registry import (
    MIGRATIONS,
)


class ManifestMigrationHandler:
    """
    This class is responsible for handling migrations in the manifest.
    """

    def __init__(self, manifest: ManifestType) -> None:
        self._manifest = manifest
        self._migrated_manifest: ManifestType = copy.deepcopy(self._manifest)

    def apply_migrations(self) -> ManifestType:
        """
        Apply all registered migrations to the manifest.

        This method iterates through all migrations in the migrations registry and applies
        them sequentially to the current manifest. If any migration fails with a
        ManifestMigrationException, the original unmodified manifest is returned instead.

        Returns:
            ManifestType: The migrated manifest if all migrations succeeded, or the original
                          manifest if any migration failed.
        """
        try:
            for migration_cls in MIGRATIONS:
                self._handle_migration(migration_cls)
            return self._migrated_manifest
        except ManifestMigrationException:
            # if any errors occur we return the original resolved manifest
            return self._manifest

    def _handle_migration(self, migration_class: Type[ManifestMigration]) -> None:
        """
        Handles a single manifest migration by instantiating the migration class and processing the manifest.

        Args:
            migration_class (Type[ManifestMigration]): The migration class to apply to the manifest.

        Raises:
            ManifestMigrationException: If the migration process encounters any errors.
        """
        try:
            migration_class()._process_manifest(self._migrated_manifest)
        except Exception as e:
            raise ManifestMigrationException(f"Failed to migrate the manifest: {e}") from e
