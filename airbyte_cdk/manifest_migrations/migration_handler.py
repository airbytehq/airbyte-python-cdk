#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import logging
from typing import Type

from packaging.version import Version

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

LOGGER = logging.getLogger("airbyte.cdk.manifest_migrations")


class ManifestMigrationHandler:
    """
    This class is responsible for handling migrations in the manifest.
    """

    def __init__(self, manifest: ManifestType) -> None:
        self._manifest = manifest
        self._migrated_manifest: ManifestType = copy.deepcopy(self._manifest)
        self._manifest_version: Version = self._get_manifest_version()

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
            migration_instance = migration_class()
            # check if the migration is supported for the given manifest version
            if self._manifest_version <= migration_instance.migration_version:
                migration_instance._process_manifest(self._migrated_manifest)
            else:
                LOGGER.info(
                    f"Manifest migration: `{migration_class.__name__}` is not supported for the given manifest version `{self._manifest_version}`.",
                )
        except Exception as e:
            raise ManifestMigrationException(str(e)) from e

    def _get_manifest_version(self) -> Version:
        """
        Get the manifest version from the manifest.

        :param manifest: The manifest to get the version from
        :return: The manifest version
        """
        return Version(str(self._migrated_manifest.get("version", "0.0.0")))
