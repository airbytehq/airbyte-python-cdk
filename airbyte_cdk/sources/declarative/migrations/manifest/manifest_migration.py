# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import re
from abc import abstractmethod
from typing import Any, Dict

ManifestType = Dict[str, Any]


TYPE_TAG = "type"

NON_MIGRATABLE_TYPES = [
    "DynamicDeclarativeStream",
]


class ManifestMigration:
    @abstractmethod
    def should_migrate(self, manifest: ManifestType) -> bool:
        """
        Check if the manifest should be migrated.

        :param manifest: The manifest to potentially migrate
        :param kwargs: Additional arguments for migration

        :return: true if the manifest is of the expected format and should be migrated. False otherwise.
        """

    @abstractmethod
    def migrate(self, manifest: ManifestType) -> None:
        """
        Migrate the manifest. Assumes should_migrate(manifest) returned True.

        :param manifest: The manifest to migrate
        :param kwargs: Additional arguments for migration
        """

    @property
    def migration_version(self) -> str:
        """
        Get the migration version.

        :return: The migration version as a string
        """
        return self._get_migration_version()

    def _is_component(self, obj: Dict[str, Any]) -> bool:
        """
        Check if the object is a component.

        :param obj: The object to check
        :return: True if the object is a component, False otherwise
        """
        return TYPE_TAG in obj.keys()

    def _is_migratable(self, obj: Dict[str, Any]) -> bool:
        """
        Check if the object is a migratable component,
        based on the Type of the component and the migration version.

        :param obj: The object to check
        :return: True if the object is a migratable component, False otherwise
        """
        return (
            obj[TYPE_TAG] not in NON_MIGRATABLE_TYPES
            and self._get_manifest_version(obj) <= self.migration_version
        )

    def _process_manifest(self, obj: Any) -> None:
        """
        Recursively processes a manifest object, migrating components that match the migration criteria.

        This method traverses the entire manifest structure (dictionaries and lists) and applies
        migrations to components that:
        1. Have a type tag
        2. Are not in the list of non-migratable types
        3. Meet the conditions defined in the should_migrate method

        Parameters:
            obj (Any): The object to process, which can be a dictionary, list, or any other type.
                       Dictionary objects are checked for component type tags and potentially migrated.
                       List objects have each of their items processed recursively.
                       Other types are ignored.

        Returns:
            None, since we process the manifest in place.
        """
        if isinstance(obj, dict):
            # Check if the object is a component
            if self._is_component(obj):
                # Check if the object is allowed to be migrated
                if not self._is_migratable(obj):
                    return

                # Check if the object should be migrated
                if self.should_migrate(obj):
                    # Perform the migration, if needed
                    self.migrate(obj)

            # Process all values in the dictionary
            for value in list(obj.values()):
                self._process_manifest(value)

        elif isinstance(obj, list):
            # Process all items in the list
            for item in obj:
                self._process_manifest(item)

    def _get_manifest_version(self, manifest: ManifestType) -> str:
        """
        Get the manifest version from the manifest.

        :param manifest: The manifest to get the version from
        :return: The manifest version
        """
        return manifest.get("version", "0.0.0")

    def _get_migration_version(self) -> str:
        """
        Get the migration version from the class name.
        The migration version is extracted from the class name using a regular expression.
        The expected format is "V_<major>_<minor>_<patch>_<migration_name>".

        For example, "V_6_45_2_ManifestMigration_HttpRequesterPathToUrl" -> "6.45.2"

        :return: The migration version as a string in the format "major.minor.patch"
        :raises ValueError: If the class name does not match the expected format
        """

        class_name = self.__class__.__name__
        migration_version = re.search(r"V_(\d+_\d+_\d+)", class_name)
        if migration_version:
            return migration_version.group(1).replace("_", ".")
        else:
            raise ValueError(
                f"Invalid migration class name, make sure the class name has the version (e.g `V_0_0_0_`): {class_name}"
            )
