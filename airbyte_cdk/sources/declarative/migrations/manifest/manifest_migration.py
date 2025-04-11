# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

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
            obj_keys = obj.keys()
            # check for component type match the designed migration
            if TYPE_TAG in obj_keys:
                obj_type = obj[TYPE_TAG]

                # do not migrate if the particular type is in the list of non-migratable types
                if obj_type in NON_MIGRATABLE_TYPES:
                    return

                if self.should_migrate(obj):
                    self.migrate(obj)

            # Process all values in the dictionary
            for v in list(obj.values()):
                self._process_manifest(v)
        elif isinstance(obj, list):
            # Process all items in the list
            for item in obj:
                self._process_manifest(item)
