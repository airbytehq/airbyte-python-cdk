#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from airbyte_cdk.sources.declarative.parsers.custom_exceptions import ManifestMigrationException
from airbyte_cdk.sources.types import EmptyString

# Type definitions for better readability
ManifestType = Dict[str, Any]
DefinitionsType = Dict[str, Any]
MigrationsType = List[Tuple[str, str, Optional[str]]]
MigrationType = Tuple[str, str, Optional[str]]
MigratedTagsType = Dict[str, List[Tuple[str, str, Optional[str]]]]
MigrationFunctionType = Callable[[Any, MigrationType], None]


# Configuration constants
TYPE_TAG = "type"
DEF_TAG = "definitions"
MIGRATIONS_TAG = "migrations"
ORIGINAL_KEY = "original_key"
REPLACEMENT_KEY = "replacement_key"


# disable migrations for these types
NON_MIGRATABLE_TYPES = [
    "DynamicDeclarativeStream",
]


class ManifestMigrationHandler:
    """
    This class is responsible for handling migrations in the manifest.
    It provides methods to migrate migrated fields and values to their new equivalents.
    """

    @property
    def _migration_type_mapping(self) -> Dict[str, MigrationFunctionType]:
        """
        Returns a mapping of migration types to their handler functions.

        This method defines how different types of migrations should be handled by mapping
        migration type identifiers to their corresponding handler methods.

        Returns:
            Dict[str, MigrationFunctionType]: A dictionary mapping migration types to handler functions:
                - "replace_field": Handler for replacing migrated fields with new ones
                - "remove_field": Handler for removing migrated fields
                - "handle_url_parts": Handler for processing URL parts
        """

        return {
            "replace_field": self._replace_migrated_field,
            "remove_field": self._remove_migrated_field,
            # the specific type of migration, handles the url parts and verifies the url is correct
            "handle_url_parts": self._handle_url_parts,
        }

    def __init__(
        self,
        manifest: ManifestType,
        declarative_schema: DefinitionsType,
    ) -> None:
        self._manifest = manifest
        self._declarative_schema = declarative_schema

        self._migrated_manifest: ManifestType = copy.deepcopy(self._manifest)
        # get the declared migrations from schema
        self._migration_tags = self._get_migration_schema_tags(self._declarative_schema)

    def migrate(self) -> ManifestType:
        try:
            for component_type, migrations in self._migration_tags.items():
                self._handle_migrations(component_type, migrations)
            return self._migrated_manifest
        except ManifestMigrationException as e:
            # if any errors occurs we return the original resolved manifest
            return self._manifest

    def _get_migration_schema_tags(self, schema: DefinitionsType) -> MigratedTagsType:
        """
        Extracts sharable tags from schema definitions.
        This function identifies properties within a schema's definitions that have the `migrations` object.

        Args:
            schema (DefinitionsType): The schema definition dictionary to process

        Returns:
            migrations_tags: A set of migrated tags found in the schema definitions.
        """

        # the migrated tags scope: ['definitions.*']
        schema_definitions = schema.get(DEF_TAG, {})
        migrations_tags: MigratedTagsType = {}

        for component_name, component_declaration in schema_definitions.items():
            if MIGRATIONS_TAG in component_declaration.keys():
                # create the placeholder for the migrations
                migrations_tags[component_name] = []
                # iterate over the migrations
                for migration in component_declaration[MIGRATIONS_TAG]:
                    migrations_tags[component_name].append(
                        (
                            # type of migration
                            migration.get(TYPE_TAG),
                            # what is the migrated key
                            migration.get(ORIGINAL_KEY),
                            # (optional) what is the new key to be used
                            migration.get(REPLACEMENT_KEY),
                        ),
                    )

        return migrations_tags

    def _handle_migrations(
        self,
        component_type: str,
        migrations: MigrationsType,
    ) -> None:
        """
        Recursively replaces all occurrences of migrated_key with new_key in the normalized manifest.

        The structure of the `migration` Tuple is:
            (
                migration[TYPE_TAG] -- type of migration,
                migration["original_key"] -- what is the migrated key,
                migration["replacement_key"] -- what is the new key to be used,
            )
        """
        try:
            for migration in migrations:
                self._process_migration(self._migrated_manifest, component_type, migration)
        except Exception as e:
            raise ManifestMigrationException(f"Failed to migrate the manifest: {e}") from e

    def _process_migration(self, obj: Any, component_type: str, migration: MigrationType) -> None:
        migration_type, migrated_key, _ = migration

        if isinstance(obj, dict):
            obj_keys = obj.keys()

            # check for component type match the designed migration
            if TYPE_TAG in obj_keys:
                obj_type = obj[TYPE_TAG]
                # do not migrate if the type is not in the list of migratable types
                if obj_type in NON_MIGRATABLE_TYPES:
                    return
                if obj_type == component_type and migrated_key in obj_keys:
                    if migration_type in self._migration_type_mapping.keys():
                        # Call the appropriate function based on the migration type
                        self._migration_type_mapping[migration_type](obj, migration)

            # Process all values in the dictionary
            for v in list(obj.values()):
                self._process_migration(v, component_type, migration)

        elif isinstance(obj, list):
            # Process all items in the list
            for item in obj:
                self._process_migration(item, component_type, migration)

    ## Migration Functions
    def _replace_migrated_field(
        self,
        obj: Any,
        migration: MigrationType,
    ) -> None:
        """
        Replaces the migrated field with the new field in the object.
        The value of the migrated field is copied to the new field.
        """
        _, original_key, replacement_key = migration

        obj[replacement_key] = obj[original_key]
        obj.pop(original_key, None)

    def _handle_url_parts(
        self,
        obj: Any,
        migration: MigrationType,
    ) -> None:
        """
        Handles the migration of URL parts by joining the original key with the replacement key.
        The value of the original key is joined with the replacement key to form a full URL.
        """
        _, original_key, replacement_key = migration

        original_key_value = obj[original_key].lstrip("/")
        replacement_key_value = obj[replacement_key]

        # return a full-url if provided directly from interpolation context
        if original_key_value == EmptyString or original_key_value is None:
            obj[replacement_key] = replacement_key_value
        else:
            # since we didn't provide a full-url, the url_base might not have a trailing slash
            # so we join the url_base and path correctly
            if not replacement_key_value.endswith("/"):
                replacement_key_value += "/"

            obj[replacement_key] = urljoin(replacement_key_value, original_key_value)

    def _remove_migrated_field(
        self,
        obj: Any,
        migration: MigrationType,
    ) -> None:
        """
        Removes the migrated field from the object.
        The value of the migrated field is neglected.
        """
        _, original_key, _ = migration

        obj.pop(original_key, None)
