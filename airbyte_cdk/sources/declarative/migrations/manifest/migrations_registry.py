from typing import List, Type

from airbyte_cdk.sources.declarative.migrations.manifest.manifest_migration import (
    ManifestMigration,
)
from airbyte_cdk.sources.declarative.migrations.manifest.migrations import (
    HttpRequesterPathToUrlMigration,
    HttpRequesterUrlBaseToUrlMigration,
)

# This is the registry of all the migrations that are available.
# Add new migrations to the bottom of the list,
# ( ! ) make sure the order of the migrations is correct.
migrations_registry: List[Type[ManifestMigration]] = [
    HttpRequesterUrlBaseToUrlMigration,
    HttpRequesterPathToUrlMigration,
]
