from airbyte_cdk.sources.declarative.migrations.manifest.migrations.http_requester_path_to_url_migration import (
    HttpRequesterPathToUrlMigration,
)
from airbyte_cdk.sources.declarative.migrations.manifest.migrations.http_requester_url_base_to_url_migration import (
    HttpRequesterUrlBaseToUrlMigration,
)

__all__ = [
    "HttpRequesterUrlBaseToUrlMigration",
    "HttpRequesterPathToUrlMigration",
]
