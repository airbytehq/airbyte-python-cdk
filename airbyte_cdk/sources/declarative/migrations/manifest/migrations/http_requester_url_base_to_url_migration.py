from airbyte_cdk.sources.declarative.migrations.manifest.manifest_migration import (
    TYPE_TAG,
    ManifestMigration,
    ManifestType,
)


class HttpRequesterUrlBaseToUrlMigration(ManifestMigration):
    component_type = "HttpRequester"
    original_key = "url_base"
    replacement_key = "url"

    def should_migrate(self, manifest: ManifestType) -> bool:
        return manifest[TYPE_TAG] == self.component_type and self.original_key in list(
            manifest.keys()
        )

    def migrate(self, manifest: ManifestType) -> None:
        manifest[self.replacement_key] = manifest[self.original_key]
        manifest.pop(self.original_key, None)
