from airbyte_cdk.manifest_migrations.manifest_migration import (
    TYPE_TAG,
    ManifestMigration,
    ManifestType,
)


class V_6_45_2_HttpRequesterRequestBodyJsonDataToRequestBody(ManifestMigration):
    """
    This migration is responsible for migrating the `url_base` key to `url` in the HttpRequester component.
    The `url_base` key is expected to be a base URL, and the `url` key is expected to be a full URL.
    The migration will copy the value of `url_base` to `url`.
    """

    component_type = "HttpRequester"
    original_keys = ["request_body_json", "request_body_data"]
    replacement_key = "request_body"

    def should_migrate(self, manifest: ManifestType) -> bool:
        return manifest[TYPE_TAG] == self.component_type and any(
            key in list(manifest.keys()) for key in self.original_keys
        )

    def migrate(self, manifest: ManifestType) -> None:
        for key in self.original_keys:
            if key in manifest:
                manifest[self.replacement_key] = manifest[key]
                manifest.pop(key, None)
