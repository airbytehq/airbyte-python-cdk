#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.manifest_migrations.manifest_migration import (
    TYPE_TAG,
    ManifestMigration,
    ManifestType,
)


class HttpRequesterRequestBodyJsonDataToRequestBody(ManifestMigration):
    """
    This migration is responsible for migrating the `request_body_json` and `request_body_data` keys
    to a unified `request_body` key in the HttpRequester component.
    The migration will copy the value of either original key to `request_body` and remove the original key.
    """

    component_type = "HttpRequester"

    body_json_key = "request_body_json"
    body_data_key = "request_body_data"
    original_keys = (body_json_key, body_data_key)

    replacement_key = "request_body"

    def should_migrate(self, manifest: ManifestType) -> bool:
        return manifest[TYPE_TAG] == self.component_type and any(
            key in list(manifest.keys()) for key in self.original_keys
        )

    def migrate(self, manifest: ManifestType) -> None:
        for key in self.original_keys:
            if key == self.body_json_key and key in manifest:
                manifest[self.replacement_key] = {
                    "type": "RequestBodyJson",
                    "value": manifest[key],
                }
                manifest.pop(key, None)
            elif key == self.body_data_key and key in manifest:
                manifest[self.replacement_key] = {
                    "type": "RequestBodyData",
                    "value": manifest[key],
                }
                manifest.pop(key, None)

    def validate(self, manifest: ManifestType) -> bool:
        return self.replacement_key in manifest and all(
            key not in manifest for key in self.original_keys
        )
