#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.manifest_migrations.manifest_migration import (
    TYPE_TAG,
    ManifestMigration,
    ManifestType,
)


class HttpRequesterRequestBodyPlainTextJsonToRequestBodyJson(ManifestMigration):
    """Migrate `RequestBodyPlainText` with JSON-like content to `RequestBodyJsonObject`.

    The Connector Builder UI sometimes generates `request_body: {type: RequestBodyPlainText, ...}`
    for raw JSON string bodies (Jinja templates containing JSON). After CDK v7.17.1 (PR #971),
    `RequestBodyPlainText` is correctly routed to `request_body_data` (form-encoded), but this
    broke connectors where the Builder had misclassified JSON content as plain text.

    This migration detects `RequestBodyPlainText` where the value is a JSON-like string and
    converts it to `RequestBodyJsonObject` with a string value. `RequestBodyJsonObject` now
    accepts both dict and string values, and routes through `InterpolatedNestedRequestInputProvider`
    which correctly handles string templates containing JSON.
    """

    component_type = "HttpRequester"
    request_body_key = "request_body"
    plain_text_type = "RequestBodyPlainText"
    json_object_type = "RequestBodyJsonObject"

    def should_migrate(self, manifest: ManifestType) -> bool:
        if manifest.get(TYPE_TAG) != self.component_type:
            return False
        request_body = manifest.get(self.request_body_key)
        if not isinstance(request_body, dict):
            return False
        if request_body.get("type") != self.plain_text_type:
            return False
        value = request_body.get("value")
        if not isinstance(value, str):
            return False
        return self._is_json_like(value)

    def migrate(self, manifest: ManifestType) -> None:
        request_body = manifest[self.request_body_key]
        request_body["type"] = self.json_object_type

    def validate(self, manifest: ManifestType) -> bool:
        request_body = manifest.get(self.request_body_key)
        if not isinstance(request_body, dict):
            return False
        is_json_object_with_string = (
            request_body.get("type") == self.json_object_type
            and isinstance(request_body.get("value"), str)
            and self._is_json_like(request_body.get("value", ""))
        )
        is_plain_text_json = (
            request_body.get("type") == self.plain_text_type
            and isinstance(request_body.get("value"), str)
            and self._is_json_like(request_body.get("value", ""))
        )
        return is_json_object_with_string and not is_plain_text_json

    @staticmethod
    def _is_json_like(value: str) -> bool:
        """Check if a string value looks like JSON content.

        Returns `True` when the stripped value starts with `{` or `[`, excluding
        Jinja expression openers (`{{`) and Jinja block openers (`{%`).
        """
        stripped = value.strip()
        if not stripped:
            return False
        if stripped.startswith("["):
            return True
        if (
            stripped.startswith("{")
            and not stripped.startswith("{{")
            and not stripped.startswith("{%")
        ):
            return True
        return False
