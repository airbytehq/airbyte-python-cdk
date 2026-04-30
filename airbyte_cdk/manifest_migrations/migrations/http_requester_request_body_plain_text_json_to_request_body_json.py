#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.manifest_migrations.manifest_migration import (
    TYPE_TAG,
    ManifestMigration,
    ManifestType,
)


class HttpRequesterRequestBodyPlainTextJsonToRequestBodyJson(ManifestMigration):
    """Migrate `RequestBodyPlainText` with JSON-like content back to `request_body_json`.

    The Connector Builder UI sometimes generates `request_body: {type: RequestBodyPlainText, ...}`
    for raw JSON string bodies (Jinja templates containing JSON). After CDK v7.17.1 (PR #971),
    `RequestBodyPlainText` is correctly routed to `request_body_data` (form-encoded), but this
    broke connectors where the Builder had misclassified JSON content as plain text.

    This migration detects `RequestBodyPlainText` where the value is a JSON-like string and
    converts it to `request_body_json` (a string-valued deprecated key that is handled correctly
    by `InterpolatedNestedRequestInputProvider`). The existing
    `HttpRequesterRequestBodyJsonDataToRequestBody` migration intentionally skips string-valued
    `request_body_json`, so there is no conflict between the two migrations.
    """

    component_type = "HttpRequester"
    request_body_key = "request_body"
    request_body_json_key = "request_body_json"
    plain_text_type = "RequestBodyPlainText"

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
        value = manifest[self.request_body_key]["value"]
        manifest.pop(self.request_body_key)
        manifest[self.request_body_json_key] = value

    def validate(self, manifest: ManifestType) -> bool:
        has_string_json = self.request_body_json_key in manifest and isinstance(
            manifest[self.request_body_json_key], str
        )
        has_request_body_plain_text = (
            isinstance(manifest.get(self.request_body_key), dict)
            and manifest[self.request_body_key].get("type") == self.plain_text_type
            and self._is_json_like(manifest[self.request_body_key].get("value", ""))
        )
        return has_string_json and not has_request_body_plain_text

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
