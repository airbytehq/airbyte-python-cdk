#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.manifest_migrations.migrations.http_requester_request_body_plain_text_json_to_request_body_json import (
    HttpRequesterRequestBodyPlainTextJsonToRequestBodyJson,
)


@pytest.mark.parametrize(
    "manifest,expected",
    [
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": '{"sort": [{"field": "createdAt"}], "filter": []}',
                },
            },
            True,
            id="json_object_string",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": '[{"field": "createdAt"}]',
                },
            },
            True,
            id="json_array_string",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": '  {"sort": [{"field": "{{ config[\'sort_field\'] }}"}]}  ',
                },
            },
            True,
            id="json_with_jinja_and_whitespace",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": "plain text body content",
                },
            },
            False,
            id="actual_plain_text",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": "interpolate_me=5&option={{ config['option'] }}",
                },
            },
            False,
            id="url_encoded_form_string",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": "{{ config['body'] }}",
                },
            },
            False,
            id="jinja_expression_not_json",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": "{% if true %}body{% endif %}",
                },
            },
            False,
            id="jinja_block_tag_not_json",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyJsonObject",
                    "value": {"key": "value"},
                },
            },
            False,
            id="json_object_type_not_plain_text",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyUrlEncodedForm",
                    "value": {"key": "value"},
                },
            },
            False,
            id="url_encoded_type_not_plain_text",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
            },
            False,
            id="no_request_body",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": "",
                },
            },
            False,
            id="empty_plain_text_value",
        ),
        pytest.param(
            {
                "type": "SomeOtherComponent",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": '{"key": "value"}',
                },
            },
            False,
            id="wrong_component_type",
        ),
    ],
)
def test_should_migrate(manifest, expected):
    migration = HttpRequesterRequestBodyPlainTextJsonToRequestBodyJson()
    assert migration.should_migrate(manifest) == expected


@pytest.mark.parametrize(
    "manifest,expected_manifest",
    [
        pytest.param(
            {
                "type": "HttpRequester",
                "url": "https://api.example.com/search",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": '{"sort": [{"field": "createdAt"}], "filter": []}',
                },
            },
            {
                "type": "HttpRequester",
                "url": "https://api.example.com/search",
                "request_body": {
                    "type": "RequestBodyJsonObject",
                    "value": '{"sort": [{"field": "createdAt"}], "filter": []}',
                },
            },
            id="json_object_migrated_to_request_body_json_object",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "url": "https://api.example.com/search",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": '{"sort": [{"field": "{{ config[\'sort_field\'] }}"}]}',
                },
            },
            {
                "type": "HttpRequester",
                "url": "https://api.example.com/search",
                "request_body": {
                    "type": "RequestBodyJsonObject",
                    "value": '{"sort": [{"field": "{{ config[\'sort_field\'] }}"}]}',
                },
            },
            id="json_with_jinja_migrated_to_request_body_json_object",
        ),
        pytest.param(
            {
                "type": "HttpRequester",
                "url": "https://api.example.com/search",
                "request_body": {
                    "type": "RequestBodyPlainText",
                    "value": '[{"id": 1}, {"id": 2}]',
                },
            },
            {
                "type": "HttpRequester",
                "url": "https://api.example.com/search",
                "request_body": {
                    "type": "RequestBodyJsonObject",
                    "value": '[{"id": 1}, {"id": 2}]',
                },
            },
            id="json_array_migrated_to_request_body_json_object",
        ),
    ],
)
def test_migrate(manifest, expected_manifest):
    migration = HttpRequesterRequestBodyPlainTextJsonToRequestBodyJson()
    assert migration.should_migrate(manifest) is True
    migration.migrate(manifest)
    assert manifest == expected_manifest
    assert migration.validate(manifest) is True


def test_validate_after_migration():
    """Validate returns True when migration was applied correctly."""
    manifest = {
        "type": "HttpRequester",
        "request_body": {
            "type": "RequestBodyJsonObject",
            "value": '{"key": "value"}',
        },
    }
    migration = HttpRequesterRequestBodyPlainTextJsonToRequestBodyJson()
    assert migration.validate(manifest) is True


def test_validate_before_migration():
    """Validate returns False when migration has not been applied yet."""
    manifest = {
        "type": "HttpRequester",
        "request_body": {
            "type": "RequestBodyPlainText",
            "value": '{"key": "value"}',
        },
    }
    migration = HttpRequesterRequestBodyPlainTextJsonToRequestBodyJson()
    assert migration.validate(manifest) is False
