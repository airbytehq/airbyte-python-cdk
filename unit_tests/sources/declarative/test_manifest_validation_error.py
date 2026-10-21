#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from copy import deepcopy

import pytest
from jsonschema.exceptions import ValidationError

from airbyte_cdk.legacy.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.manifest_validation_error import (
    format_manifest_validation_error,
)

_BASE_MANIFEST = {
    "version": "6.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["lists"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "lists",
            "primary_key": [],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "type": "object",
                    "properties": {},
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": ["result"]},
                },
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/lists",
                    "http_method": "GET",
                    "authenticator": {
                        "type": "SessionTokenAuthenticator",
                        "login_requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/login",
                            "http_method": "POST",
                        },
                        "session_token_path": ["token"],
                        "request_authentication": {
                            "type": "ApiKey",
                            "inject_into": {
                                "type": "RequestOption",
                                "field_name": "X",
                                "inject_into": "header",
                            },
                        },
                    },
                },
            },
        }
    ],
}


def _valid_manifest() -> dict:
    return deepcopy(_BASE_MANIFEST)


def _request_authentication(manifest: dict) -> dict:
    return manifest["streams"][0]["retriever"]["requester"]["authenticator"][
        "request_authentication"
    ]


def test_valid_manifest_constructs_without_error() -> None:
    ConcurrentDeclarativeSource(
        source_config=_valid_manifest(), config={}, catalog=None, state=None
    )


def test_invalid_authenticator_type_names_path_and_allowed_values() -> None:
    manifest = _valid_manifest()
    _request_authentication(manifest)["type"] = "ApiKeyAuthenticator"

    with pytest.raises(ValidationError) as exc_info:
        ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None, state=None)

    expected = (
        "Manifest field 'streams[0].retriever.requester.authenticator."
        "request_authentication.type' is invalid: value 'ApiKeyAuthenticator' "
        "is not one of ['ApiKey', 'Bearer']."
    )
    assert str(exc_info.value) == expected
    assert exc_info.value.message == str(exc_info.value)
    assert "declarative_component_schema" not in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValidationError)
    assert list(exc_info.value.__cause__.absolute_path)[-1] == "request_authentication"


def test_missing_streams_and_dynamic_streams_reports_required_properties() -> None:
    manifest = _valid_manifest()
    del manifest["streams"]

    with pytest.raises(ValidationError) as exc_info:
        ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None, state=None)

    assert str(exc_info.value) == (
        "Manifest field '<root>' is invalid: one of the following properties "
        "is required: 'streams', 'dynamic_streams'."
    )


def test_invalid_http_method_names_allowed_values() -> None:
    manifest = _valid_manifest()
    manifest["streams"][0]["retriever"]["requester"]["http_method"] = "FETCH"

    with pytest.raises(ValidationError) as exc_info:
        ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None, state=None)

    assert str(exc_info.value) == (
        "Manifest field 'streams[0].retriever.requester.http_method' is invalid: "
        "value 'FETCH' is not one of ['GET', 'POST']."
    )


def test_invalid_nested_request_option_names_path() -> None:
    manifest = _valid_manifest()
    _request_authentication(manifest)["inject_into"]["inject_into"] = "nowhere"

    with pytest.raises(ValidationError) as exc_info:
        ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None, state=None)

    message = str(exc_info.value)
    assert (
        "Manifest field 'streams[0].retriever.requester.authenticator."
        "request_authentication.inject_into.inject_into' is invalid: "
    ) in message
    assert "'nowhere' is not one of [" in message


def test_invalid_authenticator_type_in_legacy_source() -> None:
    manifest = _valid_manifest()
    _request_authentication(manifest)["type"] = "ApiKeyAuthenticator"

    with pytest.raises(ValidationError) as exc_info:
        ManifestDeclarativeSource(source_config=manifest)

    assert str(exc_info.value) == (
        "Manifest field 'streams[0].retriever.requester.authenticator."
        "request_authentication.type' is invalid: value 'ApiKeyAuthenticator' "
        "is not one of ['ApiKey', 'Bearer']."
    )


def test_format_type_error_single_type() -> None:
    error = ValidationError(
        "5 is not of type 'string'",
        validator="type",
        path=["a", "b"],
        instance=5,
        validator_value="string",
    )
    assert format_manifest_validation_error(error) == (
        "Manifest field 'a.b' is invalid: value 5 is not of type 'string'."
    )


def test_format_type_error_multiple_types() -> None:
    error = ValidationError(
        "5 is not of type 'string', 'null'",
        validator="type",
        path=["a"],
        instance=5,
        validator_value=["string", "null"],
    )
    assert format_manifest_validation_error(error) == (
        "Manifest field 'a' is invalid: value 5 is not of type 'string' or 'null'."
    )


def test_format_long_instance_repr_is_truncated() -> None:
    error = ValidationError(
        "long value",
        validator="enum",
        path=["a"],
        instance="x" * 200,
        validator_value=["y"],
    )
    message = format_manifest_validation_error(error)
    truncated_value = repr("x" * 200)[:97] + "..."
    assert truncated_value in message
    assert "x" * 100 not in message


def test_format_unknown_validator_falls_back_to_message() -> None:
    error = ValidationError(
        "'x' does not match '^y$'",
        validator="pattern",
        path=["a"],
        instance="x",
        validator_value="^y$",
    )
    assert format_manifest_validation_error(error) == (
        "Manifest field 'a' is invalid: 'x' does not match '^y$'."
    )
