#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)

_RETRIEVER = {
    "type": "SimpleRetriever",
    "requester": {
        "type": "HttpRequester",
        "url": "https://example.com/items",
        "http_method": "GET",
        "authenticator": {"type": "BearerAuthenticator", "api_token": "{{ config['api_key'] }}"},
    },
    "record_selector": {
        "type": "RecordSelector",
        "extractor": {"type": "DpathExtractor", "field_path": []},
    },
}
_INLINE_SCHEMA_LOADER = {
    "type": "InlineSchemaLoader",
    "schema": {"type": "object", "properties": {"id": {"type": "string"}}},
}
_DYNAMIC_SCHEMA_LOADER = {
    "type": "DynamicSchemaLoader",
    "retriever": _RETRIEVER,
    "schema_type_identifier": {"type": "SchemaTypeIdentifier", "key_pointer": ["name"]},
}


def _stream(name: str, schema_loader: Any) -> Mapping[str, Any]:
    return {
        "type": "DeclarativeStream",
        "name": name,
        "primary_key": [],
        "schema_loader": schema_loader,
        "retriever": _RETRIEVER,
    }


def _dynamic_streams(resolver: Mapping[str, Any], schema_loader: Any) -> list[Mapping[str, Any]]:
    return [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": _stream("template", schema_loader),
            "components_resolver": resolver,
        }
    ]


_PARAMETRIZED_RESOLVER = {
    "type": "ParametrizedComponentsResolver",
    "stream_parameters": {
        "type": "StreamParametersDefinition",
        "list_of_parameters_for_stream": [{"name": "dynamic_items"}],
    },
    "components_mapping": [
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["name"],
            "value": "{{ components_values['name'] }}",
        }
    ],
}
_HTTP_RESOLVER = {
    "type": "HttpComponentsResolver",
    "retriever": _RETRIEVER,
    "components_mapping": _PARAMETRIZED_RESOLVER["components_mapping"],
}


def _manifest(
    streams: list[Mapping[str, Any]], dynamic_streams: list[Mapping[str, Any]] | None = None
) -> Mapping[str, Any]:
    manifest: dict[str, Any] = {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["items"]},
        "streams": streams,
        "spec": {
            "type": "Spec",
            "connection_specification": {
                "type": "object",
                "required": ["api_key"],
                "properties": {"api_key": {"type": "string", "airbyte_secret": True}},
            },
        },
    }
    if dynamic_streams:
        manifest["dynamic_streams"] = dynamic_streams
    return manifest


@pytest.mark.parametrize(
    "manifest, expected_check_config_during_discover",
    [
        pytest.param(_manifest([_stream("items", _INLINE_SCHEMA_LOADER)]), False, id="inline"),
        pytest.param(
            _manifest([_stream("items", _DYNAMIC_SCHEMA_LOADER)]), True, id="dynamic_schema_loader"
        ),
        pytest.param(
            _manifest([_stream("items", [_INLINE_SCHEMA_LOADER, _DYNAMIC_SCHEMA_LOADER])]),
            True,
            id="dynamic_schema_loader_in_list",
        ),
        pytest.param(
            _manifest(
                [
                    {
                        "type": "ConditionalStreams",
                        "condition": "{{ config.get('include_dynamic') }}",
                        "streams": [_stream("items", _DYNAMIC_SCHEMA_LOADER)],
                    }
                ]
            ),
            True,
            id="dynamic_schema_loader_in_inactive_conditional_streams",
        ),
        pytest.param(
            _manifest(
                [
                    {
                        "type": "ConditionalStreams",
                        "condition": "{{ config.get('is_sandbox') }}",
                        "streams": [_stream("sandbox_items", _INLINE_SCHEMA_LOADER)],
                    }
                ]
            ),
            True,
            id="conditional_streams",
        ),
        pytest.param(
            _manifest(
                [_stream("items", _INLINE_SCHEMA_LOADER)],
                _dynamic_streams(_PARAMETRIZED_RESOLVER, _INLINE_SCHEMA_LOADER),
            ),
            False,
            id="parametrized_dynamic_streams",
        ),
        pytest.param(
            _manifest(
                [_stream("items", _INLINE_SCHEMA_LOADER)],
                _dynamic_streams(_PARAMETRIZED_RESOLVER, _DYNAMIC_SCHEMA_LOADER),
            ),
            True,
            id="parametrized_dynamic_streams_with_dynamic_schema_loader",
        ),
        pytest.param(
            _manifest(
                [_stream("items", _INLINE_SCHEMA_LOADER)],
                _dynamic_streams(_HTTP_RESOLVER, _INLINE_SCHEMA_LOADER),
            ),
            True,
            id="http_dynamic_streams",
        ),
    ],
)
def test_check_config_during_discover(
    manifest: Mapping[str, Any], expected_check_config_during_discover: bool
) -> None:
    source = ConcurrentDeclarativeSource(source_config=manifest, config={})

    assert source.check_config_during_discover is expected_check_config_during_discover
    assert source.check_config_against_spec is True


def test_discover_static_schema_without_config() -> None:
    source = ConcurrentDeclarativeSource(
        source_config=_manifest(
            [_stream("items", _INLINE_SCHEMA_LOADER)],
            _dynamic_streams(_PARAMETRIZED_RESOLVER, _INLINE_SCHEMA_LOADER),
        ),
        config={},
    )

    catalog = source.discover(source.logger, {})

    assert sorted(stream.name for stream in catalog.streams) == ["dynamic_items", "items"]
