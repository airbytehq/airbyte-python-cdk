#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    MissingConfigValueError,
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


_INCREMENTAL_SYNC = {
    "type": "DatetimeBasedCursor",
    "cursor_field": "{{ parameters['cursor'] }}",
    "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
    "start_datetime": "{{ config['start_date'] }}",
}


def _incremental_stream(name: str) -> dict[str, Any]:
    return {
        **_stream(name, _INLINE_SCHEMA_LOADER),
        "primary_key": ["id"],
        "incremental_sync": _INCREMENTAL_SYNC,
        "$parameters": {"cursor": "updated_at"},
    }


def _substream(name: str, parent: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_incremental_stream(name),
        "retriever": {
            **_RETRIEVER,
            "partition_router": {
                "type": "SubstreamPartitionRouter",
                "parent_stream_configs": [
                    {
                        "type": "ParentStreamConfig",
                        "stream": parent,
                        "parent_key": "id",
                        "partition_field": "parent_id",
                    }
                ],
            },
        },
    }


def _state_delegating_stream(name: str) -> dict[str, Any]:
    return {
        "type": "StateDelegatingStream",
        "name": name,
        "full_refresh_stream": {**_stream(name, _INLINE_SCHEMA_LOADER), "primary_key": ["id"]},
        "incremental_stream": _incremental_stream(name),
    }


@pytest.mark.parametrize(
    "streams",
    [
        pytest.param([_incremental_stream("items")], id="config_dependent_cursor"),
        pytest.param(
            [_substream("children", _incremental_stream("parents"))],
            id="substream_with_config_dependent_parent",
        ),
        pytest.param([_state_delegating_stream("items")], id="state_delegating_stream"),
    ],
)
def test_config_free_discover_matches_configured_discover(streams: list[Mapping[str, Any]]) -> None:
    config = {"api_key": "dummy", "start_date": "2024-01-01T00:00:00Z"}
    configured_source = ConcurrentDeclarativeSource(source_config=_manifest(streams), config=config)
    config_free_source = ConcurrentDeclarativeSource(source_config=_manifest(streams), config={})

    configured_catalog = configured_source.discover(configured_source.logger, config)
    config_free_catalog = config_free_source.discover(config_free_source.logger, {})

    assert config_free_catalog == configured_catalog


def test_config_free_discover_streams_cannot_be_read() -> None:
    source = ConcurrentDeclarativeSource(
        source_config=_manifest([_incremental_stream("items")]), config={}
    )

    (stream,) = source._catalog_streams()

    with pytest.raises(RuntimeError, match="discover only"):
        list(stream.generate_partitions())


def test_discover_without_config_fails_for_config_dependent_cursor_without_catalog_streams() -> (
    None
):
    source = ConcurrentDeclarativeSource(
        source_config=_manifest([_incremental_stream("items")]), config={}
    )

    with pytest.raises(ValueError):
        source.streams(config={})


@pytest.mark.parametrize(
    "cursor_field",
    [
        pytest.param("{{ config['record_cursor'] }}", id="whole_field"),
        pytest.param("updated_{{ config['suffix'] }}", id="partial_field"),
    ],
)
def test_config_free_discover_fails_when_cursor_field_reads_missing_config(
    cursor_field: str,
) -> None:
    stream = _incremental_stream("items")
    stream["incremental_sync"] = {**_INCREMENTAL_SYNC, "cursor_field": cursor_field}
    source = ConcurrentDeclarativeSource(source_config=_manifest([stream]), config={})

    with pytest.raises(MissingConfigValueError):
        source.discover(source.logger, {})


def test_config_free_discover_uses_cursor_field_template_default() -> None:
    stream = _incremental_stream("items")
    stream["incremental_sync"] = {
        **_INCREMENTAL_SYNC,
        "cursor_field": "{{ config.get('record_cursor', 'updated_at') }}",
    }
    source = ConcurrentDeclarativeSource(source_config=_manifest([stream]), config={})

    (catalog_stream,) = source.discover(source.logger, {}).streams

    assert catalog_stream.default_cursor_field == ["updated_at"]
