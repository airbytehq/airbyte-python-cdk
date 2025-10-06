#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import copy
import json
import logging
import math
import os
import sys
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union
from unittest.mock import Mock, call, mock_open, patch

import freezegun
import isodate
import pytest
import requests
import yaml
from airbyte_protocol_dataclasses.models import (
    AirbyteStreamStatus,
    AirbyteStreamStatusTraceMessage,
    AirbyteTraceMessage,
    TraceType,
)
from jsonschema.exceptions import ValidationError
from typing_extensions import deprecated

import unit_tests.sources.declarative.external_component  # Needed for dynamic imports to work
from airbyte_cdk.legacy.sources.declarative.declarative_stream import DeclarativeStream
from airbyte_cdk.models import (
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    FailureType,
    Level,
    Status,
    StreamDescriptor,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.declarative.async_job.job_tracker import ConcurrentJobLimitReached
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.extractors.record_filter import (
    ClientSideIncrementalRecordFilterDecorator,
)
from airbyte_cdk.sources.declarative.partition_routers import AsyncJobPartitionRouter
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    StreamSlicerPartitionGenerator,
)
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.checkpoint import Cursor
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.sources.streams.concurrent.state_converters.incrementing_count_stream_state_converter import (
    IncrementingCountStreamStateConverter,
)
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_parse

logger = logging.getLogger("airbyte")

_CONFIG = {"start_date": "2024-07-01T00:00:00.000Z"}

_CATALOG = ConfiguredAirbyteCatalog(
    streams=[
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="party_members", json_schema={}, supported_sync_modes=[SyncMode.incremental]
            ),
            sync_mode=SyncMode.incremental,
            destination_sync_mode=DestinationSyncMode.append,
        ),
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        ),
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="locations", json_schema={}, supported_sync_modes=[SyncMode.incremental]
            ),
            sync_mode=SyncMode.incremental,
            destination_sync_mode=DestinationSyncMode.append,
        ),
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="party_members_skills",
                json_schema={},
                supported_sync_modes=[SyncMode.full_refresh],
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        ),
    ]
)
_LOCATIONS_RESPONSE = HttpResponse(
    json.dumps(
        [
            {"id": "444", "name": "Yongen-jaya", "updated_at": "2024-08-10"},
            {"id": "scramble", "name": "Shibuya", "updated_at": "2024-08-10"},
            {"id": "aoyama", "name": "Aoyama-itchome", "updated_at": "2024-08-10"},
            {"id": "shin123", "name": "Shinjuku", "updated_at": "2024-08-10"},
        ]
    )
)
_PALACES_RESPONSE = HttpResponse(
    json.dumps(
        [
            {"id": "0", "world": "castle", "owner": "kamoshida"},
            {"id": "1", "world": "museum", "owner": "madarame"},
            {"id": "2", "world": "bank", "owner": "kaneshiro"},
            {"id": "3", "world": "pyramid", "owner": "futaba"},
            {"id": "4", "world": "spaceport", "owner": "okumura"},
            {"id": "5", "world": "casino", "owner": "nijima"},
            {"id": "6", "world": "cruiser", "owner": "shido"},
        ]
    )
)
_PARTY_MEMBERS_SKILLS_RESPONSE = HttpResponse(
    json.dumps(
        [
            {"id": "0", "name": "hassou tobi"},
            {"id": "1", "name": "mafreidyne"},
            {"id": "2", "name": "myriad truths"},
        ]
    )
)
_EMPTY_RESPONSE = HttpResponse(json.dumps([]))
_NOW = "2024-09-10T00:00:00"
_NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES = [
    (
        {"start": "2024-07-01", "end": "2024-07-15"},
        HttpResponse(
            json.dumps(
                [
                    {
                        "id": "amamiya",
                        "first_name": "ren",
                        "last_name": "amamiya",
                        "updated_at": "2024-07-10",
                    }
                ]
            )
        ),
    ),
    ({"start": "2024-07-16", "end": "2024-07-30"}, _EMPTY_RESPONSE),
    (
        {"start": "2024-07-31", "end": "2024-08-14"},
        HttpResponse(
            json.dumps(
                [
                    {
                        "id": "nijima",
                        "first_name": "makoto",
                        "last_name": "nijima",
                        "updated_at": "2024-08-10",
                    },
                ]
            )
        ),
    ),
    ({"start": "2024-08-15", "end": "2024-08-29"}, _EMPTY_RESPONSE),
    (
        {"start": "2024-08-30", "end": "2024-09-10"},
        HttpResponse(
            json.dumps(
                [
                    {
                        "id": "yoshizawa",
                        "first_name": "sumire",
                        "last_name": "yoshizawa",
                        "updated_at": "2024-09-10",
                    }
                ]
            )
        ),
    ),
]
_MANIFEST = {
    "version": "5.0.0",
    "definitions": {
        "selector": {
            "type": "RecordSelector",
            "extractor": {"type": "DpathExtractor", "field_path": []},
        },
        "requester": {
            "type": "HttpRequester",
            "url_base": "https://persona.metaverse.com",
            "http_method": "GET",
            "authenticator": {
                "type": "BasicHttpAuthenticator",
                "username": "{{ config['api_key'] }}",
                "password": "{{ config['secret_key'] }}",
            },
            "error_handler": {
                "type": "DefaultErrorHandler",
                "response_filters": [
                    {
                        "http_codes": [403],
                        "action": "FAIL",
                        "failure_type": "config_error",
                        "error_message": "Access denied due to lack of permission or invalid API/Secret key or wrong data region.",
                    },
                    {
                        "http_codes": [404],
                        "action": "IGNORE",
                        "error_message": "No data available for the time range requested.",
                    },
                ],
            },
        },
        "retriever": {
            "type": "SimpleRetriever",
            "record_selector": {"$ref": "#/definitions/selector"},
            "paginator": {"type": "NoPagination"},
            "requester": {"$ref": "#/definitions/requester"},
        },
        "incremental_cursor": {
            "type": "DatetimeBasedCursor",
            "start_datetime": {
                "datetime": "{{ format_datetime(config['start_date'], '%Y-%m-%d') }}"
            },
            "end_datetime": {"datetime": "{{ now_utc().strftime('%Y-%m-%d') }}"},
            "datetime_format": "%Y-%m-%d",
            "cursor_datetime_formats": ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
            "cursor_granularity": "P1D",
            "step": "P15D",
            "cursor_field": "updated_at",
            "lookback_window": "P5D",
            "start_time_option": {
                "type": "RequestOption",
                "field_name": "start",
                "inject_into": "request_parameter",
            },
            "end_time_option": {
                "type": "RequestOption",
                "field_name": "end",
                "inject_into": "request_parameter",
            },
        },
        "incremental_counting_cursor": {
            "type": "IncrementingCountCursor",
            "cursor_field": "id",
            "start_value": 0,
            "start_time_option": {
                "type": "RequestOption",
                "field_name": "since_id",
                "inject_into": "request_parameter",
            },
        },
        "base_stream": {"retriever": {"$ref": "#/definitions/retriever"}},
        "base_incremental_stream": {
            "retriever": {
                "$ref": "#/definitions/retriever",
                "requester": {"$ref": "#/definitions/requester"},
            },
            "incremental_sync": {"$ref": "#/definitions/incremental_cursor"},
        },
        "base_incremental_counting_stream": {
            "retriever": {
                "$ref": "#/definitions/retriever",
                "requester": {"$ref": "#/definitions/requester"},
            },
            "incremental_sync": {"$ref": "#/definitions/incremental_counting_cursor"},
        },
        "party_members_stream": {
            "$ref": "#/definitions/base_incremental_stream",
            "retriever": {
                "$ref": "#/definitions/base_incremental_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
            },
            "$parameters": {"name": "party_members", "primary_key": "id", "path": "/party_members"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the party member",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "palaces_stream": {
            "$ref": "#/definitions/base_stream",
            "$parameters": {"name": "palaces", "primary_key": "id", "path": "/palaces"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the metaverse palace",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "async_job_stream": {
            "$ref": "#/definitions/base_stream",
            "$parameters": {
                "name": "async_job_stream",
                "primary_key": "id",
                "url_base": "https://persona.metaverse.com",
            },
            "retriever": {
                "type": "AsyncRetriever",
                "status_mapping": {
                    "failed": ["failed"],
                    "running": ["pending"],
                    "timeout": ["timeout"],
                    "completed": ["ready"],
                },
                "download_target_extractor": {"type": "DpathExtractor", "field_path": ["urls"]},
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
                "status_extractor": {"type": "DpathExtractor", "field_path": ["status"]},
                "polling_requester": {
                    "type": "HttpRequester",
                    "path": "/async_job/{{creation_response['id'] }}",
                    "http_method": "GET",
                    "authenticator": {
                        "type": "BearerAuthenticator",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "creation_requester": {
                    "type": "HttpRequester",
                    "path": "async_job",
                    "http_method": "POST",
                    "authenticator": {
                        "type": "BearerAuthenticator",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "download_requester": {
                    "type": "HttpRequester",
                    "path": "{{stream_slice['url']}}",
                    "http_method": "GET",
                },
            },
            "incremental_sync": {"$ref": "#/definitions/incremental_cursor"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the metaverse palace",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "locations_stream": {
            "$ref": "#/definitions/base_incremental_stream",
            "retriever": {
                "$ref": "#/definitions/base_incremental_stream/retriever",
                "requester": {
                    "$ref": "#/definitions/base_incremental_stream/retriever/requester",
                    "request_parameters": {"m": "active", "i": "1", "g": "country"},
                },
                "record_selector": {"$ref": "#/definitions/selector"},
            },
            "incremental_sync": {
                "$ref": "#/definitions/incremental_cursor",
                "step": "P1M",
                "cursor_field": "updated_at",
            },
            "$parameters": {"name": "locations", "primary_key": "id", "path": "/locations"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the neighborhood location",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "party_members_skills_stream": {
            "$ref": "#/definitions/base_stream",
            "retriever": {
                "$ref": "#/definitions/base_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "type": "ParentStreamConfig",
                            "stream": "#/definitions/party_members_stream",
                            "parent_key": "id",
                            "partition_field": "party_member_id",
                        }
                    ],
                },
            },
            "$parameters": {
                "name": "party_members_skills",
                "primary_key": "id",
                "path": "/party_members/{{stream_slice.party_member_id}}/skills",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the party member",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "arcana_personas_stream": {
            "$ref": "#/definitions/base_stream",
            "retriever": {
                "$ref": "#/definitions/base_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "partition_router": {
                    "type": "ListPartitionRouter",
                    "cursor_field": "arcana_id",
                    "values": [
                        "Fool",
                        "Magician",
                        "Priestess",
                        "Empress",
                        "Emperor",
                        "Hierophant",
                        "Lovers",
                        "Chariot",
                        "Justice",
                        "Hermit",
                        "Fortune",
                        "Strength",
                        "Hanged Man",
                        "Death",
                        "Temperance",
                        "Devil",
                        "Tower",
                        "Star",
                        "Moon",
                        "Sun",
                        "Judgement",
                        "World",
                    ],
                },
            },
            "$parameters": {
                "name": "arcana_personas",
                "primary_key": "id",
                "path": "/arcanas/{{stream_slice.arcana_id}}/personas",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the persona",
                            "type": ["null", "string"],
                        },
                        "arcana_id": {
                            "description": "The associated arcana tarot for this persona",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "palace_enemies_stream": {
            "$ref": "#/definitions/base_incremental_stream",
            "retriever": {
                "$ref": "#/definitions/base_incremental_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "type": "ParentStreamConfig",
                            "stream": "#/definitions/palaces_stream",
                            "parent_key": "id",
                            "partition_field": "palace_id",
                        }
                    ],
                },
            },
            "$parameters": {
                "name": "palace_enemies",
                "primary_key": "id",
                "path": "/palaces/{{stream_slice.palace_id}}/enemies",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the enemy persona",
                            "type": ["null", "string"],
                        },
                        "palace_id": {
                            "description": "The palace id where this persona exists in",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "incremental_counting_stream": {
            "$ref": "#/definitions/base_incremental_counting_stream",
            "retriever": {
                "$ref": "#/definitions/base_incremental_counting_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
            },
            "$parameters": {
                "name": "incremental_counting_stream",
                "primary_key": "id",
                "path": "/party_members",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the party member",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
    },
    "streams": [
        "#/definitions/party_members_stream",
        "#/definitions/palaces_stream",
        "#/definitions/locations_stream",
        "#/definitions/party_members_skills_stream",
        "#/definitions/arcana_personas_stream",
        "#/definitions/palace_enemies_stream",
        "#/definitions/async_job_stream",
        "#/definitions/incremental_counting_stream",
    ],
    "check": {"stream_names": ["party_members", "locations"]},
    "concurrency_level": {
        "type": "ConcurrencyLevel",
        "default_concurrency": "{{ config['num_workers'] or 10 }}",
        "max_concurrency": 25,
    },
}


EXTERNAL_CONNECTION_SPECIFICATION = {
    "type": "object",
    "required": ["api_token"],
    "additionalProperties": False,
    "properties": {"api_token": {"type": "string"}},
}


@deprecated("See note in docstring for more information")
class DeclarativeStreamDecorator(Stream):
    """
    Helper class that wraps an existing DeclarativeStream but allows for overriding the output of read_records() to
    make it easier to mock behavior and test how low-code streams integrate with the Concurrent CDK.

    NOTE: We are not using that for now but the intent was to scope the tests to only testing that streams were properly instantiated and
    interacted together properly. However, in practice, we had a couple surprises like `get_cursor` and `stream_slices` needed to be
    re-implemented as well. Because of that, we've move away from that in favour of doing tests that integrate up until the HTTP request.
    The drawback of that is that we are dependent on any change later (like if the DatetimeBasedCursor changes, this will affect those
    tests) but it feels less flaky than this. If we have new information in the future to infirm that, feel free to re-use this class as
    necessary.
    """

    def __init__(
        self,
        declarative_stream: DeclarativeStream,
        slice_to_records_mapping: Mapping[tuple[str, str], List[Mapping[str, Any]]],
    ):
        self._declarative_stream = declarative_stream
        self._slice_to_records_mapping = slice_to_records_mapping

    @property
    def name(self) -> str:
        return self._declarative_stream.name

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._declarative_stream.primary_key

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        if isinstance(stream_slice, StreamSlice):
            slice_key = (stream_slice.get("start_time"), stream_slice.get("end_time"))

            # Extra logic to simulate raising an error during certain partitions to validate error handling
            if slice_key == ("2024-08-05", "2024-09-04"):
                raise AirbyteTracedException(
                    message=f"Received an unexpected error during interval with start: {slice_key[0]} and end: {slice_key[1]}.",
                    failure_type=FailureType.config_error,
                )

            if slice_key in self._slice_to_records_mapping:
                yield from self._slice_to_records_mapping.get(slice_key)
            else:
                yield from []
        else:
            raise ValueError(
                f"stream_slice should be of type StreamSlice, but received {type(stream_slice)}"
            )

    def get_json_schema(self) -> Mapping[str, Any]:
        return self._declarative_stream.get_json_schema()

    def get_cursor(self) -> Optional[Cursor]:
        return self._declarative_stream.get_cursor()


@freezegun.freeze_time(time_to_freeze=datetime(2024, 9, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
def test_create_concurrent_cursor():
    """
    Validate that the ConcurrentDeclarativeSource properly instantiates a ConcurrentCursor from the
    low-code DatetimeBasedCursor component
    """

    incoming_locations_state = {
        "slices": [
            {"start": "2024-07-01T00:00:00", "end": "2024-07-31T00:00:00"},
        ],
        "state_type": "date-range",
    }

    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(**incoming_locations_state),
            ),
        ),
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )
    streams = source.streams(config=_CONFIG)

    party_members_stream = streams[0]
    assert isinstance(party_members_stream, DefaultStream)
    party_members_cursor = party_members_stream.cursor

    assert isinstance(party_members_cursor, ConcurrentCursor)
    assert party_members_cursor._stream_name == "party_members"
    assert party_members_cursor._cursor_field.cursor_field_key == "updated_at"
    assert party_members_cursor._start == ab_datetime_parse(_CONFIG.get("start_date"))
    assert party_members_cursor._end_provider() == AirbyteDateTime(
        year=2024, month=9, day=1, tzinfo=timezone.utc
    )
    assert party_members_cursor._slice_boundary_fields == ("start_time", "end_time")
    assert party_members_cursor._slice_range == timedelta(days=15)
    assert party_members_cursor._lookback_window == timedelta(days=5)
    assert party_members_cursor._cursor_granularity == timedelta(days=1)

    locations_stream = streams[2]
    assert isinstance(locations_stream, DefaultStream)
    locations_cursor = locations_stream.cursor

    assert isinstance(locations_cursor, ConcurrentCursor)
    assert locations_cursor._stream_name == "locations"
    assert locations_cursor._cursor_field.cursor_field_key == "updated_at"
    assert locations_cursor._start == ab_datetime_parse(_CONFIG.get("start_date"))
    assert locations_cursor._end_provider() == AirbyteDateTime(
        year=2024, month=9, day=1, tzinfo=timezone.utc
    )
    assert locations_cursor._slice_boundary_fields == ("start_time", "end_time")
    assert locations_cursor._slice_range == isodate.Duration(months=1)
    assert locations_cursor._lookback_window == timedelta(days=5)
    assert locations_cursor._cursor_granularity == timedelta(days=1)
    assert locations_cursor._concurrent_state == {
        "slices": [
            {
                "start": AirbyteDateTime(2024, 7, 1, tzinfo=timezone.utc),
                "end": AirbyteDateTime(2024, 7, 31, tzinfo=timezone.utc),
            }
        ],
        "state_type": "date-range",
    }

    incremental_counting_stream = streams[7]
    assert isinstance(incremental_counting_stream, DefaultStream)
    incremental_counting_cursor = incremental_counting_stream.cursor

    assert isinstance(incremental_counting_cursor, ConcurrentCursor)
    assert isinstance(
        incremental_counting_cursor._connector_state_converter,
        IncrementingCountStreamStateConverter,
    )
    assert incremental_counting_cursor._stream_name == "incremental_counting_stream"
    assert incremental_counting_cursor._cursor_field.cursor_field_key == "id"
    assert incremental_counting_cursor._start == 0
    assert incremental_counting_cursor._end_provider() == math.inf


def test_check():
    """
    Verifies that the ConcurrentDeclarativeSource check command is run against synchronous streams
    """
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(
                "https://persona.metaverse.com/party_members?start=2024-07-01&end=2024-07-15"
            ),
            HttpResponse(
                json.dumps(
                    {
                        "id": "amamiya",
                        "first_name": "ren",
                        "last_name": "amamiya",
                        "updated_at": "2024-07-10",
                    }
                )
            ),
        )
        http_mocker.get(
            HttpRequest("https://persona.metaverse.com/palaces"),
            HttpResponse(json.dumps({"id": "palace_1"})),
        )
        http_mocker.get(
            HttpRequest(
                "https://persona.metaverse.com/locations?m=active&i=1&g=country&start=2024-07-01&end=2024-07-31"
            ),
            HttpResponse(json.dumps({"id": "location_1"})),
        )
        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
        )

        connection_status = source.check(logger=source.logger, config=_CONFIG)

    assert connection_status.status == Status.SUCCEEDED


def test_discover():
    """
    Verifies that the ConcurrentDeclarativeSource discover command returns concurrent and synchronous catalog definitions
    """
    expected_stream_names = {
        "party_members",
        "palaces",
        "locations",
        "party_members_skills",
        "arcana_personas",
        "palace_enemies",
        "async_job_stream",
        "incremental_counting_stream",
    }

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
    )

    actual_catalog = source.discover(logger=source.logger, config=_CONFIG)

    assert set(map(lambda stream: stream.name, actual_catalog.streams)) == expected_stream_names


def _mock_requests(
    http_mocker: HttpMocker,
    url: str,
    query_params: List[Dict[str, str]],
    responses: List[HttpResponse],
) -> None:
    assert len(query_params) == len(responses), "Expecting as many slices as response"

    for i in range(len(query_params)):
        http_mocker.get(HttpRequest(url, query_params=query_params[i]), responses[i])


def _mock_party_members_requests(
    http_mocker: HttpMocker, slices_and_responses: List[Tuple[Dict[str, str], HttpResponse]]
) -> None:
    slices = list(map(lambda slice_and_response: slice_and_response[0], slices_and_responses))
    responses = list(map(lambda slice_and_response: slice_and_response[1], slices_and_responses))

    _mock_requests(
        http_mocker,
        "https://persona.metaverse.com/party_members",
        slices,
        responses,
    )


def _mock_locations_requests(http_mocker: HttpMocker, slices: List[Dict[str, str]]) -> None:
    locations_query_params = list(
        map(lambda _slice: _slice | {"m": "active", "i": "1", "g": "country"}, slices)
    )
    _mock_requests(
        http_mocker,
        "https://persona.metaverse.com/locations",
        locations_query_params,
        [_LOCATIONS_RESPONSE] * len(slices),
    )


def _mock_party_members_skills_requests(http_mocker: HttpMocker) -> None:
    """
    This method assumes _mock_party_members_requests has been called before else the stream won't work.
    """
    http_mocker.get(
        HttpRequest("https://persona.metaverse.com/party_members/amamiya/skills"),
        _PARTY_MEMBERS_SKILLS_RESPONSE,
    )
    http_mocker.get(
        HttpRequest("https://persona.metaverse.com/party_members/nijima/skills"),
        _PARTY_MEMBERS_SKILLS_RESPONSE,
    )
    http_mocker.get(
        HttpRequest("https://persona.metaverse.com/party_members/yoshizawa/skills"),
        _PARTY_MEMBERS_SKILLS_RESPONSE,
    )


def mocked_init(self, is_sequential_state: bool = True):
    """
    This method is used to patch the existing __init__() function and always set is_sequential_state to
    false. This is required because we want to test the concurrent state format. And because streams are
    created under the hood of the read/discover/check command, we have no way of setting the field without
    patching __init__()
    """
    self._is_sequential_state = False


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="SQLite threading compatibility issue: Python 3.12+ has stricter thread safety checks that cause 'InterfaceError: bad parameter or other API misuse' when SQLite connections are shared across threads in the concurrent framework",
)
def test_read_with_concurrent_and_synchronous_streams():
    """
    Verifies that a ConcurrentDeclarativeSource processes concurrent streams followed by synchronous streams
    """
    location_slices = [
        {"start": "2024-07-01", "end": "2024-07-31"},
        {"start": "2024-08-01", "end": "2024-08-31"},
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]
    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=None
    )

    with HttpMocker() as http_mocker:
        _mock_party_members_requests(http_mocker, _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES)
        _mock_locations_requests(http_mocker, location_slices)
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)
        _mock_party_members_skills_requests(http_mocker)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=_CATALOG, state=[])
        )

    # See _mock_party_members_requests
    party_members_records = get_records_for_stream("party_members", messages)
    assert len(party_members_records) == 3

    party_members_states = get_states_for_stream(stream_name="party_members", messages=messages)
    assert len(party_members_states) == 6
    assert (
        party_members_states[5].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-09-10",
                }
            ],
        ).__dict__
    )

    # Expects 12 records, 3 slices, 4 records each slice
    locations_records = get_records_for_stream(stream_name="locations", messages=messages)
    assert len(locations_records) == 12

    # 3 partitions == 3 state messages + final state message
    # Because we cannot guarantee the order partitions finish, we only validate that the final state has the latest checkpoint value
    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 4
    assert (
        locations_states[3].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-08-10",
                }
            ],
        ).__dict__
    )

    # Expects 7 records, 1 empty slice, 7 records in slice
    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7

    palaces_states = get_states_for_stream(stream_name="palaces", messages=messages)
    assert len(palaces_states) == 1
    assert (
        palaces_states[0].stream.stream_state.__dict__
        == AirbyteStateBlob(__ab_no_cursor_state_message=True).__dict__
    )

    # Expects 3 records, 3 slices, 3 records in slice
    party_members_skills_records = get_records_for_stream("party_members_skills", messages)
    assert len(party_members_skills_records) == 9

    party_members_skills_states = get_states_for_stream(
        stream_name="party_members_skills", messages=messages
    )
    assert len(party_members_skills_states) == 1
    assert (
        party_members_skills_states[0].stream.stream_state.__dict__
        == AirbyteStateBlob(__ab_no_cursor_state_message=True).__dict__
    )


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_with_concurrent_and_synchronous_streams_with_concurrent_state():
    """
    Verifies that a ConcurrentDeclarativeSource processes concurrent streams correctly using the incoming
    concurrent state format
    """
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(
                    state_type="date-range",
                    slices=[{"start": "2024-07-01", "end": "2024-07-31"}],
                ),
            ),
        ),
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="party_members", namespace=None),
                stream_state=AirbyteStateBlob(
                    state_type="date-range",
                    slices=[
                        {"start": "2024-07-16", "end": "2024-07-30"},
                        {"start": "2024-07-31", "end": "2024-08-14"},
                        {"start": "2024-08-30", "end": "2024-09-09"},
                    ],
                ),
            ),
        ),
    ]

    party_members_slices_and_responses = _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES + [
        (
            {"start": "2024-09-04", "end": "2024-09-10"},  # considering lookback window
            HttpResponse(
                json.dumps(
                    [
                        {
                            "id": "yoshizawa",
                            "first_name": "sumire",
                            "last_name": "yoshizawa",
                            "updated_at": "2024-09-10",
                        }
                    ]
                )
            ),
        )
    ]
    location_slices = [
        {"start": "2024-07-26", "end": "2024-08-25"},
        {"start": "2024-08-26", "end": "2024-09-10"},
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )

    with HttpMocker() as http_mocker:
        _mock_party_members_requests(http_mocker, party_members_slices_and_responses)
        _mock_locations_requests(http_mocker, location_slices)
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)
        _mock_party_members_skills_requests(http_mocker)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=_CATALOG, state=state)
        )

    # Expects 8 records, skip successful intervals and are left with 2 slices, 4 records each slice
    locations_records = get_records_for_stream("locations", messages)
    assert len(locations_records) == 8

    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 3
    assert (
        locations_states[2].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-08-10",
                }
            ],
        ).__dict__
    )

    # slices to sync are:
    # * {"start": "2024-07-01", "end": "2024-07-15"}: one record in _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES
    # * {"start": "2024-09-04", "end": "2024-09-10"}: one record from the lookback window defined in this test
    party_members_records = get_records_for_stream("party_members", messages)
    assert len(party_members_records) == 2

    party_members_states = get_states_for_stream(stream_name="party_members", messages=messages)
    assert len(party_members_states) == 4
    assert (
        party_members_states[3].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-09-10",
                }
            ],
        ).__dict__
    )

    # Expects 7 records, 1 empty slice, 7 records in slice
    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7

    # Expects 3 records, 3 slices, 3 records in slice
    party_members_skills_records = get_records_for_stream("party_members_skills", messages)
    assert len(party_members_skills_records) == 9


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_with_concurrent_and_synchronous_streams_with_sequential_state():
    """
    Verifies that a ConcurrentDeclarativeSource processes concurrent streams correctly using the incoming
    legacy state format
    """
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(updated_at="2024-08-06"),
            ),
        ),
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="party_members", namespace=None),
                stream_state=AirbyteStateBlob(updated_at="2024-08-21"),
            ),
        ),
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )

    party_members_slices_and_responses = _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES + [
        (
            {"start": "2024-08-16", "end": "2024-08-30"},
            HttpResponse(
                json.dumps(
                    [
                        {
                            "id": "nijima",
                            "first_name": "makoto",
                            "last_name": "nijima",
                            "updated_at": "2024-08-10",
                        }
                    ]
                )
            ),
        ),  # considering lookback window
        (
            {"start": "2024-08-31", "end": "2024-09-10"},
            HttpResponse(
                json.dumps(
                    [
                        {
                            "id": "yoshizawa",
                            "first_name": "sumire",
                            "last_name": "yoshizawa",
                            "updated_at": "2024-09-10",
                        }
                    ]
                )
            ),
        ),
    ]
    location_slices = [
        {"start": "2024-08-01", "end": "2024-08-31"},
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]

    with HttpMocker() as http_mocker:
        _mock_party_members_requests(http_mocker, party_members_slices_and_responses)
        _mock_locations_requests(http_mocker, location_slices)
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)
        _mock_party_members_skills_requests(http_mocker)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=_CATALOG, state=state)
        )

    # Expects 8 records, skip successful intervals and are left with 2 slices, 4 records each slice
    locations_records = get_records_for_stream("locations", messages)
    assert len(locations_records) == 8

    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 3
    assert (
        locations_states[2].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-08-10",
                }
            ],
        ).__dict__
    )

    # From extra slices defined in party_members_slices_and_responses
    party_members_records = get_records_for_stream("party_members", messages)
    assert len(party_members_records) == 2

    party_members_states = get_states_for_stream(stream_name="party_members", messages=messages)
    assert len(party_members_states) == 3
    assert (
        party_members_states[2].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-09-10",
                }
            ],
        ).__dict__
    )

    # Expects 7 records, 1 empty slice, 7 records in slice
    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7

    # Expects 3 records, 3 slices, 3 records in slice
    party_members_skills_records = get_records_for_stream("party_members_skills", messages)
    assert len(party_members_skills_records) == 9


def test_concurrent_declarative_source_runs_state_migrations_provided_in_manifest():
    manifest = {
        "version": "5.0.0",
        "definitions": {
            "selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://persona.metaverse.com",
                "http_method": "GET",
                "authenticator": {
                    "type": "BasicHttpAuthenticator",
                    "username": "{{ config['api_key'] }}",
                    "password": "{{ config['secret_key'] }}",
                },
                "error_handler": {
                    "type": "DefaultErrorHandler",
                    "response_filters": [
                        {
                            "http_codes": [403],
                            "action": "FAIL",
                            "failure_type": "config_error",
                            "error_message": "Access denied due to lack of permission or invalid API/Secret key or wrong data region.",
                        },
                        {
                            "http_codes": [404],
                            "action": "IGNORE",
                            "error_message": "No data available for the time range requested.",
                        },
                    ],
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "paginator": {"type": "NoPagination"},
                "requester": {"$ref": "#/definitions/requester"},
            },
            "incremental_cursor": {
                "type": "DatetimeBasedCursor",
                "start_datetime": {
                    "datetime": "{{ format_datetime(config['start_date'], '%Y-%m-%d') }}"
                },
                "end_datetime": {"datetime": "{{ now_utc().strftime('%Y-%m-%d') }}"},
                "datetime_format": "%Y-%m-%d",
                "cursor_datetime_formats": ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
                "cursor_granularity": "P1D",
                "step": "P15D",
                "cursor_field": "updated_at",
                "lookback_window": "P5D",
                "start_time_option": {
                    "type": "RequestOption",
                    "field_name": "start",
                    "inject_into": "request_parameter",
                },
                "end_time_option": {
                    "type": "RequestOption",
                    "field_name": "end",
                    "inject_into": "request_parameter",
                },
            },
            "base_stream": {"retriever": {"$ref": "#/definitions/retriever"}},
            "base_incremental_stream": {
                "retriever": {
                    "$ref": "#/definitions/retriever",
                    "requester": {"$ref": "#/definitions/requester"},
                },
                "incremental_sync": {"$ref": "#/definitions/incremental_cursor"},
            },
            "party_members_stream": {
                "$ref": "#/definitions/base_incremental_stream",
                "retriever": {
                    "$ref": "#/definitions/base_incremental_stream/retriever",
                    "requester": {
                        "$ref": "#/definitions/requester",
                        "request_parameters": {"filter": "{{stream_partition['type']}}"},
                    },
                    "record_selector": {"$ref": "#/definitions/selector"},
                    "partition_router": [
                        {
                            "type": "ListPartitionRouter",
                            "values": ["type_1", "type_2"],
                            "cursor_field": "type",
                        }
                    ],
                },
                "$parameters": {
                    "name": "party_members",
                    "primary_key": "id",
                    "path": "/party_members",
                },
                "state_migrations": [
                    {
                        "type": "CustomStateMigration",
                        "class_name": "unit_tests.sources.declarative.custom_state_migration.CustomStateMigration",
                    }
                ],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "https://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "properties": {
                            "id": {
                                "description": "The identifier",
                                "type": ["null", "string"],
                            },
                            "name": {
                                "description": "The name of the party member",
                                "type": ["null", "string"],
                            },
                        },
                    },
                },
            },
        },
        "streams": [
            "#/definitions/party_members_stream",
        ],
        "check": {"stream_names": ["party_members", "locations"]},
        "concurrency_level": {
            "type": "ConcurrencyLevel",
            "default_concurrency": "{{ config['num_workers'] or 10 }}",
            "max_concurrency": 25,
        },
    }
    state_blob = AirbyteStateBlob(updated_at="2024-08-21")
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="party_members", namespace=None),
                stream_state=state_blob,
            ),
        ),
    ]
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=_CONFIG, catalog=_CATALOG, state=state
    )
    streams = source.streams(_CONFIG)
    assert streams[0].cursor.state.get("state") != state_blob.__dict__, "State was not migrated."
    assert streams[0].cursor.state.get("states") == [
        {"cursor": {"updated_at": "2024-08-21"}, "partition": {"type": "type_1"}},
        {"cursor": {"updated_at": "2024-08-21"}, "partition": {"type": "type_2"}},
    ], "State was migrated, but actual state don't match expected"


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_concurrent_with_failing_partition_in_the_middle():
    """
    Verify that partial state is emitted when only some partitions are successful during a concurrent sync attempt
    """
    location_slices = [
        {"start": "2024-07-01", "end": "2024-07-31"},
        # missing slice `{"start": "2024-08-01", "end": "2024-08-31"}` here
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]
    expected_stream_state = {
        "state_type": "date-range",
        "slices": [
            location_slice | {"most_recent_cursor_value": "2024-08-10"}
            for location_slice in location_slices
        ],
    }

    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="locations", json_schema={}, supported_sync_modes=[SyncMode.incremental]
                ),
                sync_mode=SyncMode.incremental,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=catalog, state=[]
    )

    location_slices = [
        {"start": "2024-07-01", "end": "2024-07-31"},
        # missing slice `{"start": "2024-08-01", "end": "2024-08-31"}` here
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]

    with HttpMocker() as http_mocker:
        _mock_locations_requests(http_mocker, location_slices)

        messages = []
        try:
            for message in source.read(
                logger=source.logger, config=_CONFIG, catalog=catalog, state=[]
            ):
                messages.append(message)
        except AirbyteTracedException:
            locations_states = get_states_for_stream(stream_name="locations", messages=messages)
            assert len(locations_states) == 3
            assert (
                get_states_for_stream(stream_name="locations", messages=messages)[
                    -1
                ].stream.stream_state.__dict__
                == expected_stream_state
            )


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_concurrent_skip_streams_not_in_catalog():
    """
    Verifies that the ConcurrentDeclarativeSource only syncs streams that are specified in the incoming ConfiguredCatalog
    """
    with HttpMocker() as http_mocker:
        catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                    ),
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.append,
                ),
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="locations",
                        json_schema={},
                        supported_sync_modes=[SyncMode.incremental],
                    ),
                    sync_mode=SyncMode.incremental,
                    destination_sync_mode=DestinationSyncMode.append,
                ),
            ]
        )

        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=_CONFIG, catalog=catalog, state=None
        )
        # locations requests
        location_slices = [
            {"start": "2024-07-01", "end": "2024-07-31"},
            {"start": "2024-08-01", "end": "2024-08-31"},
            {"start": "2024-09-01", "end": "2024-09-10"},
        ]
        locations_query_params = list(
            map(lambda _slice: _slice | {"m": "active", "i": "1", "g": "country"}, location_slices)
        )
        _mock_requests(
            http_mocker,
            "https://persona.metaverse.com/locations",
            locations_query_params,
            [_LOCATIONS_RESPONSE] * len(location_slices),
        )

        # palaces requests
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=catalog, state=[])
        )

    locations_records = get_records_for_stream(stream_name="locations", messages=messages)
    assert len(locations_records) == 12
    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 4

    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7
    palaces_states = get_states_for_stream(stream_name="palaces", messages=messages)
    assert len(palaces_states) == 1

    assert len(get_records_for_stream(stream_name="party_members", messages=messages)) == 0
    assert len(get_states_for_stream(stream_name="party_members", messages=messages)) == 0

    assert len(get_records_for_stream(stream_name="party_members_skills", messages=messages)) == 0
    assert len(get_states_for_stream(stream_name="party_members_skills", messages=messages)) == 0


def test_default_perform_interpolation_on_concurrency_level():
    config = {"start_date": "2024-07-01T00:00:00.000Z", "num_workers": 20}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=config, catalog=catalog, state=[]
    )
    assert (
        source._concurrent_source._initial_number_partitions_to_generate == 10
    )  # We floor the number of initial partitions on creation


def test_default_to_single_threaded_when_no_concurrency_level():
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    manifest = copy.deepcopy(_MANIFEST)
    del manifest["concurrency_level"]

    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=_CONFIG, catalog=catalog, state=[]
    )
    assert source._concurrent_source._initial_number_partitions_to_generate == 1


def test_concurrency_level_initial_number_partitions_to_generate_is_always_one_or_more():
    config = {"start_date": "2024-07-01T00:00:00.000Z", "num_workers": 1}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    manifest = copy.deepcopy(_MANIFEST)
    manifest["concurrency_level"] = {
        "type": "ConcurrencyLevel",
        "default_concurrency": "{{ config.get('num_workers', 1) }}",
        "max_concurrency": 25,
    }

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=config, catalog=catalog, state=[]
    )
    assert source._concurrent_source._initial_number_partitions_to_generate == 1


def test_async_incremental_stream_uses_concurrent_cursor_with_state():
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="async_job_stream", namespace=None),
                stream_state=AirbyteStateBlob(updated_at="2024-08-06"),
            ),
        )
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )

    expected_state = {
        "legacy": {"updated_at": "2024-08-06"},
        "slices": [
            {
                "end": datetime(2024, 8, 6, 0, 0, tzinfo=timezone.utc),
                "most_recent_cursor_value": datetime(2024, 8, 6, 0, 0, tzinfo=timezone.utc),
                "start": datetime(2024, 7, 1, 0, 0, tzinfo=timezone.utc),
            }
        ],
        "state_type": "date-range",
    }

    streams = source.streams(config=_CONFIG)
    async_job_stream = streams[6]
    assert isinstance(async_job_stream, DefaultStream)
    cursor = async_job_stream._cursor
    assert isinstance(cursor, ConcurrentCursor)
    assert cursor._concurrent_state == expected_state
    stream_partition_generator = async_job_stream._stream_partition_generator
    assert isinstance(stream_partition_generator, StreamSlicerPartitionGenerator)
    async_job_partition_router = stream_partition_generator._stream_slicer
    assert isinstance(async_job_partition_router, AsyncJobPartitionRouter)
    assert isinstance(async_job_partition_router.stream_slicer, ConcurrentCursor)
    assert async_job_partition_router.stream_slicer._concurrent_state == expected_state


def test_max_concurrent_async_job_count_is_passed_to_job_tracker():
    limit = 5
    manifest_with_max_concurrent_async_job_count = copy.deepcopy(_MANIFEST)
    manifest_with_max_concurrent_async_job_count["max_concurrent_async_job_count"] = str(limit)
    source = ConcurrentDeclarativeSource(
        source_config=manifest_with_max_concurrent_async_job_count,
        config=_CONFIG,
        catalog=_CATALOG,
        state={},
    )
    source_job_tracker = source._constructor._job_tracker
    assert source_job_tracker._limit == limit

    [source_job_tracker.try_to_get_intent() for i in range(limit)]
    with pytest.raises(ConcurrentJobLimitReached):
        source_job_tracker.try_to_get_intent()


def test_stream_using_is_client_side_incremental_has_cursor_state():
    expected_cursor_value = "2024-07-01"
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(updated_at=expected_cursor_value),
            ),
        )
    ]

    manifest_with_stream_state_interpolation = copy.deepcopy(_MANIFEST)

    # Enable semi-incremental on the locations stream
    manifest_with_stream_state_interpolation["definitions"]["locations_stream"]["incremental_sync"][
        "is_client_side_incremental"
    ] = True

    source = ConcurrentDeclarativeSource(
        source_config=manifest_with_stream_state_interpolation,
        config=_CONFIG,
        catalog=_CATALOG,
        state=state,
    )
    streams = source.streams(config=_CONFIG)

    locations_stream = streams[2]
    assert isinstance(locations_stream, DefaultStream)

    simple_retriever = locations_stream._stream_partition_generator._partition_factory._retriever
    record_filter = simple_retriever.record_selector.record_filter
    assert isinstance(record_filter, ClientSideIncrementalRecordFilterDecorator)
    assert list(record_filter._cursor.state.values()) == [expected_cursor_value]


@pytest.mark.parametrize(
    "expected_transform_before_filtering",
    [
        pytest.param(
            True,
            id="transform before filtering",
        ),
        pytest.param(
            False,
            id="transform after filtering",
        ),
        pytest.param(
            None,
            id="default transform before filtering",
        ),
    ],
)
def test_stream_using_is_client_side_incremental_has_transform_before_filtering_according_to_manifest(
    expected_transform_before_filtering,
):
    expected_cursor_value = "2024-07-01"
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(updated_at=expected_cursor_value),
            ),
        )
    ]

    manifest_with_stream_state_interpolation = copy.deepcopy(_MANIFEST)

    # Enable semi-incremental on the locations stream
    manifest_with_stream_state_interpolation["definitions"]["locations_stream"]["incremental_sync"][
        "is_client_side_incremental"
    ] = True

    if expected_transform_before_filtering is not None:
        manifest_with_stream_state_interpolation["definitions"]["locations_stream"]["retriever"][
            "record_selector"
        ]["transform_before_filtering"] = expected_transform_before_filtering

    source = ConcurrentDeclarativeSource(
        source_config=manifest_with_stream_state_interpolation,
        config=_CONFIG,
        catalog=_CATALOG,
        state=state,
    )
    streams = source.streams(config=_CONFIG)

    locations_stream = streams[2]
    assert isinstance(locations_stream, DefaultStream)

    simple_retriever = locations_stream._stream_partition_generator._partition_factory._retriever
    record_selector = simple_retriever.record_selector

    if expected_transform_before_filtering is not None:
        assert record_selector.transform_before_filtering == expected_transform_before_filtering
    else:
        assert record_selector.transform_before_filtering is True


def create_wrapped_stream(stream: DeclarativeStream) -> Stream:
    slice_to_records_mapping = get_mocked_read_records_output(stream_name=stream.name)

    return DeclarativeStreamDecorator(
        declarative_stream=stream, slice_to_records_mapping=slice_to_records_mapping
    )


def get_mocked_read_records_output(stream_name: str) -> Mapping[tuple[str, str], List[StreamData]]:
    match stream_name:
        case "locations":
            slices = [
                # Slices used during first incremental sync
                StreamSlice(
                    cursor_slice={"start": "2024-07-01", "end": "2024-07-31"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-01", "end": "2024-08-31"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-09-01", "end": "2024-09-09"}, partition={}
                ),
                # Slices used during incremental checkpoint sync
                StreamSlice(
                    cursor_slice={"start": "2024-07-26", "end": "2024-08-25"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-26", "end": "2024-09-09"}, partition={}
                ),
                # Slices used during incremental sync with some partitions that exit with an error
                StreamSlice(
                    cursor_slice={"start": "2024-07-05", "end": "2024-08-04"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-05", "end": "2024-09-04"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-09-05", "end": "2024-09-09"}, partition={}
                ),
            ]

            records = [
                {"id": "444", "name": "Yongen-jaya", "updated_at": "2024-08-10"},
                {"id": "scramble", "name": "Shibuya", "updated_at": "2024-08-10"},
                {"id": "aoyama", "name": "Aoyama-itchome", "updated_at": "2024-08-10"},
                {"id": "shin123", "name": "Shinjuku", "updated_at": "2024-08-10"},
            ]
        case "party_members":
            slices = [
                # Slices used during first incremental sync
                StreamSlice(
                    cursor_slice={"start": "2024-07-01", "end": "2024-07-15"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-07-16", "end": "2024-07-30"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-07-31", "end": "2024-08-14"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-15", "end": "2024-08-29"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-30", "end": "2024-09-09"}, partition={}
                ),
                # Slices used during incremental checkpoint sync. Unsuccessful partitions use the P5D lookback window which explains
                # the skew of records midway through
                StreamSlice(
                    cursor_slice={"start": "2024-07-01", "end": "2024-07-16"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-07-30", "end": "2024-08-13"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-14", "end": "2024-08-14"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-09-04", "end": "2024-09-09"}, partition={}
                ),
            ]

            records = [
                {
                    "id": "amamiya",
                    "first_name": "ren",
                    "last_name": "amamiya",
                    "updated_at": "2024-07-10",
                },
                {
                    "id": "nijima",
                    "first_name": "makoto",
                    "last_name": "nijima",
                    "updated_at": "2024-08-10",
                },
                {
                    "id": "yoshizawa",
                    "first_name": "sumire",
                    "last_name": "yoshizawa",
                    "updated_at": "2024-09-10",
                },
            ]
        case "palaces":
            slices = [StreamSlice(cursor_slice={}, partition={})]

            records = [
                {"id": "0", "world": "castle", "owner": "kamoshida"},
                {"id": "1", "world": "museum", "owner": "madarame"},
                {"id": "2", "world": "bank", "owner": "kaneshiro"},
                {"id": "3", "world": "pyramid", "owner": "futaba"},
                {"id": "4", "world": "spaceport", "owner": "okumura"},
                {"id": "5", "world": "casino", "owner": "nijima"},
                {"id": "6", "world": "cruiser", "owner": "shido"},
            ]

        case "party_members_skills":
            slices = [StreamSlice(cursor_slice={}, partition={})]

            records = [
                {"id": "0", "name": "hassou tobi"},
                {"id": "1", "name": "mafreidyne"},
                {"id": "2", "name": "myriad truths"},
            ]
        case _:
            raise ValueError(f"Stream '{stream_name}' does not have associated mocked records")

    return {
        (_slice.get("start"), _slice.get("end")): [
            Record(data=stream_data, associated_slice=_slice) for stream_data in records
        ]
        for _slice in slices
    }


@freezegun.freeze_time("2025-01-01T00:00:00")
def test_catalog_contains_missing_stream_in_source():
    expected_messages = [
        AirbyteMessage(
            type=Type.TRACE,
            trace=AirbyteTraceMessage(
                type=TraceType.STREAM_STATUS,
                stream_status=AirbyteStreamStatusTraceMessage(
                    stream_descriptor=StreamDescriptor(name="missing"),
                    status=AirbyteStreamStatus.INCOMPLETE,
                ),
                emitted_at=1735689600000.0,
            ),
        ),
    ]

    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="missing", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=catalog, state=[]
    )

    list(source.read(logger=source.logger, config=_CONFIG, catalog=catalog, state=[]))
    queue = source._concurrent_source._queue

    for expected_message in expected_messages:
        queue_message = queue.get()
        assert queue_message == expected_message


def get_records_for_stream(
    stream_name: str, messages: List[AirbyteMessage]
) -> List[AirbyteRecordMessage]:
    return [
        message.record
        for message in messages
        if message.record and message.record.stream == stream_name
    ]


def get_states_for_stream(
    stream_name: str, messages: List[AirbyteMessage]
) -> List[AirbyteStateMessage]:
    return [
        message.state
        for message in messages
        if message.state and message.state.stream.stream_descriptor.name == stream_name
    ]


# The tests below were originally written to test the ManifestDeclarativeSource class. However,
# after deprecating the class when migrating away from legacy synchronous CDK flow, the tests
# were adjusted to validate ConcurrentDeclarativeSource.


class MockConcurrentDeclarativeSource(ConcurrentDeclarativeSource):
    """
    Mock test class that is needed to monkey patch how we read from various files that make up a declarative source because of how our
    tests write configuration files during testing. It is also used to properly namespace where files get written in specific
    cases like when we temporarily write files like spec.yaml to the package unit_tests, which is the directory where it will
    be read in during the tests.
    """


def create_catalog(stream_name: str) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=stream_name, json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            )
        ]
    )


class TestConcurrentDeclarativeSource:
    @pytest.fixture
    def use_external_yaml_spec(self):
        # Our way of resolving the absolute path to root of the airbyte-cdk unit test directory where spec.yaml files should
        # be written to (i.e. ~/airbyte/airbyte-cdk/python/unit-tests) because that is where they are read from during testing.
        module = sys.modules[__name__]
        module_path = os.path.abspath(module.__file__)
        test_path = os.path.dirname(module_path)
        spec_root = test_path.split("/sources/declarative")[0]

        spec = {
            "documentationUrl": "https://airbyte.com/#yaml-from-external",
            "connectionSpecification": EXTERNAL_CONNECTION_SPECIFICATION,
        }

        yaml_path = os.path.join(spec_root, "spec.yaml")
        with open(yaml_path, "w") as f:
            f.write(yaml.dump(spec))
        yield
        os.remove(yaml_path)

    @pytest.fixture
    def _base_manifest(self):
        """Base manifest without streams or dynamic streams."""
        return {
            "version": "3.8.2",
            "description": "This is a sample source connector that is very valid.",
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }

    @pytest.fixture
    def _declarative_stream(self):
        def declarative_stream_config(
            name="lists", requester_type="HttpRequester", custom_requester=None
        ):
            """Generates a DeclarativeStream configuration."""
            requester_config = {
                "type": requester_type,
                "path": "/v3/marketing/lists",
                "authenticator": {
                    "type": "BearerAuthenticator",
                    "api_token": "{{ config.apikey }}",
                },
                "request_parameters": {"page_size": "{{ 10 }}"},
            }
            if custom_requester:
                requester_config.update(custom_requester)

            return {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": name,
                    "primary_key": "id",
                    "url_base": "https://api.sendgrid.com",
                },
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": f"./source_sendgrid/schemas/{{{{ parameters.name }}}}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                            "page_size": 10,
                        },
                    },
                    "requester": requester_config,
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            }

        return declarative_stream_config

    @pytest.fixture
    def _dynamic_declarative_stream(self, _declarative_stream):
        """Generates a DynamicDeclarativeStream configuration."""
        return {
            "type": "DynamicDeclarativeStream",
            "stream_template": _declarative_stream(),
            "components_resolver": {
                "type": "HttpComponentsResolver",
                "$parameters": {
                    "name": "lists",
                    "primary_key": "id",
                    "url_base": "https://api.sendgrid.com",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                            "page_size": 10,
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{ components_value['name'] }}",
                    }
                ],
            },
        }

    def test_valid_manifest(self):
        manifest = {
            "version": "3.8.2",
            "definitions": {},
            "description": "This is a sample source connector that is very valid.",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "$schema": "http://json-schema.org/schema#",
                            "properties": {
                                "id": {"type": "string"},
                            },
                            "type": "object",
                        },
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "stream_with_custom_requester",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "$schema": "http://json-schema.org/schema#",
                            "properties": {
                                "id": {"type": "string"},
                            },
                            "type": "object",
                        },
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "type": "CustomRequester",
                            "class_name": "unit_tests.sources.declarative.external_component.SampleCustomComponent",
                            "path": "/v3/marketing/lists",
                            "custom_request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        assert "unit_tests" in sys.modules
        assert "unit_tests.sources" in sys.modules
        assert "unit_tests.sources.declarative" in sys.modules
        assert "unit_tests.sources.declarative.external_component" in sys.modules

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
        )

        pages = [_create_page({"records": [{"id": 0}], "_metadata": {}})]
        with patch.object(SimpleRetriever, "_fetch_next_page", side_effect=pages):
            connection_status = source.check(logging.getLogger(""), {})
            assert connection_status.status == Status.SUCCEEDED

        streams = source.streams({})
        assert len(streams) == 2
        assert isinstance(streams[0], DefaultStream)
        assert isinstance(streams[1], DefaultStream)
        assert (
            source.resolved_manifest["description"]
            == "This is a sample source connector that is very valid."
        )

    def test_manifest_with_spec(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
            "spec": {
                "type": "Spec",
                "documentation_url": "https://airbyte.com/#yaml-from-manifest",
                "connection_specification": {
                    "title": "Test Spec",
                    "type": "object",
                    "required": ["api_key"],
                    "additionalProperties": False,
                    "properties": {
                        "api_key": {
                            "type": "string",
                            "airbyte_secret": True,
                            "title": "API Key",
                            "description": "Test API Key",
                            "order": 0,
                        }
                    },
                },
            },
        }
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
        )
        connector_specification = source.spec(logger)
        assert connector_specification is not None
        assert connector_specification.documentationUrl == "https://airbyte.com/#yaml-from-manifest"
        assert connector_specification.connectionSpecification["title"] == "Test Spec"
        assert connector_specification.connectionSpecification["required"][0] == "api_key"
        assert connector_specification.connectionSpecification["additionalProperties"] is False
        assert connector_specification.connectionSpecification["properties"]["api_key"] == {
            "type": "string",
            "airbyte_secret": True,
            "title": "API Key",
            "description": "Test API Key",
            "order": 0,
        }

    def test_manifest_with_external_spec(self, use_external_yaml_spec):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        source = MockConcurrentDeclarativeSource(
            source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
        )

        connector_specification = source.spec(logger)

        assert connector_specification.documentationUrl == "https://airbyte.com/#yaml-from-external"
        assert connector_specification.connectionSpecification == EXTERNAL_CONNECTION_SPECIFICATION

    def test_source_is_not_created_if_toplevel_fields_are_unknown(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": 10},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
            "not_a_valid_field": "error",
        }
        with pytest.raises(ValidationError):
            ConcurrentDeclarativeSource(
                source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
            )

    def test_source_missing_checker_fails_validation(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": 10},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
        }
        with pytest.raises(ValidationError):
            ConcurrentDeclarativeSource(
                source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
            )

    def test_source_with_missing_streams_and_dynamic_streams_fails(
        self, _base_manifest, _dynamic_declarative_stream, _declarative_stream
    ):
        # test case for manifest without streams or dynamic streams
        manifest_without_streams_and_dynamic_streams = _base_manifest
        with pytest.raises(ValidationError):
            ConcurrentDeclarativeSource(
                source_config=manifest_without_streams_and_dynamic_streams,
                config={},
                catalog=create_catalog("lists"),
                state=None,
            )

        # test case for manifest with streams
        manifest_with_streams = {
            **manifest_without_streams_and_dynamic_streams,
            "streams": [
                _declarative_stream(name="lists"),
                _declarative_stream(
                    name="stream_with_custom_requester",
                    requester_type="CustomRequester",
                    custom_requester={
                        "class_name": "unit_tests.sources.declarative.external_component.SampleCustomComponent",
                        "custom_request_parameters": {"page_size": 10},
                    },
                ),
            ],
        }
        ConcurrentDeclarativeSource(
            source_config=manifest_with_streams,
            config={},
            catalog=create_catalog("lists"),
            state=None,
        )

        # test case for manifest with dynamic streams
        manifest_with_dynamic_streams = {
            **manifest_without_streams_and_dynamic_streams,
            "dynamic_streams": [_dynamic_declarative_stream],
        }
        ConcurrentDeclarativeSource(
            source_config=manifest_with_dynamic_streams,
            config={},
            catalog=create_catalog("lists"),
            state=None,
        )

    def test_source_with_missing_version_fails(self):
        manifest = {
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": 10},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        with pytest.raises(ValidationError):
            ConcurrentDeclarativeSource(
                source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
            )

    def test_source_with_invalid_stream_config_fails_validation(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                }
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        with pytest.raises(ValidationError):
            ConcurrentDeclarativeSource(
                source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
            )

    def test_source_with_no_external_spec_and_no_in_yaml_spec_fails(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
        )

        # We expect to fail here because we have not created a temporary spec.yaml file
        with pytest.raises(FileNotFoundError):
            source.spec(logger)

    @pytest.mark.parametrize(
        "is_sandbox, expected_stream_count",
        [
            pytest.param(True, 3, id="test_sandbox_config_includes_conditional_streams"),
            pytest.param(False, 1, id="test_non_sandbox_config_skips_conditional_streams"),
        ],
    )
    def test_conditional_streams_manifest(self, is_sandbox, expected_stream_count):
        manifest = {
            "version": "3.8.2",
            "definitions": {},
            "description": "This is a sample source connector that is very valid.",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "students",
                        "primary_key": "id",
                        "url_base": "https://api.yasogamihighschool.com",
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "$schema": "http://json-schema.org/schema#",
                            "properties": {
                                "id": {"type": "string"},
                                "first_name": {"type": "string"},
                                "last_name": {"type": "string"},
                                "grade": {"type": "number"},
                            },
                            "type": "object",
                        },
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v1/students",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "ConditionalStreams",
                    "condition": "{{ config['is_sandbox'] }}",
                    "streams": [
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "classrooms",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "type": "InlineSchemaLoader",
                                "schema": {
                                    "$schema": "http://json-schema.org/schema#",
                                    "properties": {
                                        "id": {"type": "string"},
                                        "floor": {"type": "number"},
                                        "room_number": {"type": "number"},
                                    },
                                    "type": "object",
                                },
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/classrooms",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "clubs",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "type": "InlineSchemaLoader",
                                "schema": {
                                    "$schema": "http://json-schema.org/schema#",
                                    "properties": {
                                        "id": {"type": "string"},
                                        "name": {"type": "string"},
                                        "category": {"type": "string"},
                                    },
                                    "type": "object",
                                },
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/clubs",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                    ],
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["students"]},
        }

        assert "unit_tests" in sys.modules
        assert "unit_tests.sources" in sys.modules
        assert "unit_tests.sources.declarative" in sys.modules
        assert "unit_tests.sources.declarative.external_component" in sys.modules

        config = {"is_sandbox": is_sandbox}
        catalog = create_catalog("students")

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=catalog, state=None
        )

        pages = [
            _create_page(
                {
                    "students": [{"id": 0, "first_name": "yu", "last_name": "narukami"}],
                    "_metadata": {},
                }
            )
        ]
        with patch.object(SimpleRetriever, "_fetch_next_page", side_effect=pages):
            connection_status = source.check(logging.getLogger(""), config=config)
            assert connection_status.status == Status.SUCCEEDED

        actual_streams = source.streams(config=config)
        assert len(actual_streams) == expected_stream_count
        assert isinstance(actual_streams[0], DefaultStream)
        assert actual_streams[0].name == "students"

        if is_sandbox:
            assert isinstance(actual_streams[1], DefaultStream)
            assert actual_streams[1].name == "classrooms"
            assert isinstance(actual_streams[2], DefaultStream)
            assert actual_streams[2].name == "clubs"

        assert (
            source.resolved_manifest["description"]
            == "This is a sample source connector that is very valid."
        )

    @pytest.mark.parametrize(
        "field_to_remove,expected_error",
        [
            pytest.param("condition", ValidationError, id="test_no_condition_raises_error"),
            pytest.param("streams", ValidationError, id="test_no_streams_raises_error"),
        ],
    )
    def test_conditional_streams_invalid_manifest(self, field_to_remove, expected_error):
        manifest = {
            "version": "3.8.2",
            "definitions": {},
            "description": "This is a sample source connector that is very valid.",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "students",
                        "primary_key": "id",
                        "url_base": "https://api.yasogamihighschool.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v1/students",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "ConditionalStreams",
                    "condition": "{{ config['is_sandbox'] }}",
                    "streams": [
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "classrooms",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "name": "{{ parameters.stream_name }}",
                                "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/classrooms",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                    "request_parameters": {"page_size": "{{ 10 }}"},
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "clubs",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "name": "{{ parameters.stream_name }}",
                                "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/clubs",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                    "request_parameters": {"page_size": "{{ 10 }}"},
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                    ],
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["students"]},
        }

        assert "unit_tests" in sys.modules
        assert "unit_tests.sources" in sys.modules
        assert "unit_tests.sources.declarative" in sys.modules
        assert "unit_tests.sources.declarative.external_component" in sys.modules

        del manifest["streams"][1][field_to_remove]

        with pytest.raises(ValidationError):
            ConcurrentDeclarativeSource(
                source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
            )


def request_log_message(request: dict) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.LOG,
        log=AirbyteLogMessage(level=Level.INFO, message=f"request:{json.dumps(request)}"),
    )


def response_log_message(response: dict) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.LOG,
        log=AirbyteLogMessage(level=Level.INFO, message=f"response:{json.dumps(response)}"),
    )


def _create_request():
    url = "https://example.com/api"
    headers = {"Content-Type": "application/json"}
    return requests.Request("POST", url, headers=headers, json={"key": "value"}).prepare()


def _create_response(body):
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(json.dumps(body), "utf-8")
    response.headers["Content-Type"] = "application/json"
    return response


def _create_page(response_body):
    response = _create_response(response_body)
    response.request = _create_request()
    return response


@pytest.mark.parametrize(
    "test_name, manifest, pages, expected_records, expected_calls",
    [
        (
            "test_read_manifest_no_pagination_no_partitions",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {"type": "NoPagination"},
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            }
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page({"rates": [{"ABC": 0}, {"AED": 1}], "_metadata": {"next": "next"}}),
                _create_page({"rates": [{"USD": 2}], "_metadata": {"next": "next"}}),
            )
            * 10,
            [{"ABC": 0}, {"AED": 1}],
            [call({}, {}, None)],
        ),
        (
            "test_read_manifest_with_added_fields",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "transformations": [
                            {
                                "type": "AddFields",
                                "fields": [
                                    {
                                        "type": "AddedFieldDefinition",
                                        "path": ["added_field_key"],
                                        "value": "added_field_value",
                                    }
                                ],
                            }
                        ],
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {"type": "NoPagination"},
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            }
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page({"rates": [{"ABC": 0}, {"AED": 1}], "_metadata": {"next": "next"}}),
                _create_page({"rates": [{"USD": 2}], "_metadata": {"next": "next"}}),
            )
            * 10,
            [
                {"ABC": 0, "added_field_key": "added_field_value"},
                {"AED": 1, "added_field_key": "added_field_value"},
            ],
            [call({}, {}, None)],
        ),
        (
            "test_read_manifest_with_flatten_fields",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "transformations": [{"type": "FlattenFields"}],
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {"type": "NoPagination"},
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            }
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page(
                    {
                        "rates": [
                            {"nested_fields": {"ABC": 0}, "id": 1},
                            {"nested_fields": {"AED": 1}, "id": 2},
                        ],
                        "_metadata": {"next": "next"},
                    }
                ),
                _create_page({"rates": [{"USD": 2}], "_metadata": {"next": "next"}}),
            )
            * 10,
            [
                {"ABC": 0, "id": 1},
                {"AED": 1, "id": 2},
            ],
            [call({}, {}, None)],
        ),
        (
            "test_read_with_pagination_no_partitions",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                    "USD": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {
                                "type": "DefaultPaginator",
                                "page_size": 2,
                                "page_size_option": {
                                    "inject_into": "request_parameter",
                                    "field_name": "page_size",
                                },
                                "page_token_option": {"inject_into": "path", "type": "RequestPath"},
                                "pagination_strategy": {
                                    "type": "CursorPagination",
                                    "cursor_value": "{{ response._metadata.next }}",
                                    "page_size": 2,
                                },
                            },
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            }
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page({"rates": [{"ABC": 0}, {"AED": 1}], "_metadata": {"next": "next"}}),
                _create_page({"rates": [{"USD": 2}], "_metadata": {}}),
            )
            * 10,
            [{"ABC": 0}, {"AED": 1}, {"USD": 2}],
            [
                call({}, {}, None),
                call({}, {}, {"next_page_token": "next"}),
            ],
        ),
        (
            "test_no_pagination_with_partition_router",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                    "partition": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "partition_router": {
                                "type": "ListPartitionRouter",
                                "values": ["0", "1"],
                                "cursor_field": "partition",
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {"type": "NoPagination"},
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            }
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page(
                    {
                        "rates": [{"ABC": 0, "partition": 0}, {"AED": 1, "partition": 0}],
                        "_metadata": {"next": "next"},
                    }
                ),
                _create_page(
                    {"rates": [{"ABC": 2, "partition": 1}], "_metadata": {"next": "next"}}
                ),
            ),
            [{"ABC": 0, "partition": 0}, {"AED": 1, "partition": 0}, {"ABC": 2, "partition": 1}],
            [
                call({}, {"partition": "0"}, None),
                call({}, {"partition": "1"}, None),
            ],
        ),
        (
            "test_with_pagination_and_partition_router",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                    "partition": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "partition_router": {
                                "type": "ListPartitionRouter",
                                "values": ["0", "1"],
                                "cursor_field": "partition",
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {
                                "type": "DefaultPaginator",
                                "page_size": 2,
                                "page_size_option": {
                                    "inject_into": "request_parameter",
                                    "field_name": "page_size",
                                },
                                "page_token_option": {"inject_into": "path", "type": "RequestPath"},
                                "pagination_strategy": {
                                    "type": "CursorPagination",
                                    "cursor_value": "{{ response._metadata.next }}",
                                    "page_size": 2,
                                },
                            },
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            }
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page(
                    {
                        "rates": [{"ABC": 0, "partition": 0}, {"AED": 1, "partition": 0}],
                        "_metadata": {"next": "next"},
                    }
                ),
                _create_page({"rates": [{"USD": 3, "partition": 0}], "_metadata": {}}),
                _create_page({"rates": [{"ABC": 2, "partition": 1}], "_metadata": {}}),
            ),
            [
                {"ABC": 0, "partition": 0},
                {"AED": 1, "partition": 0},
                {"USD": 3, "partition": 0},
                {"ABC": 2, "partition": 1},
            ],
            [
                call({}, {"partition": "0"}, None),
                call({}, {"partition": "0"}, {"next_page_token": "next"}),
                call({}, {"partition": "1"}, None),
            ],
        ),
    ],
)
def test_read_concurrent_declarative_source(
    test_name, manifest, pages, expected_records, expected_calls
):
    _stream_name = "Rates"
    with patch.object(SimpleRetriever, "_fetch_next_page", side_effect=pages) as mock_retriever:
        output_data = [
            message.record.data for message in _run_read(manifest, _stream_name) if message.record
        ]
        assert output_data == expected_records
        mock_retriever.assert_has_calls(expected_calls)


def test_only_parent_streams_use_cache():
    applications_stream = {
        "type": "DeclarativeStream",
        "$parameters": {
            "name": "applications",
            "primary_key": "id",
            "url_base": "https://harvest.greenhouse.io/v1/",
        },
        "schema_loader": {
            "name": "{{ parameters.stream_name }}",
            "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
        },
        "retriever": {
            "paginator": {
                "type": "DefaultPaginator",
                "page_size": 10,
                "page_size_option": {
                    "type": "RequestOption",
                    "inject_into": "request_parameter",
                    "field_name": "per_page",
                },
                "page_token_option": {"type": "RequestPath"},
                "pagination_strategy": {
                    "type": "CursorPagination",
                    "cursor_value": "{{ headers['link']['next']['url'] }}",
                    "stop_condition": "{{ 'next' not in headers['link'] }}",
                    "page_size": 100,
                },
            },
            "requester": {
                "path": "applications",
                "authenticator": {
                    "type": "BasicHttpAuthenticator",
                    "username": "{{ config['api_key'] }}",
                },
            },
            "record_selector": {"extractor": {"type": "DpathExtractor", "field_path": []}},
        },
    }

    manifest = {
        "version": "0.29.3",
        "definitions": {},
        "streams": [
            deepcopy(applications_stream),
            {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": "applications_interviews",
                    "primary_key": "id",
                    "url_base": "https://harvest.greenhouse.io/v1/",
                },
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "per_page",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ headers['link']['next']['url'] }}",
                            "stop_condition": "{{ 'next' not in headers['link'] }}",
                            "page_size": 100,
                        },
                    },
                    "requester": {
                        "path": "applications_interviews",
                        "authenticator": {
                            "type": "BasicHttpAuthenticator",
                            "username": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {"extractor": {"type": "DpathExtractor", "field_path": []}},
                    "partition_router": {
                        "parent_stream_configs": [
                            {
                                "parent_key": "id",
                                "partition_field": "parent_id",
                                "stream": deepcopy(applications_stream),
                            }
                        ],
                        "type": "SubstreamPartitionRouter",
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": "jobs",
                    "primary_key": "id",
                    "url_base": "https://harvest.greenhouse.io/v1/",
                },
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "per_page",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ headers['link']['next']['url'] }}",
                            "stop_condition": "{{ 'next' not in headers['link'] }}",
                            "page_size": 100,
                        },
                    },
                    "requester": {
                        "path": "jobs",
                        "authenticator": {
                            "type": "BasicHttpAuthenticator",
                            "username": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {"extractor": {"type": "DpathExtractor", "field_path": []}},
                },
            },
        ],
        "check": {"type": "CheckStream", "stream_names": ["applications"]},
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
    )

    streams = source.streams({})
    assert len(streams) == 3

    # Main stream with caching (parent for substream `applications_interviews`)
    stream_0 = streams[0]
    assert stream_0.name == "applications"
    assert isinstance(stream_0, DefaultStream)
    assert stream_0._stream_partition_generator._partition_factory._retriever.requester.use_cache

    # Substream
    stream_1 = streams[1]
    assert stream_1.name == "applications_interviews"
    assert isinstance(stream_1, DefaultStream)
    assert (
        not stream_1._stream_partition_generator._partition_factory._retriever.requester.use_cache
    )

    # Parent stream created for substream
    assert (
        stream_1._stream_partition_generator._stream_slicer.parent_stream_configs[0].stream.name
        == "applications"
    )
    assert stream_1._stream_partition_generator._stream_slicer.parent_stream_configs[
        0
    ].stream._stream_partition_generator._partition_factory._retriever.requester.use_cache

    # Main stream without caching
    stream_2 = streams[2]
    assert stream_2.name == "jobs"
    assert isinstance(stream_2, DefaultStream)
    assert (
        not stream_2._stream_partition_generator._partition_factory._retriever.requester.use_cache
    )


def _run_read(manifest: Mapping[str, Any], stream_name: str) -> List[AirbyteMessage]:
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config={}, catalog=create_catalog("lists"), state=None
    )
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=stream_name, json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            )
        ]
    )
    return list(source.read(logger, {}, catalog, {}))


def test_declarative_component_schema_valid_ref_links():
    def load_yaml(file_path) -> Mapping[str, Any]:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)

    def extract_refs(data, base_path="#") -> List[str]:
        refs = []
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "$ref" and isinstance(value, str) and value.startswith("#"):
                    ref_path = value
                    refs.append(ref_path)
                else:
                    refs.extend(extract_refs(value, base_path))
        elif isinstance(data, list):
            for item in data:
                refs.extend(extract_refs(item, base_path))
        return refs

    def resolve_pointer(data: Mapping[str, Any], pointer: str) -> bool:
        parts = pointer.split("/")[1:]  # Skip the first empty part due to leading '#/'
        current = data
        try:
            for part in parts:
                part = part.replace("~1", "/").replace("~0", "~")  # Unescape JSON Pointer
                current = current[part]
            return True
        except (KeyError, TypeError):
            return False

    def validate_refs(yaml_file: str) -> List[str]:
        data = load_yaml(yaml_file)
        refs = extract_refs(data)
        invalid_refs = [ref for ref in refs if not resolve_pointer(data, ref.replace("#", ""))]
        return invalid_refs

    yaml_file_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "airbyte_cdk/sources/declarative/declarative_component_schema.yaml"
    )
    assert not validate_refs(yaml_file_path)


@pytest.mark.parametrize(
    "test_name, manifest, pages, expected_states_qty",
    [
        (
            "test_with_pagination_and_partition_router",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                    "partition": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "partition_router": {
                                "type": "ListPartitionRouter",
                                "values": ["0", "1"],
                                "cursor_field": "partition",
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {
                                "type": "DefaultPaginator",
                                "page_size": 2,
                                "page_size_option": {
                                    "inject_into": "request_parameter",
                                    "field_name": "page_size",
                                },
                                "page_token_option": {"inject_into": "path", "type": "RequestPath"},
                                "pagination_strategy": {
                                    "type": "CursorPagination",
                                    "cursor_value": "{{ response._metadata.next }}",
                                    "page_size": 2,
                                },
                            },
                        },
                        "incremental_sync": {
                            "type": "DatetimeBasedCursor",
                            "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%S.%fZ"],
                            "datetime_format": "%Y-%m-%dT%H:%M:%S.%fZ",
                            "cursor_field": "updated_at",
                            "start_datetime": {
                                "datetime": "{{ config.get('start_date', '2020-10-16T00:00:00.000Z') }}"
                            },
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            },
                            "start_date": {
                                "title": "Start Date",
                                "description": "UTC date and time in the format YYYY-MM-DDTHH:MM:SS.000Z. During incremental sync, any data generated before this date will not be replicated. If left blank, the start date will be set to 2 years before the present date.",
                                "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                                "pattern_descriptor": "YYYY-MM-DDTHH:MM:SS.000Z",
                                "examples": ["2020-11-16T00:00:00.000Z"],
                                "type": "string",
                                "format": "date-time",
                            },
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page(
                    {
                        "rates": [
                            {"ABC": 0, "partition": 0, "updated_at": "2020-11-16T00:00:00.000Z"},
                            {"AED": 1, "partition": 0, "updated_at": "2020-11-16T00:00:00.000Z"},
                        ],
                        "_metadata": {"next": "next"},
                    }
                ),
                _create_page(
                    {
                        "rates": [
                            {"USD": 3, "partition": 0, "updated_at": "2020-11-16T00:00:00.000Z"}
                        ],
                        "_metadata": {},
                    }
                ),
                _create_page(
                    {
                        "rates": [
                            {"ABC": 2, "partition": 1, "updated_at": "2020-11-16T00:00:00.000Z"}
                        ],
                        "_metadata": {},
                    }
                ),
            ),
            2,
        ),
    ],
)
def test_slice_checkpoint(test_name, manifest, pages, expected_states_qty):
    _stream_name = "Rates"
    with patch.object(SimpleRetriever, "_fetch_next_page", side_effect=pages):
        states = [message.state for message in _run_read(manifest, _stream_name) if message.state]
        assert len(states) == expected_states_qty


@pytest.fixture
def migration_mocks(monkeypatch):
    mock_message_repository = Mock()
    mock_message_repository.consume_queue.return_value = [Mock()]

    _mock_open = mock_open()
    mock_json_dump = Mock()
    mock_print = Mock()
    mock_serializer_dump = Mock()

    mock_decoded_bytes = Mock()
    mock_decoded_bytes.decode.return_value = "decoded_message"
    mock_orjson_dumps = Mock(return_value=mock_decoded_bytes)

    monkeypatch.setattr("builtins.open", _mock_open)
    monkeypatch.setattr("json.dump", mock_json_dump)
    monkeypatch.setattr("builtins.print", mock_print)
    monkeypatch.setattr(
        "airbyte_cdk.models.airbyte_protocol_serializers.AirbyteMessageSerializer.dump",
        mock_serializer_dump,
    )
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.concurrent_declarative_source.orjson.dumps",
        mock_orjson_dumps,
    )

    return {
        "message_repository": mock_message_repository,
        "open": _mock_open,
        "json_dump": mock_json_dump,
        "print": mock_print,
        "serializer_dump": mock_serializer_dump,
        "orjson_dumps": mock_orjson_dumps,
        "decoded_bytes": mock_decoded_bytes,
    }


def test_given_unmigrated_config_when_migrating_then_config_is_migrated(migration_mocks) -> None:
    input_config = {"planet": "CRSC"}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "config_migrations": [
                    {
                        "type": "ConfigMigration",
                        "description": "Test migration",
                        "transformations": [
                            {
                                "type": "ConfigRemapField",
                                "map": {"CRSC": "Coruscant"},
                                "field_path": ["planet"],
                            }
                        ],
                    }
                ],
            },
        },
    }

    ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        config_path="/fake/config/path",
        catalog=create_catalog("lists"),
        state=None,
    )

    migration_mocks["open"].assert_called_once_with("/fake/config/path", "w")
    migration_mocks["json_dump"].assert_called_once()
    migration_mocks["print"].assert_called()
    migration_mocks["serializer_dump"].assert_called()
    migration_mocks["orjson_dumps"].assert_called()
    migration_mocks["decoded_bytes"].decode.assert_called()


def test_given_already_migrated_config_no_control_message_is_emitted(migration_mocks) -> None:
    input_config = {"planet": "Coruscant"}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "config_migrations": [
                    {
                        "type": "ConfigMigration",
                        "description": "Test migration",
                        "transformations": [
                            {
                                "type": "ConfigRemapField",
                                "map": {"CRSC": "Coruscant"},
                                "field_path": ["planet"],
                            }
                        ],
                    }
                ],
            },
        },
    }

    ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        config_path="/fake/config/path",
        catalog=create_catalog("lists"),
        state=None,
    )

    migration_mocks["open"].assert_not_called()
    migration_mocks["json_dump"].assert_not_called()
    migration_mocks["print"].assert_not_called()
    migration_mocks["serializer_dump"].assert_not_called()
    migration_mocks["orjson_dumps"].assert_not_called()
    migration_mocks["decoded_bytes"].decode.assert_not_called()


def test_given_transformations_config_is_transformed():
    input_config = {"planet": "CRSC"}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "transformations": [
                    {
                        "type": "ConfigAddFields",
                        "fields": [
                            {
                                "type": "AddedFieldDefinition",
                                "path": ["population"],
                                "value": "{{ config['planet'] }}",
                            }
                        ],
                    },
                    {
                        "type": "ConfigRemapField",
                        "map": {"CRSC": "Coruscant"},
                        "field_path": ["planet"],
                    },
                    {
                        "type": "ConfigRemapField",
                        "map": {"CRSC": 3_000_000_000_000},
                        "field_path": ["population"],
                    },
                ],
            },
        },
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        catalog=create_catalog("lists"),
        state=None,
    )

    source.write_config = Mock(return_value=None)

    config = source.configure(input_config, "/fake/temp/dir")

    assert config != input_config
    assert config == {"planet": "Coruscant", "population": 3_000_000_000_000}


def test_given_valid_config_streams_validates_config_and_does_not_raise():
    input_config = {"schema_to_validate": {"planet": "Coruscant"}}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "parameters": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "validations": [
                    {
                        "type": "DpathValidator",
                        "field_path": ["schema_to_validate"],
                        "validation_strategy": {
                            "type": "ValidateAdheresToSchema",
                            "base_schema": {
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "title": "Test Spec",
                                "type": "object",
                                "properties": {"planet": {"type": "string"}},
                                "required": ["planet"],
                                "additionalProperties": False,
                            },
                        },
                    }
                ],
            },
        },
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        catalog=create_catalog("lists"),
        state=None,
    )

    source.streams(input_config)


def test_given_invalid_config_streams_validates_config_and_raises():
    input_config = {"schema_to_validate": {"will_fail": "Coruscant"}}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "parameters": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "validations": [
                    {
                        "type": "DpathValidator",
                        "field_path": ["schema_to_validate"],
                        "validation_strategy": {
                            "type": "ValidateAdheresToSchema",
                            "base_schema": {
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "title": "Test Spec",
                                "type": "object",
                                "properties": {"planet": {"type": "string"}},
                                "required": ["planet"],
                                "additionalProperties": False,
                            },
                        },
                    }
                ],
            },
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        catalog=create_catalog("lists"),
        state=None,
    )

    with pytest.raises(ValueError):
        source.streams(input_config)


def test_parameter_propagation_for_concurrent_cursor():
    cursor_field_parameter_override = "created_at"
    manifest = {
        "version": "5.0.0",
        "definitions": {
            "selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://persona.metaverse.com",
                "http_method": "GET",
            },
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "paginator": {"type": "NoPagination"},
                "requester": {"$ref": "#/definitions/requester"},
            },
            "incremental_cursor": {
                "type": "DatetimeBasedCursor",
                "start_datetime": {"datetime": "2024-01-01"},
                "end_datetime": "2024-12-31",
                "datetime_format": "%Y-%m-%d",
                "cursor_datetime_formats": ["%Y-%m-%d"],
                "cursor_granularity": "P1D",
                "step": "P400D",
                "cursor_field": "{{ parameters.get('cursor_field',  'updated_at') }}",
                "start_time_option": {
                    "type": "RequestOption",
                    "field_name": "start",
                    "inject_into": "request_parameter",
                },
                "end_time_option": {
                    "type": "RequestOption",
                    "field_name": "end",
                    "inject_into": "request_parameter",
                },
            },
            "base_stream": {"retriever": {"$ref": "#/definitions/retriever"}},
            "incremental_stream": {
                "retriever": {
                    "$ref": "#/definitions/retriever",
                    "requester": {"$ref": "#/definitions/requester"},
                },
                "incremental_sync": {"$ref": "#/definitions/incremental_cursor"},
                "$parameters": {
                    "name": "stream_name",
                    "primary_key": "id",
                    "path": "/path",
                    "cursor_field": cursor_field_parameter_override,
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "https://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "properties": {
                            "id": {
                                "description": "The identifier",
                                "type": ["null", "string"],
                            },
                        },
                    },
                },
            },
        },
        "streams": [
            "#/definitions/incremental_stream",
        ],
        "check": {"stream_names": ["stream_name"]},
        "concurrency_level": {
            "type": "ConcurrencyLevel",
            "default_concurrency": "{{ config['num_workers'] or 10 }}",
            "max_concurrency": 25,
        },
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config={},
        catalog=create_catalog("stream_name"),
        state=None,
    )
    streams = source.streams({})

    assert streams[0].cursor.cursor_field.cursor_field_key == cursor_field_parameter_override


def test_given_response_action_is_pagination_reset_when_read_then_reset_pagination():
    input_config = {}
    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                        "error_handler": {
                            "type": "DefaultErrorHandler",
                            "response_filters": [
                                {
                                    "http_codes": [400],
                                    "action": "RESET_PAGINATION",
                                    "failure_type": "system_error",
                                },
                            ],
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
        },
    }

    catalog = create_catalog("Test")
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        catalog=catalog,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest("https://example.org/test"),
            [
                HttpResponse("", 400),
                HttpResponse(json.dumps([{"id": 1}]), 200),
            ],
        )
        messages = list(
            source.read(logger=source.logger, config=input_config, catalog=catalog, state=[])
        )

    assert len(list(filter(lambda message: message.type == Type.RECORD, messages)))


def test_given_pagination_limit_reached_when_read_then_reset_pagination():
    input_config = {}
    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test?from={{ stream_interval.start_time }}",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "pagination_reset": {
                        "type": "PaginationReset",
                        "action": "SPLIT_USING_CURSOR",
                        "limits": {
                            "type": "PaginationResetLimits",
                            "number_of_records": 2,
                        },
                    },
                },
                "incremental_sync": {
                    "type": "DatetimeBasedCursor",
                    "start_datetime": {"datetime": "2022-01-01"},
                    "end_datetime": "2023-12-31",
                    "datetime_format": "%Y-%m-%d",
                    "cursor_datetime_formats": ["%Y-%m-%d"],
                    "cursor_granularity": "P1D",
                    "step": "P1Y",
                    "cursor_field": "updated_at",
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
        },
    }

    catalog = create_catalog("Test")
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        catalog=catalog,
        state=None,
    )

    with HttpMocker() as http_mocker:
        # Slice from 2022-01-01 to 2022-12-31
        http_mocker.get(
            HttpRequest("https://example.org/test?from=2022-01-01"),
            HttpResponse(
                json.dumps(
                    [{"id": 1, "updated_at": "2022-02-01"}, {"id": 2, "updated_at": "2022-03-01"}]
                ),
                200,
            ),
        )
        http_mocker.get(
            HttpRequest("https://example.org/test?from=2022-03-01"),
            HttpResponse(json.dumps([{"id": 3, "updated_at": "2022-04-01"}]), 200),
        )
        # Slice from 2023-01-01 to 2023-12-31
        http_mocker.get(
            HttpRequest("https://example.org/test?from=2023-01-01"),
            HttpResponse(json.dumps([{"id": 4, "updated_at": "2023-04-01"}]), 200),
        )
        messages = list(
            source.read(logger=source.logger, config=input_config, catalog=catalog, state=[])
        )

    assert len(list(filter(lambda message: message.type == Type.RECORD, messages))) == 4


def test_given_per_partition_cursor_when_read_then_reset_pagination():
    input_config = {}
    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test?partition={{ stream_partition.parent_id }}&from={{ stream_interval.start_time }}",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "pagination_reset": {
                        "type": "PaginationReset",
                        "action": "SPLIT_USING_CURSOR",
                        "limits": {
                            "type": "PaginationResetLimits",
                            "number_of_records": 2,
                        },
                    },
                    "partition_router": {
                        "type": "ListPartitionRouter",
                        "cursor_field": "parent_id",
                        "values": ["1", "2"],
                    },
                },
                "incremental_sync": {
                    "type": "DatetimeBasedCursor",
                    "start_datetime": {"datetime": "2022-01-01"},
                    "end_datetime": "2022-12-31",
                    "datetime_format": "%Y-%m-%d",
                    "cursor_datetime_formats": ["%Y-%m-%d"],
                    "cursor_granularity": "P1D",
                    "step": "P1Y",
                    "cursor_field": "updated_at",
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
        },
    }

    catalog = create_catalog("Test")
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        catalog=catalog,
        state=None,
    )

    with HttpMocker() as http_mocker:
        # Partition 1
        http_mocker.get(
            HttpRequest("https://example.org/test?partition=1&from=2022-01-01"),
            HttpResponse(
                json.dumps(
                    [{"id": 1, "updated_at": "2022-02-01"}, {"id": 2, "updated_at": "2022-03-01"}]
                ),
                200,
            ),
        )
        http_mocker.get(
            HttpRequest("https://example.org/test?partition=1&from=2022-03-01"),
            HttpResponse(json.dumps([{"id": 3, "updated_at": "2022-04-01"}]), 200),
        )
        # Partition 2
        http_mocker.get(
            HttpRequest("https://example.org/test?partition=2&from=2022-01-01"),
            HttpResponse(json.dumps([{"id": 4, "updated_at": "2023-04-01"}]), 200),
        )
        messages = list(
            source.read(logger=source.logger, config=input_config, catalog=catalog, state=[])
        )

    assert len(list(filter(lambda message: message.type == Type.RECORD, messages))) == 4


def test_given_record_selector_is_filtering_when_read_then_raise_error():
    """
    This test is here to show the limitations of pagination reset. If it starts failing, maybe we just want to delete
    it. Basically, since the filtering happens before we count the number of entries, than we might not have an
    accurate picture of the number of records passed through the HTTP response.
    """
    input_config = {}
    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test?from={{ stream_interval.start_time }}",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                        "record_filter": {
                            "type": "RecordFilter",
                            "condition": "{{ record['id'] != 1 }}",
                        },
                    },
                    "pagination_reset": {
                        "type": "PaginationReset",
                        "action": "SPLIT_USING_CURSOR",
                        "limits": {
                            "type": "PaginationResetLimits",
                            "number_of_records": 2,
                        },
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
        },
    }

    catalog = create_catalog("Test")
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=input_config,
        catalog=catalog,
        state=None,
    )

    with pytest.raises(ValueError):
        list(source.read(logger=source.logger, config=input_config, catalog=catalog, state=[]))
