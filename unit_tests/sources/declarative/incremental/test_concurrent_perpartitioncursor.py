# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
import copy
import re
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, List, Mapping, MutableMapping, Optional, Union
from unittest.mock import MagicMock, patch
from urllib.parse import unquote

import pytest
from orjson import orjson

from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.incremental import ConcurrentPerPartitionCursor
from airbyte_cdk.sources.declarative.partition_routers import ListPartitionRouter
from airbyte_cdk.sources.declarative.schema import InlineSchemaLoader
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    DeclarativePartition,
    RecordCounter,
)
from airbyte_cdk.sources.streams.concurrent.cursor import CursorField
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import (
    CustomFormatConcurrentStreamStateConverter,
)
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read

_EMPTY_SCHEMA_LOADER = InlineSchemaLoader(schema={}, parameters={})

SUBSTREAM_MANIFEST: MutableMapping[str, Any] = {
    "version": "0.51.42",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["post_comment_votes"]},
    "definitions": {
        "basic_authenticator": {
            "type": "BasicHttpAuthenticator",
            "username": "{{ config['credentials']['email'] + '/token' }}",
            "password": "{{ config['credentials']['api_token'] }}",
        },
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://api.example.com",
                "http_method": "GET",
                "authenticator": "#/definitions/basic_authenticator",
            },
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {
                    "type": "DpathExtractor",
                    "field_path": ["{{ parameters.get('data_path') or parameters['name'] }}"],
                },
                "schema_normalization": "Default",
            },
            "paginator": {
                "type": "DefaultPaginator",
                "page_size_option": {
                    "type": "RequestOption",
                    "field_name": "per_page",
                    "inject_into": "request_parameter",
                },
                "pagination_strategy": {
                    "type": "CursorPagination",
                    "page_size": 100,
                    "cursor_value": "{{ response.get('next_page', {}) }}",
                    "stop_condition": "{{ not response.get('next_page', {}) }}",
                },
                "page_token_option": {"type": "RequestPath"},
            },
        },
        "cursor_incremental_sync": {
            "type": "DatetimeBasedCursor",
            "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%ms"],
            "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
            "cursor_field": "{{ parameters.get('cursor_field',  'updated_at') }}",
            "start_datetime": {"datetime": "{{ config.get('start_date')}}"},
            "start_time_option": {
                "inject_into": "request_parameter",
                "field_name": "start_time",
                "type": "RequestOption",
            },
        },
        "posts_stream": {
            "type": "DeclarativeStream",
            "name": "posts",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "updated_at": {"type": "string", "format": "date-time"},
                        "title": {"type": "string"},
                        "content": {"type": "string"},
                    },
                    "type": "object",
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/community/posts",
                    "http_method": "GET",
                    "authenticator": "#/definitions/basic_authenticator",
                },
                "record_selector": "#/definitions/retriever/record_selector",
                "paginator": "#/definitions/retriever/paginator",
            },
            "incremental_sync": "#/definitions/cursor_incremental_sync",
            "$parameters": {
                "name": "posts",
                "path": "community/posts",
                "data_path": "posts",
                "cursor_field": "updated_at",
                "primary_key": "id",
            },
        },
        "post_comments_stream": {
            "type": "DeclarativeStream",
            "name": "post_comments",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "updated_at": {"type": "string", "format": "date-time"},
                        "post_id": {"type": "integer"},
                        "comment": {"type": "string"},
                    },
                    "type": "object",
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/community/posts/{{ stream_slice.id }}/comments",
                    "http_method": "GET",
                    "authenticator": "#/definitions/basic_authenticator",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": ["comments"]},
                    "record_filter": {
                        "condition": "{{ record['updated_at'] >= stream_interval.extra_fields.get('updated_at', config.get('start_date')) }}"
                    },
                },
                "paginator": "#/definitions/retriever/paginator",
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "stream": "#/definitions/posts_stream",
                            "parent_key": "id",
                            "partition_field": "id",
                            "incremental_dependency": True,
                        }
                    ],
                },
            },
            "incremental_sync": {
                "type": "DatetimeBasedCursor",
                "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"],
                "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                "cursor_field": "{{ parameters.get('cursor_field',  'updated_at') }}",
                "start_datetime": {"datetime": "{{ config.get('start_date') }}"},
            },
            "$parameters": {
                "name": "post_comments",
                "path": "community/posts/{{ stream_slice.id }}/comments",
                "data_path": "comments",
                "cursor_field": "updated_at",
                "primary_key": "id",
            },
        },
        "post_comment_votes_stream": {
            "type": "DeclarativeStream",
            "name": "post_comment_votes",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "comment_id": {"type": "integer"},
                        "vote": {"type": "number"},
                    },
                    "type": "object",
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/community/posts/{{ stream_slice.parent_slice.id }}/comments/{{ stream_slice.id }}/votes",
                    "http_method": "GET",
                    "authenticator": "#/definitions/basic_authenticator",
                },
                "record_selector": "#/definitions/retriever/record_selector",
                "paginator": "#/definitions/retriever/paginator",
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "stream": "#/definitions/post_comments_stream",
                            "parent_key": "id",
                            "partition_field": "id",
                            "incremental_dependency": True,
                            "extra_fields": [["updated_at"]],
                        }
                    ],
                },
            },
            "transformations": [
                {
                    "type": "AddFields",
                    "fields": [
                        {
                            "path": ["comment_updated_at"],
                            "value_type": "string",
                            "value": "{{ stream_slice.extra_fields['updated_at'] }}",
                        },
                    ],
                },
            ],
            "incremental_sync": "#/definitions/cursor_incremental_sync",
            "$parameters": {
                "name": "post_comment_votes",
                "path": "community/posts/{{ stream_slice.parent_slice.id }}/comments/{{ stream_slice.id }}/votes",
                "data_path": "votes",
                "cursor_field": "created_at",
                "primary_key": "id",
            },
        },
    },
    "streams": [
        {"$ref": "#/definitions/posts_stream"},
        {"$ref": "#/definitions/post_comments_stream"},
        {"$ref": "#/definitions/post_comment_votes_stream"},
    ],
    "concurrency_level": {
        "type": "ConcurrencyLevel",
        "default_concurrency": "{{ config['num_workers'] or 10 }}",
        "max_concurrency": 25,
    },
    "spec": {
        "type": "Spec",
        "documentation_url": "https://airbyte.com/#yaml-from-manifest",
        "connection_specification": {
            "title": "Test Spec",
            "type": "object",
            "required": ["credentials", "start_date"],
            "additionalProperties": False,
            "properties": {
                "credentials": {
                    "type": "object",
                    "required": ["email", "api_token"],
                    "properties": {
                        "email": {
                            "type": "string",
                            "title": "Email",
                            "description": "The email for authentication.",
                        },
                        "api_token": {
                            "type": "string",
                            "airbyte_secret": True,
                            "title": "API Token",
                            "description": "The API token for authentication.",
                        },
                    },
                },
                "start_date": {
                    "type": "string",
                    "format": "date-time",
                    "title": "Start Date",
                    "description": "The date from which to start syncing data.",
                },
            },
        },
    },
}

STREAM_NAME = "post_comment_votes"

SUBSTREAM_MANIFEST_NO_DEPENDENCY = deepcopy(SUBSTREAM_MANIFEST)
# Disable incremental_dependency
SUBSTREAM_MANIFEST_NO_DEPENDENCY["definitions"]["post_comments_stream"]["retriever"][
    "partition_router"
]["parent_stream_configs"][0]["incremental_dependency"] = False
SUBSTREAM_MANIFEST_NO_DEPENDENCY["definitions"]["post_comment_votes_stream"]["retriever"][
    "partition_router"
]["parent_stream_configs"][0]["incremental_dependency"] = False

SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY = deepcopy(SUBSTREAM_MANIFEST)
# Disable incremental_dependency
SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY["definitions"]["post_comments_stream"][
    "retriever"
]["partition_router"]["parent_stream_configs"][0]["incremental_dependency"] = False
SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY["definitions"]["post_comment_votes_stream"][
    "retriever"
]["partition_router"]["parent_stream_configs"][0]["incremental_dependency"] = False
# Enable global_cursor
SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY["definitions"]["cursor_incremental_sync"][
    "global_substream_cursor"
] = True


import orjson
import requests_mock


def run_mocked_test(
    mock_requests,
    manifest,
    config,
    stream_name,
    initial_state,
    expected_records,
    expected_state,
    state_count=None,
):
    """
    Helper function to mock requests, run the test, and verify the results.

    Args:
        mock_requests (list): List of tuples containing the URL and response data to mock.
        manifest (dict): Manifest configuration for the source.
        config (dict): Source configuration.
        stream_name (str): Name of the stream being tested.
        initial_state (dict): Initial state for the stream.
        expected_records (list): Expected records to be returned by the stream.
        expected_state (dict): Expected state after processing the records.

    Raises:
        AssertionError: If the test output does not match the expected records or state.
    """
    with requests_mock.Mocker() as m:
        for url, response in mock_requests:
            if response is None:
                m.get(url, status_code=404)
            else:
                m.get(url, json=response)

        initial_state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name=stream_name, namespace=None),
                    stream_state=AirbyteStateBlob(initial_state),
                ),
            )
        ]
        output = _run_read(manifest, config, stream_name, initial_state)

        # Verify records
        assert sorted([r.record.data for r in output.records], key=lambda x: x["id"]) == sorted(
            expected_records, key=lambda x: x["id"]
        )

        assert len(output.state_messages) == state_count if state_count else True

        # Verify state
        final_state = output.state_messages[-1].state.stream.stream_state
        assert final_state.__dict__ == expected_state

        # Verify that each request was made exactly once
        for url, _ in mock_requests:
            request_count = len(
                [req for req in m.request_history if unquote(req.url) == unquote(url)]
            )
            assert request_count == 1, (
                f"URL {url} was called {request_count} times, expected exactly once."
            )


def _run_read(
    manifest: Mapping[str, Any],
    config: Mapping[str, Any],
    stream_name: str,
    state: Optional[Union[List[AirbyteStateMessage], MutableMapping[str, Any]]] = None,
) -> EntrypointOutput:
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=config, catalog=None, state=state
    )
    output = read(
        source,
        config,
        CatalogBuilder()
        .with_stream(ConfiguredAirbyteStreamBuilder().with_name(stream_name))
        .build(),
    )
    return output


# Existing Constants for Dates
START_DATE = "2024-01-01T00:00:01Z"  # Start of the sync
POST_1_UPDATED_AT = "2024-01-30T00:00:00Z"  # Latest update date for post 1
POST_2_UPDATED_AT = "2024-01-29T00:00:00Z"  # Latest update date for post 2
POST_3_UPDATED_AT = "2024-01-28T00:00:00Z"  # Latest update date for post 3

COMMENT_9_OLDEST = "2023-01-01T00:00:00Z"  # Comment in partition 1 - filtered out due to date
COMMENT_10_UPDATED_AT = "2024-01-25T00:00:00Z"  # Latest comment in partition 1
COMMENT_11_UPDATED_AT = "2024-01-24T00:00:00Z"  # Comment in partition 1
COMMENT_12_UPDATED_AT = "2024-01-23T00:00:00Z"  # Comment in partition 1
COMMENT_20_UPDATED_AT = "2024-01-22T00:00:00Z"  # Latest comment in partition 2
COMMENT_21_UPDATED_AT = "2024-01-21T00:00:00Z"  # Comment in partition 2
COMMENT_30_UPDATED_AT = "2024-01-09T00:00:00Z"  # Latest comment in partition 3
LOOKBACK_WINDOW_DAYS = 1  # Lookback window duration in days

# Votes Date Constants
VOTE_100_CREATED_AT = "2024-01-15T00:00:00Z"  # Latest vote in partition 10
VOTE_101_CREATED_AT = "2024-01-14T00:00:00Z"  # Second-latest vote in partition 10
VOTE_111_CREATED_AT = "2024-01-13T00:00:00Z"  # Latest vote in partition 11
VOTE_200_CREATED_AT = "2024-01-12T00:00:00Z"  # Latest vote in partition 20
VOTE_210_CREATED_AT = "2024-01-12T00:00:15Z"  # Latest vote in partition 21
VOTE_300_CREATED_AT = "2024-01-10T00:00:00Z"  # Latest vote in partition 30
VOTE_300_CREATED_AT_TIMESTAMP = 1704844800000  # Latest vote in partition 30

# Initial State Constants
PARENT_COMMENT_CURSOR_PARTITION_1 = "2023-01-04T00:00:00Z"  # Parent comment cursor (partition)
PARENT_POSTS_CURSOR = "2024-01-05T00:00:00Z"  # Parent posts cursor (expected in state)

INITIAL_STATE_PARTITION_10_CURSOR = "2024-01-02T00:00:01Z"
INITIAL_STATE_PARTITION_10_CURSOR_TIMESTAMP = 1704153601000
INITIAL_STATE_PARTITION_11_CURSOR = "2024-01-03T00:00:02Z"
INITIAL_STATE_PARTITION_11_CURSOR_TIMESTAMP = 1704240002000
INITIAL_GLOBAL_CURSOR = INITIAL_STATE_PARTITION_11_CURSOR
INITIAL_GLOBAL_CURSOR_DATE = datetime.fromisoformat(
    INITIAL_STATE_PARTITION_11_CURSOR.replace("Z", "")
)
LOOKBACK_DATE = (
    INITIAL_GLOBAL_CURSOR_DATE - timedelta(days=LOOKBACK_WINDOW_DAYS)
).isoformat() + "Z"

PARTITION_SYNC_START_TIME = "2024-01-02T00:00:00Z"
CONFIG = {
    "start_date": START_DATE,
    "credentials": {"email": "email", "api_token": "api_token"},
}


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state, state_count",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST_NO_DEPENDENCY,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                            {"id": 2, "updated_at": POST_2_UPDATED_AT},
                        ],
                        "next_page": f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}&page=2",
                    {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 9,
                                "post_id": 1,
                                "updated_at": COMMENT_9_OLDEST,  # No requests for comment 9, filtered out due to the date
                            },
                            {
                                "id": 10,
                                "post_id": 1,
                                "updated_at": COMMENT_10_UPDATED_AT,
                            },
                            {
                                "id": 11,
                                "post_id": 1,
                                "updated_at": COMMENT_11_UPDATED_AT,
                            },
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {
                        "comments": [
                            {
                                "id": 12,
                                "post_id": 1,
                                "updated_at": COMMENT_12_UPDATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 100,
                                "comment_id": 10,
                                "created_at": VOTE_100_CREATED_AT,
                            }
                        ],
                        "next_page": f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 101,
                                "comment_id": 10,
                                "created_at": VOTE_101_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 111,
                                "comment_id": 11,
                                "created_at": VOTE_111_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 20,
                                "post_id": 2,
                                "updated_at": COMMENT_20_UPDATED_AT,
                            }
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {
                        "comments": [
                            {
                                "id": 21,
                                "post_id": 2,
                                "updated_at": COMMENT_21_UPDATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 200,
                                "comment_id": 20,
                                "created_at": VOTE_200_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 210,
                                "comment_id": 21,
                                "created_at": VOTE_210_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 30,
                                "post_id": 3,
                                "updated_at": COMMENT_30_UPDATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts/3/comments/30/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 300,
                                "comment_id": 30,
                                "created_at": VOTE_300_CREATED_AT_TIMESTAMP,
                            }
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_100_CREATED_AT,
                    "id": 100,
                },
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_101_CREATED_AT,
                    "id": 101,
                },
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "created_at": VOTE_111_CREATED_AT,
                    "id": 111,
                },
                {
                    "comment_id": 20,
                    "comment_updated_at": COMMENT_20_UPDATED_AT,
                    "created_at": VOTE_200_CREATED_AT,
                    "id": 200,
                },
                {
                    "comment_id": 21,
                    "comment_updated_at": COMMENT_21_UPDATED_AT,
                    "created_at": VOTE_210_CREATED_AT,
                    "id": 210,
                },
                {
                    "comment_id": 30,
                    "comment_updated_at": COMMENT_30_UPDATED_AT,
                    "created_at": str(VOTE_300_CREATED_AT_TIMESTAMP),
                    "id": 300,
                },
            ],
            # Initial state
            {
                # This should not happen since parent state is disabled, but I've added this to validate that and
                # incoming parent_state is ignored when the parent stream's incremental_dependency is disabled
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR_TIMESTAMP},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "state": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR_TIMESTAMP},
                "lookback_window": 86400,
            },
            # Expected state
            {
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": VOTE_100_CREATED_AT},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": VOTE_111_CREATED_AT},
                    },
                    {
                        "partition": {
                            "id": 12,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": LOOKBACK_DATE},
                    },
                    {
                        "partition": {
                            "id": 20,
                            "parent_slice": {"id": 2, "parent_slice": {}},
                        },
                        "cursor": {"created_at": VOTE_200_CREATED_AT},
                    },
                    {
                        "partition": {
                            "id": 21,
                            "parent_slice": {"id": 2, "parent_slice": {}},
                        },
                        "cursor": {"created_at": VOTE_210_CREATED_AT},
                    },
                    {
                        "partition": {
                            "id": 30,
                            "parent_slice": {"id": 3, "parent_slice": {}},
                        },
                        "cursor": {"created_at": VOTE_300_CREATED_AT},
                    },
                ],
                "use_global_cursor": False,
                "lookback_window": 1,
                "parent_state": {},
                "state": {"created_at": VOTE_100_CREATED_AT},
            },
            # State count
            2,
        ),
        (
            "test_incremental_parent_state_with",
            SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                            {"id": 2, "updated_at": POST_2_UPDATED_AT},
                        ],
                        "next_page": f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}&page=2",
                    {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 9,
                                "post_id": 1,
                                "updated_at": COMMENT_9_OLDEST,  # No requests for comment 9, filtered out due to the date
                            },
                            {
                                "id": 10,
                                "post_id": 1,
                                "updated_at": COMMENT_10_UPDATED_AT,
                            },
                            {
                                "id": 11,
                                "post_id": 1,
                                "updated_at": COMMENT_11_UPDATED_AT,
                            },
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {
                        "comments": [
                            {
                                "id": 12,
                                "post_id": 1,
                                "updated_at": COMMENT_12_UPDATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 100,
                                "comment_id": 10,
                                "created_at": VOTE_100_CREATED_AT,
                            }
                        ],
                        "next_page": f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 101,
                                "comment_id": 10,
                                "created_at": VOTE_101_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 111,
                                "comment_id": 11,
                                "created_at": VOTE_111_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 20,
                                "post_id": 2,
                                "updated_at": COMMENT_20_UPDATED_AT,
                            }
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {
                        "comments": [
                            {
                                "id": 21,
                                "post_id": 2,
                                "updated_at": COMMENT_21_UPDATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 200,
                                "comment_id": 20,
                                "created_at": VOTE_200_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 210,
                                "comment_id": 21,
                                "created_at": VOTE_210_CREATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 30,
                                "post_id": 3,
                                "updated_at": COMMENT_30_UPDATED_AT,
                            }
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts/3/comments/30/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 300,
                                "comment_id": 30,
                                "created_at": VOTE_300_CREATED_AT_TIMESTAMP,
                            }
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_100_CREATED_AT,
                    "id": 100,
                },
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_101_CREATED_AT,
                    "id": 101,
                },
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "created_at": VOTE_111_CREATED_AT,
                    "id": 111,
                },
                {
                    "comment_id": 20,
                    "comment_updated_at": COMMENT_20_UPDATED_AT,
                    "created_at": VOTE_200_CREATED_AT,
                    "id": 200,
                },
                {
                    "comment_id": 21,
                    "comment_updated_at": COMMENT_21_UPDATED_AT,
                    "created_at": VOTE_210_CREATED_AT,
                    "id": 210,
                },
                {
                    "comment_id": 30,
                    "comment_updated_at": COMMENT_30_UPDATED_AT,
                    "created_at": str(VOTE_300_CREATED_AT_TIMESTAMP),
                    "id": 300,
                },
            ],
            # Initial state
            {
                "parent_state": {},
                "use_global_cursor": True,
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR_TIMESTAMP},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "state": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR_TIMESTAMP},
                "lookback_window": 86400,
            },
            # Expected state
            {
                "use_global_cursor": True,
                "lookback_window": 1,
                "parent_state": {},
                "state": {"created_at": VOTE_100_CREATED_AT},
            },
            # Two state messages: one interim heartbeat-keepalive checkpoint emitted
            # during the walk (previously suppressed by the empty-parent guard) plus
            # the final state. The interim checkpoint carries the same inert global
            # cursor, so the final state is unchanged.
            2,
        ),
    ],
)
def test_incremental_parent_state_no_incremental_dependency(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state, state_count
):
    """
    This is a pretty complicated test that syncs a low-code connector stream with three levels of substreams
    - posts: (ids: 1, 2, 3)
    - post comments: (parent post 1 with ids: 9, 10, 11, 12; parent post 2 with ids: 20, 21; parent post 3 with id: 30)
    - post comment votes: (parent comment 10 with ids: 100, 101; parent comment 11 with id: 111;
      parent comment 20 with id: 200; parent comment 21 with id: 210, parent comment 30 with id: 300)

    By setting incremental_dependency to false, parent streams will not use the incoming state and will not update state.
    The post_comment_votes substream is incremental and will emit state messages We verify this by ensuring that mocked
    parent stream requests use the incoming config as query parameters and the substream state messages does not
    contain parent stream state.
    """
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        STREAM_NAME,
        initial_state,
        expected_records,
        expected_state,
        state_count=state_count,
    )


# Fresh initial sync of the ``post_comment_votes`` global-cursor substream whose parent
# (``post_comments``) has no ``incremental_dependency`` -- i.e. the ``bank_accounts`` shape
# from OC-12977. Every child request filters on the config ``start_date`` because the global
# cursor is empty/unadvanced during the walk, so the exact same URLs answer both the initial
# read and any resume-from-interim-checkpoint read.
GLOBAL_CURSOR_RESUME_MOCK_REQUESTS = [
    # Posts page 1
    (
        f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}",
        {
            "posts": [
                {"id": 1, "updated_at": POST_1_UPDATED_AT},
                {"id": 2, "updated_at": POST_2_UPDATED_AT},
            ],
            "next_page": f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}&page=2",
        },
    ),
    # Posts page 2
    (
        f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}&page=2",
        {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
    ),
    # Comments for post 1 page 1
    (
        "https://api.example.com/community/posts/1/comments?per_page=100",
        {
            "comments": [
                {"id": 9, "post_id": 1, "updated_at": COMMENT_9_OLDEST},
                {"id": 10, "post_id": 1, "updated_at": COMMENT_10_UPDATED_AT},
                {"id": 11, "post_id": 1, "updated_at": COMMENT_11_UPDATED_AT},
            ],
            "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
        },
    ),
    # Comments for post 1 page 2
    (
        "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
        {"comments": [{"id": 12, "post_id": 1, "updated_at": COMMENT_12_UPDATED_AT}]},
    ),
    # Votes for comment 10 page 1
    (
        f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time={START_DATE}",
        {
            "votes": [{"id": 100, "comment_id": 10, "created_at": VOTE_100_CREATED_AT}],
            "next_page": f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time={START_DATE}",
        },
    ),
    # Votes for comment 10 page 2
    (
        f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time={START_DATE}",
        {"votes": [{"id": 101, "comment_id": 10, "created_at": VOTE_101_CREATED_AT}]},
    ),
    # Votes for comment 11
    (
        f"https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time={START_DATE}",
        {"votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}]},
    ),
    # Votes for comment 12 (empty child partition)
    (
        f"https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time={START_DATE}",
        {"votes": []},
    ),
    # Comments for post 2 page 1
    (
        "https://api.example.com/community/posts/2/comments?per_page=100",
        {
            "comments": [{"id": 20, "post_id": 2, "updated_at": COMMENT_20_UPDATED_AT}],
            "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
        },
    ),
    # Comments for post 2 page 2
    (
        "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
        {"comments": [{"id": 21, "post_id": 2, "updated_at": COMMENT_21_UPDATED_AT}]},
    ),
    # Votes for comment 20
    (
        f"https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time={START_DATE}",
        {"votes": [{"id": 200, "comment_id": 20, "created_at": VOTE_200_CREATED_AT}]},
    ),
    # Votes for comment 21
    (
        f"https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time={START_DATE}",
        {"votes": [{"id": 210, "comment_id": 21, "created_at": VOTE_210_CREATED_AT}]},
    ),
    # Comments for post 3
    (
        "https://api.example.com/community/posts/3/comments?per_page=100",
        {"comments": [{"id": 30, "post_id": 3, "updated_at": COMMENT_30_UPDATED_AT}]},
    ),
    # Votes for comment 30
    (
        f"https://api.example.com/community/posts/3/comments/30/votes?per_page=100&start_time={START_DATE}",
        {"votes": [{"id": 300, "comment_id": 30, "created_at": VOTE_300_CREATED_AT_TIMESTAMP}]},
    ),
]

GLOBAL_CURSOR_RESUME_EXPECTED_RECORDS = [
    {
        "comment_id": 10,
        "comment_updated_at": COMMENT_10_UPDATED_AT,
        "created_at": VOTE_100_CREATED_AT,
        "id": 100,
    },
    {
        "comment_id": 10,
        "comment_updated_at": COMMENT_10_UPDATED_AT,
        "created_at": VOTE_101_CREATED_AT,
        "id": 101,
    },
    {
        "comment_id": 11,
        "comment_updated_at": COMMENT_11_UPDATED_AT,
        "created_at": VOTE_111_CREATED_AT,
        "id": 111,
    },
    {
        "comment_id": 20,
        "comment_updated_at": COMMENT_20_UPDATED_AT,
        "created_at": VOTE_200_CREATED_AT,
        "id": 200,
    },
    {
        "comment_id": 21,
        "comment_updated_at": COMMENT_21_UPDATED_AT,
        "created_at": VOTE_210_CREATED_AT,
        "id": 210,
    },
    {
        "comment_id": 30,
        "comment_updated_at": COMMENT_30_UPDATED_AT,
        "created_at": str(VOTE_300_CREATED_AT_TIMESTAMP),
        "id": 300,
    },
]


def test_global_cursor_substream_resume_after_interruption_skips_no_records():
    """Interrupt a global-cursor substream mid-walk, resume from every interim checkpoint, and prove no records are skipped.

    Regression test for OC-12977. A ``global_substream_cursor`` substream whose parent has no
    ``incremental_dependency`` (empty parent state -- the ``bank_accounts`` shape) now emits its
    throttled checkpoint STATE during the walk because the empty-parent guard was removed. This
    test proves those interim checkpoints are *safe to resume from*: for each interim STATE the
    cursor emits, we simulate the source being killed right after it (keeping only the records
    emitted before that STATE), then start a fresh read seeded with that STATE and assert the
    union of pre-kill records and resume records covers the full expected set with nothing
    skipped. Because the interim global cursor is inert until the sync finishes, resuming
    re-reads rather than skips -- which is exactly the no-data-loss guarantee we want.
    """
    # Disable the 600s throttle so an interim checkpoint STATE is emitted at every close_partition,
    # giving us many mid-walk "kill" points to resume from.
    with patch.object(
        ConcurrentPerPartitionCursor, "_throttle_state_message", return_value=9999999.0
    ):
        with requests_mock.Mocker() as m:
            for url, response in GLOBAL_CURSOR_RESUME_MOCK_REQUESTS:
                m.get(url, json=response)

            # Fresh initial sync (no incoming state), interrupted implicitly by inspecting
            # every interim checkpoint it emits.
            output = _run_read(
                SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY,
                CONFIG,
                STREAM_NAME,
                None,
            )

            # Sanity: the uninterrupted run yields the full expected record set.
            assert sorted([r.record.data for r in output.records], key=lambda x: x["id"]) == sorted(
                GLOBAL_CURSOR_RESUME_EXPECTED_RECORDS, key=lambda x: x["id"]
            )

            # The fix must produce at least one interim checkpoint STATE during the walk
            # (before the final state) -- otherwise there is nothing to resume from.
            assert len(output.state_messages) >= 2, (
                "Expected at least one interim checkpoint STATE plus the final STATE; "
                f"got {len(output.state_messages)} state message(s)."
            )

            # Map each interim STATE to the records emitted before it (what the destination
            # would have committed had the source been killed right after that checkpoint).
            cumulative_records = []
            interim_checkpoints = []
            for message in output.records_and_state_messages:
                if message.type.value == "RECORD":
                    cumulative_records.append(message.record.data)
                elif message.type.value == "STATE":
                    interim_checkpoints.append((message.state, cumulative_records.copy()))

            expected_deduped = list(
                {orjson.dumps(r): r for r in GLOBAL_CURSOR_RESUME_EXPECTED_RECORDS}.values()
            )

            # Resume from every checkpoint except the final one (the final state is the
            # completed sync, not an interruption).
            for state, records_before_kill in interim_checkpoints[:-1]:
                resume_output = _run_read(
                    SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY,
                    CONFIG,
                    STREAM_NAME,
                    [state],
                )
                records_after_resume = [r.record.data for r in resume_output.records]

                combined = records_before_kill + records_after_resume
                combined_deduped = list({orjson.dumps(r): r for r in combined}.values())

                assert sorted(combined_deduped, key=lambda x: x["id"]) == sorted(
                    expected_deduped, key=lambda x: x["id"]
                ), (
                    "Records were skipped when resuming from interim checkpoint "
                    f"{state.state.stream.stream_state.__dict__}. "
                    f"Expected {expected_deduped}, got {combined_deduped}."
                )


# OC-12977 / Serhii's orchestrator-overload risk: removing the empty-parent guard makes a
# global_substream_cursor substream emit interim checkpoint STATE during a long empty-parent
# walk. The constants + helpers below build that substream (the bank_accounts shape) walking an
# arbitrary number of empty parents (comments whose votes are all empty) so we can measure the
# persisted state size as the empty-parent count grows.
MANY_EMPTY_PARENTS_UPDATED_AT = "2024-06-01T00:00:00Z"  # >= START_DATE so no parent is filtered out


def _emit_state_message_with_guard(self, throttle: bool = True) -> None:
    """Pre-fix ``_emit_state_message`` with the empty-parent guard restored.

    Used to reproduce the pre-fix baseline so we can compare persisted state size before vs.
    after the guard removal on the exact same fixture.
    """
    if throttle:
        current_time = self._throttle_state_message()
        if current_time is None:
            return
        self._last_emission_time = current_time
        # The guard that PR #1068 removes:
        if self._use_global_cursor and not self._parent_state:
            return

    self._connector_state_manager.update_state_for_stream(
        self._stream_name,
        self._stream_namespace,
        self.state,
    )
    state_message = self._connector_state_manager.create_state_message(
        self._stream_name, self._stream_namespace
    )
    self._message_repository.emit_message(state_message)


def _run_global_cursor_over_empty_parents(
    num_empty_parents, num_non_empty_parents=0, reintroduce_guard=False
):
    """Run the global-cursor substream over ``num_empty_parents`` empty parents.

    One post fans out to ``num_non_empty_parents + num_empty_parents`` comments (the parents).
    The first ``num_non_empty_parents`` comments each return a single vote (advancing the global
    cursor and populating the in-memory per-partition map), and the remaining ``num_empty_parents``
    comments return empty votes. The only thing that could grow with the empty-parent count is the
    persisted state, which is exactly what we measure. When ``reintroduce_guard`` is True the
    pre-fix empty-parent guard is patched back in to produce the baseline for comparison.
    """
    posts_response = {"posts": [{"id": 1, "updated_at": MANY_EMPTY_PARENTS_UPDATED_AT}]}
    non_empty_ids = [1000 + i for i in range(num_non_empty_parents)]
    empty_ids = [1000 + num_non_empty_parents + i for i in range(num_empty_parents)]
    comments_response = {
        "comments": [
            {"id": cid, "post_id": 1, "updated_at": MANY_EMPTY_PARENTS_UPDATED_AT}
            for cid in non_empty_ids + empty_ids
        ]
    }

    guard_ctx = (
        patch.object(
            ConcurrentPerPartitionCursor,
            "_emit_state_message",
            _emit_state_message_with_guard,
        )
        if reintroduce_guard
        else nullcontext()
    )
    with guard_ctx, requests_mock.Mocker() as m:
        # Every empty parent's votes call (one per comment) answers empty by default.
        m.get(re.compile(r"/votes"), json={"votes": []})
        # The non-empty parents each return a single vote (more specific match wins).
        for idx, cid in enumerate(non_empty_ids):
            m.get(
                f"https://api.example.com/community/posts/1/comments/{cid}/votes?per_page=100&start_time={START_DATE}",
                json={
                    "votes": [
                        {
                            "id": 900000 + idx,
                            "comment_id": cid,
                            "created_at": MANY_EMPTY_PARENTS_UPDATED_AT,
                        }
                    ]
                },
            )
        m.get(
            f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}",
            json=posts_response,
        )
        m.get(
            "https://api.example.com/community/posts/1/comments?per_page=100",
            json=comments_response,
        )
        return _run_read(
            SUBSTREAM_MANIFEST_WITH_GLOBAL_CURSOR_AND_NO_DEPENDENCY,
            CONFIG,
            STREAM_NAME,
            None,
        )


def _persisted_state_size(state_message) -> int:
    return len(orjson.dumps(state_message.state.stream.stream_state.__dict__))


def test_removing_empty_parent_guard_keeps_state_size_flat_as_empty_parents_grow():
    """OC-12977: prove removing the empty-parent guard does not balloon persisted state.

    Serhii's flagged risk is that emitting interim checkpoint STATE for a ``global_substream_cursor``
    substream with empty parents could grow the persisted connection state per empty parent and
    overload the orchestrator. This test walks the same substream over a handful vs. thousands of
    empty parents and asserts:

    1. the persisted state carries a single bounded global-cursor value with **no** per-partition
       ``states`` map -- so there is no per-parent growth vector at all;
    2. the persisted state is byte-for-byte identical at low vs. high empty-parent counts;
    3. the persisted state is byte-for-byte identical to the pre-fix (guard-restored) baseline;
    4. the 600s throttle keeps the emitted-state *count* a small constant, independent of the
       parent count (we re-emit the same small state at most ~once/600s, not once per parent); and
    5. the in-memory per-partition retention cap is unchanged (it bounds memory, not state).
    """
    low, high = 5, 3000

    low_output = _run_global_cursor_over_empty_parents(low)
    high_output = _run_global_cursor_over_empty_parents(high)

    # Pure empty-parent walk: no records emitted for either count.
    assert len(low_output.records) == 0
    assert len(high_output.records) == 0

    low_final = low_output.state_messages[-1].state.stream.stream_state.__dict__
    high_final = high_output.state_messages[-1].state.stream.stream_state.__dict__

    # (1) Global mode never serializes a per-partition state map -> no per-parent growth vector.
    assert "states" not in low_final
    assert "states" not in high_final

    # (2) Persisted state is byte-for-byte identical at low vs. high empty-parent counts.
    assert orjson.dumps(low_final) == orjson.dumps(high_final)
    low_size = len(orjson.dumps(low_final))
    high_size = len(orjson.dumps(high_final))
    assert low_size == high_size, (
        f"Persisted state grew with empty-parent count: {low_size}B at {low} parents "
        f"vs {high_size}B at {high} parents."
    )
    # Bounded: a single small global-cursor value, not kilobytes-per-parent.
    assert high_size < 500, f"Persisted state unexpectedly large: {high_size}B"
    # Every emitted state message (interim + final) is the same bounded size.
    assert max(_persisted_state_size(m) for m in high_output.state_messages) < 500

    # (3) Unchanged vs. the pre-fix (guard-restored) baseline on the same fixture.
    baseline_output = _run_global_cursor_over_empty_parents(high, reintroduce_guard=True)
    baseline_final = baseline_output.state_messages[-1].state.stream.stream_state.__dict__
    assert orjson.dumps(high_final) == orjson.dumps(baseline_final), (
        "Persisted final state differs from the pre-fix baseline; guard removal must not "
        f"change state content. after={high_final} baseline={baseline_final}"
    )

    # (4) The 600s throttle keeps the emitted-state count a small constant, not per-parent.
    assert len(high_output.state_messages) <= 3, (
        "Expected the 600s throttle to bound state emissions to a small constant, got "
        f"{len(high_output.state_messages)} state messages for {high} empty parents."
    )
    assert len(high_output.state_messages) == len(low_output.state_messages), (
        "State-emission count scaled with empty-parent count: "
        f"{len(low_output.state_messages)} at {low} vs {len(high_output.state_messages)} at {high}."
    )

    # (5) The in-memory per-partition retention cap is unchanged (bounds memory, not state).
    assert ConcurrentPerPartitionCursor.DEFAULT_MAX_PARTITIONS_NUMBER == 25_000
    assert ConcurrentPerPartitionCursor.SWITCH_TO_GLOBAL_LIMIT == 10_000


def test_persisted_state_has_no_per_partition_entries_even_when_cursor_advances():
    """The per-partition state map must not leak into persisted state, even with real records.

    A skeptic might argue the flat state size above is only because a fully-empty walk produces
    no cursor value. This test mixes a few non-empty parents (which advance the global cursor and
    populate the in-memory per-partition map) among many empty parents, then proves the persisted
    state still carries **no** ``states`` per-partition list and stays byte-for-byte identical as
    the empty-parent count grows from a handful to thousands. This is the core guarantee behind the
    orchestrator-overload risk: a ``global_substream_cursor`` stream serializes a single global
    cursor value, never one entry per parent.
    """
    low_output = _run_global_cursor_over_empty_parents(5, num_non_empty_parents=3)
    high_output = _run_global_cursor_over_empty_parents(3000, num_non_empty_parents=3)

    # The non-empty parents produced records, so the global cursor actually advanced.
    assert len(low_output.records) == 3
    assert len(high_output.records) == 3

    low_final = low_output.state_messages[-1].state.stream.stream_state.__dict__
    high_final = high_output.state_messages[-1].state.stream.stream_state.__dict__

    # A real global cursor value is present now...
    assert "state" in high_final
    # ...but there is still no per-partition state map, regardless of the empty-parent count.
    assert "states" not in low_final
    assert "states" not in high_final

    # The persisted state is identical at 5 vs. 3000 empty parents apart from ``lookback_window``,
    # which is a small integer derived from the sync's wall-clock duration (not the parent count).
    # Everything else -- the single global cursor value and the empty parent_state -- is unchanged.
    def _without_lookback(state):
        return {k: v for k, v in state.items() if k != "lookback_window"}

    assert _without_lookback(low_final) == _without_lookback(high_final), (
        f"Persisted state (excluding lookback_window) changed with empty-parent count: "
        f"{low_final} vs {high_final}"
    )
    # And the total serialized size stays flat and bounded regardless of the empty-parent count
    # (the only possible variation is a digit or two in the wall-clock-derived lookback_window).
    assert abs(len(orjson.dumps(low_final)) - len(orjson.dumps(high_final))) <= 3
    assert len(orjson.dumps(high_final)) < 500


def run_incremental_parent_state_test(
    manifest,
    mock_requests,
    expected_records,
    num_intermediate_states,
    initial_state,
    expected_states,
):
    """
    Run an incremental parent state test for the specified stream.

    This function performs the following steps:
    1. Mocks the API requests as defined in mock_requests.
    2. Executes the read operation using the provided manifest and config.
    3. Asserts that the output records match the expected records.
    4. Collects intermediate states and records, performing additional reads as necessary.
    5. Compares the cumulative records from each state against the expected records.
    6. Asserts that the final state matches one of the expected states for each run.

    Args:
        manifest (dict): The manifest configuration for the stream.
        mock_requests (list): A list of tuples containing URL and response data for mocking API requests.
        expected_records (list): The expected records to compare against the output.
        num_intermediate_states (int): The number of intermediate states to expect.
        initial_state (list): The initial state to start the read operation.
        expected_states (list): A list of expected final states after the read operation.
    """
    initial_state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=STREAM_NAME, namespace=None),
                stream_state=AirbyteStateBlob(initial_state),
            ),
        )
    ]

    with requests_mock.Mocker() as m:
        for url, response in mock_requests:
            m.get(url, json=response)

        # Run the initial read
        output = _run_read(manifest, CONFIG, STREAM_NAME, initial_state)

        # Assert that output_data equals expected_records
        assert sorted([r.record.data for r in output.records], key=lambda x: x["id"]) == sorted(
            expected_records, key=lambda x: x["id"]
        )

        # Collect the intermediate states and records produced before each state
        cumulative_records = []
        intermediate_states = []
        final_states = []  # To store the final state after each read

        # Store the final state after the initial read
        final_states.append(output.state_messages[-1].state.stream.stream_state.__dict__)

        for message in output.records_and_state_messages:
            if message.type.value == "RECORD":
                record_data = message.record.data
                cumulative_records.append(record_data)
            elif message.type.value == "STATE":
                # Record the state and the records produced before this state
                state = message.state
                records_before_state = cumulative_records.copy()
                intermediate_states.append((state, records_before_state))

        # Assert that the number of intermediate states is as expected
        assert len(intermediate_states) - 1 == num_intermediate_states
        # Assert that ensure_at_least_one_state_emitted is called before yielding the last record from the last slice
        assert (
            intermediate_states[-1][0].stream.stream_state.__dict__["parent_state"]
            == intermediate_states[-2][0].stream.stream_state.__dict__["parent_state"]
        )

        # For each intermediate state, perform another read starting from that state
        for state, records_before_state in intermediate_states[:-1]:
            output_intermediate = _run_read(manifest, CONFIG, STREAM_NAME, [state])
            records_from_state = [r.record.data for r in output_intermediate.records]

            # Combine records produced before the state with records from the new read
            cumulative_records_state = records_before_state + records_from_state

            # Duplicates may occur because the state matches the cursor of the last record, causing it to be re-emitted in the next sync.
            cumulative_records_state_deduped = list(
                {orjson.dumps(record): record for record in cumulative_records_state}.values()
            )

            # Compare the cumulative records with the expected records
            expected_records_set = list(
                {orjson.dumps(record): record for record in expected_records}.values()
            )
            assert sorted(cumulative_records_state_deduped, key=lambda x: x["id"]) == sorted(
                expected_records_set, key=lambda x: x["id"]
            ), (
                f"Records mismatch with intermediate state {state}. Expected {expected_records}, got {cumulative_records_state_deduped}"
            )

            # Store the final state after each intermediate read
            final_state_intermediate = [
                message.state.stream.stream_state.__dict__
                for message in output_intermediate.state_messages
            ]
            final_states.append(final_state_intermediate[-1])

        # Assert that the final state matches the expected state for all runs
        for i, final_state in enumerate(final_states):
            assert final_state in expected_states, (
                f"Final state mismatch at run {i + 1}. Expected {expected_states}, got {final_state}"
            )


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, num_intermediate_states, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                            {"id": 2, "updated_at": POST_2_UPDATED_AT},
                        ],
                        "next_page": (
                            f"https://api.example.com/community/posts"
                            f"?per_page=100&start_time={PARENT_POSTS_CURSOR}&page=2"
                        ),
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}&page=2",
                    {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
                ),
                # FIXME this is an interesting case. The previous solution would not update the parent state until `ensure_at_least_one_state_emitted` but the concurrent cursor does just before which is probably fine too
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={POST_1_UPDATED_AT}",
                    {"posts": [{"id": 1, "updated_at": POST_1_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 9,
                                "post_id": 1,
                                "updated_at": COMMENT_9_OLDEST,
                            },
                            {
                                "id": 10,
                                "post_id": 1,
                                "updated_at": COMMENT_10_UPDATED_AT,
                            },
                            {
                                "id": 11,
                                "post_id": 1,
                                "updated_at": COMMENT_11_UPDATED_AT,
                            },
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": COMMENT_12_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 100,
                                "comment_id": 10,
                                "created_at": VOTE_100_CREATED_AT,
                            }
                        ],
                        "next_page": (
                            f"https://api.example.com/community/posts/1/comments/10/votes"
                            f"?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}"
                        ),
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes"
                    f"?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {"votes": [{"id": 101, "comment_id": 10, "created_at": VOTE_101_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [{"id": 20, "post_id": 2, "updated_at": COMMENT_20_UPDATED_AT}],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": COMMENT_21_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {"votes": [{"id": 200, "comment_id": 20, "created_at": VOTE_200_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {"votes": [{"id": 210, "comment_id": 21, "created_at": VOTE_210_CREATED_AT}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": COMMENT_30_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts/3/comments/30/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 300,
                                "comment_id": 30,
                                "created_at": VOTE_300_CREATED_AT_TIMESTAMP,
                            }
                        ]
                    },
                ),
                # Requests with intermediate states
                # Fetch votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time={VOTE_100_CREATED_AT}",
                    {
                        "votes": [{"id": 100, "comment_id": 10, "created_at": VOTE_100_CREATED_AT}],
                    },
                ),
                # Fetch votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time={VOTE_111_CREATED_AT}",
                    {
                        "votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}],
                    },
                ),
                # Fetch votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time={VOTE_111_CREATED_AT}",
                    {
                        "votes": [],
                    },
                ),
                # Fetch votes for comment 20 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time={VOTE_200_CREATED_AT}",
                    {"votes": [{"id": 200, "comment_id": 20, "created_at": VOTE_200_CREATED_AT}]},
                ),
                # Fetch votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time={VOTE_210_CREATED_AT}",
                    {"votes": [{"id": 210, "comment_id": 21, "created_at": VOTE_210_CREATED_AT}]},
                ),
                # Fetch votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts/3/comments/30/votes?per_page=100&start_time={VOTE_300_CREATED_AT}",
                    {
                        "votes": [
                            {
                                "id": 300,
                                "comment_id": 30,
                                "created_at": VOTE_300_CREATED_AT_TIMESTAMP,
                            }
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_100_CREATED_AT,
                    "id": 100,
                },
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_101_CREATED_AT,
                    "id": 101,
                },
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "created_at": VOTE_111_CREATED_AT,
                    "id": 111,
                },
                {
                    "comment_id": 20,
                    "comment_updated_at": COMMENT_20_UPDATED_AT,
                    "created_at": VOTE_200_CREATED_AT,
                    "id": 200,
                },
                {
                    "comment_id": 21,
                    "comment_updated_at": COMMENT_21_UPDATED_AT,
                    "created_at": VOTE_210_CREATED_AT,
                    "id": 210,
                },
                {
                    "comment_id": 30,
                    "comment_updated_at": COMMENT_30_UPDATED_AT,
                    "created_at": str(VOTE_300_CREATED_AT_TIMESTAMP),
                    "id": 300,
                },
            ],
            # Number of intermediate states - 6 as number of parent partitions
            6,
            # Initial state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "state": {"created_at": INITIAL_GLOBAL_CURSOR},
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "lookback_window": 86400,
            },
            # Expected state
            {
                "state": {"created_at": VOTE_100_CREATED_AT},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": COMMENT_10_UPDATED_AT},  # 10 is the "latest"
                        "parent_state": {
                            "posts": {"updated_at": POST_1_UPDATED_AT}
                        },  # post 1 is the latest
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_10_UPDATED_AT},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_20_UPDATED_AT},
                            },
                            {
                                "partition": {"id": 3, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_30_UPDATED_AT},
                            },
                        ],
                    }
                },
                "lookback_window": 1,
                "use_global_cursor": False,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_100_CREATED_AT},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_111_CREATED_AT},
                    },
                    {
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": LOOKBACK_DATE},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_200_CREATED_AT},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_210_CREATED_AT},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_300_CREATED_AT},
                    },
                ],
            },
        ),
        (
            "test_incremental_parent_state_one_record_without_cursor",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                        ]
                    },
                ),
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={POST_1_UPDATED_AT}",
                    {"posts": [{"id": 1, "updated_at": POST_1_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 9,
                                "post_id": 1,
                                "updated_at": COMMENT_9_OLDEST,
                            },
                            {
                                "id": 10,
                                "post_id": 1,
                                "updated_at": COMMENT_10_UPDATED_AT,
                            },
                            {
                                "id": 11,
                                "post_id": 1,
                                "updated_at": COMMENT_11_UPDATED_AT,
                            },
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 10 of post 1 (vote without cursor field)
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 100,
                                "comment_id": 10,
                            }
                        ],
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}]},
                ),
                # Requests with intermediate states
                # Fetch votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time={VOTE_111_CREATED_AT}",
                    {
                        "votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}],
                    },
                ),
            ],
            # Expected records
            [
                {"comment_id": 10, "comment_updated_at": COMMENT_10_UPDATED_AT, "id": 100},
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "created_at": "2024-01-13T00:00:00Z",
                    "id": 111,
                },
            ],
            # Number of intermediate states - 6 as number of parent partitions
            2,
            # Initial state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "state": {"created_at": INITIAL_GLOBAL_CURSOR},
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "lookback_window": 86400,
            },
            # Expected state
            {
                "state": {"created_at": VOTE_111_CREATED_AT},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": COMMENT_10_UPDATED_AT},  # 10 is the "latest"
                        "parent_state": {
                            "posts": {"updated_at": POST_1_UPDATED_AT}
                        },  # post 1 is the latest
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_10_UPDATED_AT},
                            },
                        ],
                    }
                },
                "lookback_window": 1,
                "use_global_cursor": False,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        # initial state because record doesn't have a cursor field
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_111_CREATED_AT},
                    },
                ],
            },
        ),
        (
            "test_incremental_parent_state_all_records_without_cursor",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                        ]
                    },
                ),
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={POST_1_UPDATED_AT}",
                    {"posts": [{"id": 1, "updated_at": POST_1_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {
                                "id": 9,
                                "post_id": 1,
                                "updated_at": COMMENT_9_OLDEST,
                            },
                            {
                                "id": 10,
                                "post_id": 1,
                                "updated_at": COMMENT_10_UPDATED_AT,
                            },
                            {
                                "id": 11,
                                "post_id": 1,
                                "updated_at": COMMENT_11_UPDATED_AT,
                            },
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 10 of post 1 (vote without cursor field)
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 100,
                                "comment_id": 10,
                            }
                        ],
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1 (vote without cursor field)
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": [{"id": 111, "comment_id": 11}]},
                ),
            ],
            # Expected records
            [
                {"comment_id": 10, "comment_updated_at": COMMENT_10_UPDATED_AT, "id": 100},
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "id": 111,
                },
            ],
            # Number of intermediate states - 6 as number of parent partitions
            2,
            # Initial state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "state": {"created_at": INITIAL_GLOBAL_CURSOR},
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "lookback_window": 86400,
            },
            # Expected state
            {
                "state": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": COMMENT_10_UPDATED_AT},  # 10 is the "latest"
                        "parent_state": {
                            "posts": {"updated_at": POST_1_UPDATED_AT}
                        },  # post 1 is the latest
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_10_UPDATED_AT},
                            },
                        ],
                    }
                },
                "lookback_window": 86400,
                "use_global_cursor": False,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        # initial state because record doesn't have a cursor field
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
            },
        ),
    ],
)
def test_incremental_parent_state(
    test_name,
    manifest,
    mock_requests,
    expected_records,
    num_intermediate_states,
    initial_state,
    expected_state,
):
    # Patch `_throttle_state_message` so it always returns a float (indicating "no throttle")
    with patch.object(
        ConcurrentPerPartitionCursor, "_throttle_state_message", return_value=9999999.0
    ):
        run_incremental_parent_state_test(
            manifest,
            mock_requests,
            expected_records,
            num_intermediate_states,
            initial_state,
            [expected_state],
        )


STATE_MIGRATION_EXPECTED_STATE = {
    "state": {"created_at": VOTE_100_CREATED_AT},
    "parent_state": {
        "post_comments": {
            "use_global_cursor": False,
            "state": {"updated_at": COMMENT_10_UPDATED_AT},
            "parent_state": {"posts": {"updated_at": POST_1_UPDATED_AT}},
            "lookback_window": 1,
            "states": [
                {
                    "partition": {"id": 1, "parent_slice": {}},
                    "cursor": {"updated_at": COMMENT_10_UPDATED_AT},
                },
                {
                    "partition": {"id": 2, "parent_slice": {}},
                    "cursor": {"updated_at": COMMENT_20_UPDATED_AT},
                },
                {
                    "partition": {"id": 3, "parent_slice": {}},
                    "cursor": {"updated_at": COMMENT_30_UPDATED_AT},
                },
            ],
        }
    },
    "lookback_window": 1,
    "use_global_cursor": False,
    "states": [
        {
            "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
            "cursor": {"created_at": VOTE_100_CREATED_AT},
        },
        {
            "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
            "cursor": {"created_at": VOTE_111_CREATED_AT},
        },
        {
            "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
            "cursor": {"created_at": PARTITION_SYNC_START_TIME},
        },
        {
            "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
            "cursor": {"created_at": VOTE_200_CREATED_AT},
        },
        {
            "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
            "cursor": {"created_at": VOTE_210_CREATED_AT},
        },
        {
            "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
            "cursor": {"created_at": VOTE_300_CREATED_AT},
        },
    ],
}
STATE_MIGRATION_GLOBAL_EXPECTED_STATE = copy.deepcopy(STATE_MIGRATION_EXPECTED_STATE)
del STATE_MIGRATION_GLOBAL_EXPECTED_STATE["states"]
STATE_MIGRATION_GLOBAL_EXPECTED_STATE["use_global_cursor"] = True


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARTITION_SYNC_START_TIME}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                            {"id": 2, "updated_at": POST_2_UPDATED_AT},
                        ],
                        "next_page": (
                            f"https://api.example.com/community/posts?per_page=100"
                            f"&start_time={PARTITION_SYNC_START_TIME}&page=2"
                        ),
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100"
                    f"&start_time={PARTITION_SYNC_START_TIME}&page=2",
                    {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": COMMENT_9_OLDEST},
                            {"id": 10, "post_id": 1, "updated_at": COMMENT_10_UPDATED_AT},
                            {"id": 11, "post_id": 1, "updated_at": COMMENT_11_UPDATED_AT},
                        ],
                        "next_page": (
                            "https://api.example.com/community/posts/1/comments?per_page=100&page=2"
                        ),
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": COMMENT_12_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes"
                    f"?per_page=100&start_time={PARTITION_SYNC_START_TIME}",
                    {
                        "votes": [{"id": 100, "comment_id": 10, "created_at": VOTE_100_CREATED_AT}],
                        "next_page": (
                            f"https://api.example.com/community/posts/1/comments/10/votes"
                            f"?per_page=100&page=2&start_time={PARTITION_SYNC_START_TIME}"
                        ),
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes"
                    f"?per_page=100&page=2&start_time={PARTITION_SYNC_START_TIME}",
                    {"votes": [{"id": 101, "comment_id": 10, "created_at": VOTE_101_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes"
                    f"?per_page=100&start_time={PARTITION_SYNC_START_TIME}",
                    {"votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/12/votes"
                    f"?per_page=100&start_time={PARTITION_SYNC_START_TIME}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [{"id": 20, "post_id": 2, "updated_at": COMMENT_20_UPDATED_AT}],
                        "next_page": (
                            "https://api.example.com/community/posts/2/comments?per_page=100&page=2"
                        ),
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": COMMENT_21_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/20/votes"
                    f"?per_page=100&start_time={PARTITION_SYNC_START_TIME}",
                    {"votes": [{"id": 200, "comment_id": 20, "created_at": VOTE_200_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/21/votes"
                    f"?per_page=100&start_time={PARTITION_SYNC_START_TIME}",
                    {"votes": [{"id": 210, "comment_id": 21, "created_at": VOTE_210_CREATED_AT}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": COMMENT_30_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts/3/comments/30/votes"
                    f"?per_page=100&start_time={PARTITION_SYNC_START_TIME}",
                    {
                        "votes": [
                            {
                                "id": 300,
                                "comment_id": 30,
                                "created_at": VOTE_300_CREATED_AT_TIMESTAMP,
                            }
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_100_CREATED_AT,
                    "id": 100,
                },
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_101_CREATED_AT,
                    "id": 101,
                },
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "created_at": VOTE_111_CREATED_AT,
                    "id": 111,
                },
                {
                    "comment_id": 20,
                    "comment_updated_at": COMMENT_20_UPDATED_AT,
                    "created_at": VOTE_200_CREATED_AT,
                    "id": 200,
                },
                {
                    "comment_id": 21,
                    "comment_updated_at": COMMENT_21_UPDATED_AT,
                    "created_at": VOTE_210_CREATED_AT,
                    "id": 210,
                },
                {
                    "comment_id": 30,
                    "comment_updated_at": COMMENT_30_UPDATED_AT,
                    "created_at": str(VOTE_300_CREATED_AT_TIMESTAMP),
                    "id": 300,
                },
            ],
        ),
    ],
)
@pytest.mark.parametrize(
    "initial_state, expected_state",
    [
        ({"created_at": PARTITION_SYNC_START_TIME}, STATE_MIGRATION_EXPECTED_STATE),
        (
            {
                "state": {"created_at": PARTITION_SYNC_START_TIME},
                "lookback_window": 0,
                "use_global_cursor": False,
                "parent_state": {
                    "post_comments": {
                        "state": {"updated_at": PARTITION_SYNC_START_TIME},
                        "parent_state": {"posts": {"updated_at": PARTITION_SYNC_START_TIME}},
                        "lookback_window": 0,
                    }
                },
            },
            STATE_MIGRATION_EXPECTED_STATE,
        ),
        (
            {
                "state": {"created_at": PARTITION_SYNC_START_TIME},
                "lookback_window": 0,
                "use_global_cursor": True,
                "parent_state": {
                    "post_comments": {
                        "state": {"updated_at": PARTITION_SYNC_START_TIME},
                        "parent_state": {"posts": {"updated_at": PARTITION_SYNC_START_TIME}},
                        "lookback_window": 0,
                    }
                },
            },
            STATE_MIGRATION_GLOBAL_EXPECTED_STATE,
        ),
        (
            {
                "state": {"created_at": PARTITION_SYNC_START_TIME},
            },
            STATE_MIGRATION_EXPECTED_STATE,
        ),
    ],
    ids=[
        "legacy_python_format",
        "low_code_per_partition_state",
        "low_code_global_format",
        "global_state_no_parent",
    ],
)
def test_incremental_parent_state_migration(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    """
    Test incremental partition router with parent state migration
    """
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        STREAM_NAME,
        initial_state,
        expected_records,
        expected_state,
    )


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}",
                    {
                        "posts": [],
                        "next_page": (
                            f"https://api.example.com/community/posts?per_page=100"
                            f"&start_time={PARENT_POSTS_CURSOR}&page=2"
                        ),
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100"
                    f"&start_time={PARENT_POSTS_CURSOR}&page=2",
                    {"posts": []},
                ),
            ],
            # Expected records (empty)
            [],
            # Initial state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "state": {"created_at": INITIAL_GLOBAL_CURSOR},
                "lookback_window": 1,
            },
            # Expected state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": START_DATE},
                            }
                        ],
                        "lookback_window": 0,
                        "use_global_cursor": False,
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "state": {"created_at": INITIAL_GLOBAL_CURSOR},
                "lookback_window": 1,
                "use_global_cursor": False,
            },
        ),
    ],
)
def test_incremental_parent_state_no_slices(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    """
    Test incremental partition router with no parent records
    """
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        STREAM_NAME,
        initial_state,
        expected_records,
        expected_state,
    )


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                            {"id": 2, "updated_at": POST_2_UPDATED_AT},
                        ],
                        "next_page": (
                            f"https://api.example.com/community/posts?per_page=100"
                            f"&start_time={PARENT_POSTS_CURSOR}&page=2"
                        ),
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100"
                    f"&start_time={PARENT_POSTS_CURSOR}&page=2",
                    {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": COMMENT_9_OLDEST},
                            {"id": 10, "post_id": 1, "updated_at": COMMENT_10_UPDATED_AT},
                            {"id": 11, "post_id": 1, "updated_at": COMMENT_11_UPDATED_AT},
                        ],
                        "next_page": (
                            "https://api.example.com/community/posts/1/comments?per_page=100&page=2"
                        ),
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": COMMENT_12_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [],
                        "next_page": (
                            f"https://api.example.com/community/posts/1/comments/10/votes"
                            f"?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}"
                        ),
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes"
                    f"?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/12/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [{"id": 20, "post_id": 2, "updated_at": COMMENT_20_UPDATED_AT}],
                        "next_page": (
                            "https://api.example.com/community/posts/2/comments?per_page=100&page=2"
                        ),
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": COMMENT_21_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/20/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/21/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": COMMENT_30_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts/3/comments/30/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": []},
                ),
            ],
            # Expected records
            [],
            # Initial state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "use_global_cursor": False,
                "state": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                "lookback_window": 0,
            },
            # Expected state
            {
                "lookback_window": 0,
                "use_global_cursor": False,
                "state": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                    {
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": COMMENT_10_UPDATED_AT},
                        "parent_state": {"posts": {"updated_at": POST_1_UPDATED_AT}},
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_10_UPDATED_AT},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_20_UPDATED_AT},
                            },
                            {
                                "partition": {"id": 3, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_30_UPDATED_AT},
                            },
                        ],
                    }
                },
            },
        ),
    ],
)
def test_incremental_parent_state_no_records(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    """
    Test incremental partition router with no child records
    """
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        STREAM_NAME,
        initial_state,
        expected_records,
        expected_state,
    )


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                            {"id": 2, "updated_at": POST_2_UPDATED_AT},
                        ],
                        "next_page": (
                            f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}&page=2"
                        ),
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}&page=2",
                    {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": COMMENT_9_OLDEST},
                            {"id": 10, "post_id": 1, "updated_at": COMMENT_10_UPDATED_AT},
                            {"id": 11, "post_id": 1, "updated_at": COMMENT_11_UPDATED_AT},
                        ],
                        "next_page": (
                            "https://api.example.com/community/posts/1/comments?per_page=100&page=2"
                        ),
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": COMMENT_12_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [{"id": 100, "comment_id": 10, "created_at": VOTE_100_CREATED_AT}],
                        "next_page": (
                            f"https://api.example.com/community/posts/1/comments/10/votes"
                            f"?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}"
                        ),
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/10/votes"
                    f"?per_page=100&page=2&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {"votes": [{"id": 101, "comment_id": 10, "created_at": VOTE_101_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/11/votes"
                    f"?per_page=100&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time={LOOKBACK_DATE}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [{"id": 20, "post_id": 2, "updated_at": COMMENT_20_UPDATED_AT}],
                        "next_page": (
                            "https://api.example.com/community/posts/2/comments?per_page=100&page=2"
                        ),
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": COMMENT_21_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2 - 404 error
                (
                    f"https://api.example.com/community/posts/2/comments/20/votes"
                    f"?per_page=100&start_time={LOOKBACK_DATE}",
                    None,
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts/2/comments/21/votes"
                    f"?per_page=100&start_time={LOOKBACK_DATE}",
                    {"votes": [{"id": 210, "comment_id": 21, "created_at": VOTE_210_CREATED_AT}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": COMMENT_30_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts/3/comments/30/votes"
                    f"?per_page=100&start_time={LOOKBACK_DATE}",
                    {
                        "votes": [
                            {
                                "id": 300,
                                "comment_id": 30,
                                "created_at": VOTE_300_CREATED_AT_TIMESTAMP,
                            }
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_100_CREATED_AT,
                    "id": 100,
                },
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_101_CREATED_AT,
                    "id": 101,
                },
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "created_at": VOTE_111_CREATED_AT,
                    "id": 111,
                },
                {
                    "comment_id": 21,
                    "comment_updated_at": COMMENT_21_UPDATED_AT,
                    "created_at": VOTE_210_CREATED_AT,
                    "id": 210,
                },
                {
                    "comment_id": 30,
                    "comment_updated_at": COMMENT_30_UPDATED_AT,
                    "created_at": str(VOTE_300_CREATED_AT_TIMESTAMP),
                    "id": 300,
                },
            ],
            # Initial state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "state": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                "lookback_window": 86400,
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
            },
            # Expected state
            {
                # The global state and lookback window are the same because sync failed for comment 20.
                # The parent state will be updated up until the child records that were successful i.t. until post 2.
                # Note that we still have an entry for the partition with post 2 but it is populated with the start date.
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_10_UPDATED_AT},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": START_DATE},
                            },
                        ],
                        "lookback_window": 0,
                        "use_global_cursor": False,
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "state": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                "lookback_window": 86400,
                "use_global_cursor": False,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_100_CREATED_AT},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_111_CREATED_AT},
                    },
                    {
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": LOOKBACK_DATE},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": LOOKBACK_DATE},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_210_CREATED_AT},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_300_CREATED_AT},
                    },
                ],
            },
        ),
    ],
)
def test_incremental_substream_error(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        STREAM_NAME,
        initial_state,
        expected_records,
        expected_state,
    )


LISTPARTITION_MANIFEST: MutableMapping[str, Any] = {
    "version": "0.51.42",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["post_comments"]},
    "definitions": {
        "basic_authenticator": {
            "type": "BasicHttpAuthenticator",
            "username": "{{ config['credentials']['email'] + '/token' }}",
            "password": "{{ config['credentials']['api_token'] }}",
        },
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://api.example.com",
                "http_method": "GET",
                "authenticator": "#/definitions/basic_authenticator",
            },
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {
                    "type": "DpathExtractor",
                    "field_path": ["{{ parameters.get('data_path') or parameters['name'] }}"],
                },
                "schema_normalization": "Default",
            },
            "paginator": {
                "type": "DefaultPaginator",
                "page_size_option": {
                    "type": "RequestOption",
                    "field_name": "per_page",
                    "inject_into": "request_parameter",
                },
                "pagination_strategy": {
                    "type": "CursorPagination",
                    "page_size": 100,
                    "cursor_value": "{{ response.get('next_page', {}) }}",
                    "stop_condition": "{{ not response.get('next_page', {}) }}",
                },
                "page_token_option": {"type": "RequestPath"},
            },
        },
        "cursor_incremental_sync": {
            "type": "DatetimeBasedCursor",
            "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"],
            "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
            "cursor_field": "{{ parameters.get('cursor_field',  'updated_at') }}",
            "start_datetime": {"datetime": "{{ config.get('start_date')}}"},
            "start_time_option": {
                "inject_into": "request_parameter",
                "field_name": "start_time",
                "type": "RequestOption",
            },
        },
        "post_comments_stream": {
            "type": "DeclarativeStream",
            "name": "post_comments",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "updated_at": {"type": "string", "format": "date-time"},
                        "post_id": {"type": "integer"},
                        "comment": {"type": "string"},
                    },
                    "type": "object",
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/community/posts/{{ stream_slice.id }}/comments",
                    "http_method": "GET",
                    "authenticator": "#/definitions/basic_authenticator",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {
                        "type": "DpathExtractor",
                        "field_path": ["{{ parameters.get('data_path') or parameters['name'] }}"],
                    },
                    "schema_normalization": "Default",
                },
                "paginator": "#/definitions/retriever/paginator",
                "partition_router": {
                    "type": "ListPartitionRouter",
                    "cursor_field": "id",
                    "values": ["1", "2", "3"],
                },
            },
            "incremental_sync": {
                "$ref": "#/definitions/cursor_incremental_sync",
                "is_client_side_incremental": True,
            },
            "$parameters": {
                "name": "post_comments",
                "path": "community/posts/{{ stream_slice.id }}/comments",
                "data_path": "comments",
                "cursor_field": "updated_at",
                "primary_key": "id",
            },
        },
    },
    "streams": [
        {"$ref": "#/definitions/post_comments_stream"},
    ],
    "concurrency_level": {
        "type": "ConcurrencyLevel",
        "default_concurrency": "{{ config['num_workers'] or 10 }}",
        "max_concurrency": 25,
    },
    "spec": {
        "type": "Spec",
        "documentation_url": "https://airbyte.com/#yaml-from-manifest",
        "connection_specification": {
            "title": "Test Spec",
            "type": "object",
            "required": ["credentials", "start_date"],
            "additionalProperties": False,
            "properties": {
                "credentials": {
                    "type": "object",
                    "required": ["email", "api_token"],
                    "properties": {
                        "email": {
                            "type": "string",
                            "title": "Email",
                            "description": "The email for authentication.",
                        },
                        "api_token": {
                            "type": "string",
                            "airbyte_secret": True,
                            "title": "API Token",
                            "description": "The API token for authentication.",
                        },
                    },
                },
                "start_date": {
                    "type": "string",
                    "format": "date-time",
                    "title": "Start Date",
                    "description": "The date from which to start syncing data.",
                },
            },
        },
    },
}


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            LISTPARTITION_MANIFEST,
            [
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&start_time=2024-01-24T00:00:00Z",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2&start_time=2024-01-24T00:00:00Z",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2&start_time=2024-01-24T00:00:00Z",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": "2024-01-23T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&start_time=2024-01-21T05:00:00Z",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2&start_time=2024-01-21T05:00:00Z",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2&start_time=2024-01-21T05:00:00Z",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100&start_time=2024-01-08T00:00:00Z",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"}]},
                ),
            ],
            # Expected records
            [
                {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"},
                {"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"},
            ],
            # Initial state
            {
                "state": {"updated_at": "2024-01-08T00:00:00Z"},
                "states": [
                    {
                        "cursor": {"updated_at": "2024-01-24T00:00:00Z"},
                        "partition": {"id": "1"},
                    },
                    {
                        "cursor": {"updated_at": "2024-01-21T05:00:00Z"},
                        "partition": {"id": "2"},
                    },
                ],
                "use_global_cursor": False,
            },
            # Expected state
            {
                "use_global_cursor": False,
                "lookback_window": 1,
                "state": {"updated_at": "2024-01-25T00:00:00Z"},
                "states": [
                    {"cursor": {"updated_at": "2024-01-25T00:00:00Z"}, "partition": {"id": "1"}},
                    {"cursor": {"updated_at": "2024-01-22T00:00:00Z"}, "partition": {"id": "2"}},
                    {"cursor": {"updated_at": "2024-01-09T00:00:00Z"}, "partition": {"id": "3"}},
                ],
            },
        ),
    ],
)
def test_incremental_list_partition_router(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    """
    Test ConcurrentPerPartitionCursor with ListPartitionRouter
    """
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        "post_comments",
        initial_state,
        expected_records,
        expected_state,
    )


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_error_handling",
            LISTPARTITION_MANIFEST,
            [
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&start_time=2024-01-20T00:00:00Z",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2&start_time=2024-01-20T00:00:00Z",
                    },
                ),
                # Error response for the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2&start_time=2024-01-20T00:00:00Z",
                    None,  # Simulate a network error or an empty response
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&start_time=2024-01-21T05:00:00Z",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2&start_time=2024-01-21T05:00:00Z",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2&start_time=2024-01-21T05:00:00Z",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100&start_time=2024-01-08T00:00:00Z",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"}]},
                ),
            ],
            # Expected records
            [
                {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"},
                {"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"},
            ],
            # Initial state
            {
                "state": {"updated_at": "2024-01-08T00:00:00Z"},
                "states": [
                    {
                        "cursor": {"updated_at": "2024-01-20T00:00:00Z"},
                        "partition": {"id": "1"},
                    },
                    {
                        "cursor": {"updated_at": "2024-01-21T05:00:00Z"},
                        "partition": {"id": "2"},
                    },
                ],
                "use_global_cursor": False,
            },
            # Expected state
            {
                "lookback_window": 0,
                "use_global_cursor": False,
                "state": {"updated_at": "2024-01-08T00:00:00Z"},
                "states": [
                    {"cursor": {"updated_at": "2024-01-20T00:00:00Z"}, "partition": {"id": "1"}},
                    {"cursor": {"updated_at": "2024-01-22T00:00:00Z"}, "partition": {"id": "2"}},
                    {"cursor": {"updated_at": "2024-01-09T00:00:00Z"}, "partition": {"id": "3"}},
                ],
            },
        ),
    ],
)
def test_incremental_error(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    """
    Test with failed request.
    """
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        "post_comments",
        initial_state,
        expected_records,
        expected_state,
    )


SUBSTREAM_REQUEST_OPTIONS_MANIFEST: MutableMapping[str, Any] = {
    "version": "0.51.42",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["post_comment_votes"]},
    "definitions": {
        "basic_authenticator": {
            "type": "BasicHttpAuthenticator",
            "username": "{{ config['credentials']['email'] + '/token' }}",
            "password": "{{ config['credentials']['api_token'] }}",
        },
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://api.example.com",
                "http_method": "GET",
                "authenticator": "#/definitions/basic_authenticator",
            },
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {
                    "type": "DpathExtractor",
                    "field_path": ["{{ parameters.get('data_path') or parameters['name'] }}"],
                },
                "schema_normalization": "Default",
            },
            "paginator": {
                "type": "DefaultPaginator",
                "page_size_option": {
                    "type": "RequestOption",
                    "field_name": "per_page",
                    "inject_into": "request_parameter",
                },
                "pagination_strategy": {
                    "type": "CursorPagination",
                    "page_size": 100,
                    "cursor_value": "{{ response.get('next_page', {}) }}",
                    "stop_condition": "{{ not response.get('next_page', {}) }}",
                },
                "page_token_option": {"type": "RequestPath"},
            },
        },
        "cursor_incremental_sync": {
            "type": "DatetimeBasedCursor",
            "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"],
            "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
            "cursor_field": "{{ parameters.get('cursor_field',  'updated_at') }}",
            "start_datetime": {"datetime": "{{ config.get('start_date')}}"},
            "start_time_option": {
                "inject_into": "request_parameter",
                "field_name": "start_time",
                "type": "RequestOption",
            },
        },
        "posts_stream": {
            "type": "DeclarativeStream",
            "name": "posts",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "updated_at": {"type": "string", "format": "date-time"},
                        "title": {"type": "string"},
                        "content": {"type": "string"},
                    },
                    "type": "object",
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/community/posts",
                    "http_method": "GET",
                    "authenticator": "#/definitions/basic_authenticator",
                },
                "record_selector": "#/definitions/retriever/record_selector",
                "paginator": "#/definitions/retriever/paginator",
            },
            "incremental_sync": "#/definitions/cursor_incremental_sync",
            "$parameters": {
                "name": "posts",
                "path": "community/posts",
                "data_path": "posts",
                "cursor_field": "updated_at",
                "primary_key": "id",
            },
        },
        "post_comments_stream": {
            "type": "DeclarativeStream",
            "name": "post_comments",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "updated_at": {"type": "string", "format": "date-time"},
                        "post_id": {"type": "integer"},
                        "comment": {"type": "string"},
                    },
                    "type": "object",
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/community/posts_comments",
                    "http_method": "GET",
                    "authenticator": "#/definitions/basic_authenticator",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": ["comments"]},
                    "record_filter": {
                        "condition": "{{ record['updated_at'] >= stream_interval['extra_fields'].get('updated_at', config.get('start_date')) }}"
                    },
                },
                "paginator": "#/definitions/retriever/paginator",
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "stream": "#/definitions/posts_stream",
                            "parent_key": "id",
                            "partition_field": "id",
                            "incremental_dependency": True,
                            "request_option": {
                                "inject_into": "request_parameter",
                                "type": "RequestOption",
                                "field_name": "post_id",
                            },
                        }
                    ],
                },
            },
            "incremental_sync": {
                "type": "DatetimeBasedCursor",
                "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"],
                "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                "cursor_field": "{{ parameters.get('cursor_field',  'updated_at') }}",
                "start_datetime": {"datetime": "{{ config.get('start_date') }}"},
            },
            "$parameters": {
                "name": "post_comments",
                "path": "community/posts_comments",
                "data_path": "comments",
                "cursor_field": "updated_at",
                "primary_key": "id",
            },
        },
        "post_comment_votes_stream": {
            "type": "DeclarativeStream",
            "name": "post_comment_votes",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "comment_id": {"type": "integer"},
                        "vote": {"type": "number"},
                    },
                    "type": "object",
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/community/posts_comments_votes",
                    "http_method": "GET",
                    "authenticator": "#/definitions/basic_authenticator",
                },
                "record_selector": "#/definitions/retriever/record_selector",
                "paginator": "#/definitions/retriever/paginator",
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "stream": "#/definitions/post_comments_stream",
                            "parent_key": "id",
                            "partition_field": "id",
                            "incremental_dependency": True,
                            "extra_fields": [["updated_at"]],
                            "request_option": {
                                "inject_into": "request_parameter",
                                "type": "RequestOption",
                                "field_name": "comment_id",
                            },
                        }
                    ],
                },
            },
            "transformations": [
                {
                    "type": "AddFields",
                    "fields": [
                        {
                            "path": ["comment_updated_at"],
                            "value_type": "string",
                            "value": "{{ stream_slice.extra_fields['updated_at'] }}",
                        },
                    ],
                },
            ],
            "incremental_sync": "#/definitions/cursor_incremental_sync",
            "$parameters": {
                "name": "post_comment_votes",
                "path": "community/posts_comments_votes",
                "data_path": "votes",
                "cursor_field": "created_at",
                "primary_key": "id",
            },
        },
    },
    "streams": [
        {"$ref": "#/definitions/posts_stream"},
        {"$ref": "#/definitions/post_comments_stream"},
        {"$ref": "#/definitions/post_comment_votes_stream"},
    ],
    "concurrency_level": {
        "type": "ConcurrencyLevel",
        "default_concurrency": "{{ config['num_workers'] or 10 }}",
        "max_concurrency": 25,
    },
    "spec": {
        "type": "Spec",
        "documentation_url": "https://airbyte.com/#yaml-from-manifest",
        "connection_specification": {
            "title": "Test Spec",
            "type": "object",
            "required": ["credentials", "start_date"],
            "additionalProperties": False,
            "properties": {
                "credentials": {
                    "type": "object",
                    "required": ["email", "api_token"],
                    "properties": {
                        "email": {
                            "type": "string",
                            "title": "Email",
                            "description": "The email for authentication.",
                        },
                        "api_token": {
                            "type": "string",
                            "airbyte_secret": True,
                            "title": "API Token",
                            "description": "The API token for authentication.",
                        },
                    },
                },
                "start_date": {
                    "type": "string",
                    "format": "date-time",
                    "title": "Start Date",
                    "description": "The date from which to start syncing data.",
                },
            },
        },
    },
}


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_REQUEST_OPTIONS_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}",
                    {
                        "posts": [
                            {"id": 1, "updated_at": POST_1_UPDATED_AT},
                            {"id": 2, "updated_at": POST_2_UPDATED_AT},
                        ],
                        "next_page": (
                            f"https://api.example.com/community/posts"
                            f"?per_page=100&start_time={PARENT_POSTS_CURSOR}&page=2"
                        ),
                    },
                ),
                # Fetch the second page of posts
                (
                    f"https://api.example.com/community/posts?per_page=100&start_time={PARENT_POSTS_CURSOR}&page=2",
                    {"posts": [{"id": 3, "updated_at": POST_3_UPDATED_AT}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts_comments?per_page=100&post_id=1",
                    {
                        "comments": [
                            {
                                "id": 9,
                                "post_id": 1,
                                "updated_at": COMMENT_9_OLDEST,
                            },
                            {
                                "id": 10,
                                "post_id": 1,
                                "updated_at": COMMENT_10_UPDATED_AT,
                            },
                            {
                                "id": 11,
                                "post_id": 1,
                                "updated_at": COMMENT_11_UPDATED_AT,
                            },
                        ],
                        "next_page": "https://api.example.com/community/posts_comments?per_page=100&post_id=1&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts_comments?per_page=100&post_id=1&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": COMMENT_12_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts_comments_votes?per_page=100&comment_id=10&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {
                        "votes": [
                            {
                                "id": 100,
                                "comment_id": 10,
                                "created_at": VOTE_100_CREATED_AT,
                            }
                        ],
                        "next_page": (
                            f"https://api.example.com/community/posts_comments_votes"
                            f"?per_page=100&page=2&comment_id=10&start_time={INITIAL_STATE_PARTITION_10_CURSOR}"
                        ),
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    f"https://api.example.com/community/posts_comments_votes"
                    f"?per_page=100&page=2&comment_id=10&start_time={INITIAL_STATE_PARTITION_10_CURSOR}",
                    {"votes": [{"id": 101, "comment_id": 10, "created_at": VOTE_101_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    f"https://api.example.com/community/posts_comments_votes"
                    f"?per_page=100&comment_id=11&start_time={INITIAL_STATE_PARTITION_11_CURSOR}",
                    {"votes": [{"id": 111, "comment_id": 11, "created_at": VOTE_111_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    f"https://api.example.com/community/posts_comments_votes?"
                    f"per_page=100&comment_id=12&start_time={LOOKBACK_DATE}",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts_comments?per_page=100&post_id=2",
                    {
                        "comments": [{"id": 20, "post_id": 2, "updated_at": COMMENT_20_UPDATED_AT}],
                        "next_page": "https://api.example.com/community/posts_comments?per_page=100&post_id=2&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts_comments?per_page=100&post_id=2&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": COMMENT_21_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    f"https://api.example.com/community/posts_comments_votes"
                    f"?per_page=100&comment_id=20&start_time={LOOKBACK_DATE}",
                    {"votes": [{"id": 200, "comment_id": 20, "created_at": VOTE_200_CREATED_AT}]},
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    f"https://api.example.com/community/posts_comments_votes?"
                    f"per_page=100&comment_id=21&start_time={LOOKBACK_DATE}",
                    {"votes": [{"id": 210, "comment_id": 21, "created_at": VOTE_210_CREATED_AT}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts_comments?per_page=100&post_id=3",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": COMMENT_30_UPDATED_AT}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    f"https://api.example.com/community/posts_comments_votes?"
                    f"per_page=100&comment_id=30&start_time={LOOKBACK_DATE}",
                    {"votes": [{"id": 300, "comment_id": 30, "created_at": VOTE_300_CREATED_AT}]},
                ),
            ],
            # Expected records
            [
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_100_CREATED_AT,
                    "id": 100,
                },
                {
                    "comment_id": 10,
                    "comment_updated_at": COMMENT_10_UPDATED_AT,
                    "created_at": VOTE_101_CREATED_AT,
                    "id": 101,
                },
                {
                    "comment_id": 11,
                    "comment_updated_at": COMMENT_11_UPDATED_AT,
                    "created_at": VOTE_111_CREATED_AT,
                    "id": 111,
                },
                {
                    "comment_id": 20,
                    "comment_updated_at": COMMENT_20_UPDATED_AT,
                    "created_at": VOTE_200_CREATED_AT,
                    "id": 200,
                },
                {
                    "comment_id": 21,
                    "comment_updated_at": COMMENT_21_UPDATED_AT,
                    "created_at": VOTE_210_CREATED_AT,
                    "id": 210,
                },
                {
                    "comment_id": 30,
                    "comment_updated_at": COMMENT_30_UPDATED_AT,
                    "created_at": VOTE_300_CREATED_AT,
                    "id": 300,
                },
            ],
            # Initial state
            {
                "parent_state": {
                    "post_comments": {
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "parent_state": {"posts": {"updated_at": PARENT_POSTS_CURSOR}},
                    }
                },
                "state": {"created_at": INITIAL_GLOBAL_CURSOR},
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_10_CURSOR},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": INITIAL_STATE_PARTITION_11_CURSOR},
                    },
                ],
                "lookback_window": 86400,
            },
            # Expected state
            {
                "state": {"created_at": VOTE_100_CREATED_AT},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": COMMENT_10_UPDATED_AT},  # 10 is the "latest"
                        "parent_state": {
                            "posts": {"updated_at": POST_1_UPDATED_AT}
                        },  # post 1 is the latest
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_10_UPDATED_AT},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_20_UPDATED_AT},
                            },
                            {
                                "partition": {"id": 3, "parent_slice": {}},
                                "cursor": {"updated_at": COMMENT_30_UPDATED_AT},
                            },
                        ],
                    }
                },
                "lookback_window": 1,
                "use_global_cursor": False,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_100_CREATED_AT},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_111_CREATED_AT},
                    },
                    {
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": LOOKBACK_DATE},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_200_CREATED_AT},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_210_CREATED_AT},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": VOTE_300_CREATED_AT},
                    },
                ],
            },
        ),
    ],
)
def test_incremental_substream_request_options_provider(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    """
    Test incremental syncing for a stream that uses request options provider from parent stream config.
    """
    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        "post_comment_votes",
        initial_state,
        expected_records,
        expected_state,
    )


def test_state_throttling(mocker):
    """
    Verifies that _emit_state_message does not emit a new state if less than 600s
    have passed since last emission, and does emit once 600s or more have passed.
    """
    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=MagicMock(),
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=MagicMock(),
        cursor_field=MagicMock(),
    )

    mock_connector_manager = cursor._connector_state_manager
    mock_repo = cursor._message_repository

    # Set the last emission time to "0" so we can control offset from that
    cursor._last_emission_time = 0

    mock_time = mocker.patch("time.time")

    # First attempt: only 100 seconds passed => NO emission
    mock_time.return_value = 100
    cursor._emit_state_message()
    mock_connector_manager.update_state_for_stream.assert_not_called()
    mock_repo.emit_message.assert_not_called()

    # Second attempt: 300 seconds passed => still NO emission
    mock_time.return_value = 300
    cursor._emit_state_message()
    mock_connector_manager.update_state_for_stream.assert_not_called()
    mock_repo.emit_message.assert_not_called()

    # Advance time: 700 seconds => exceed 600s => MUST emit
    mock_time.return_value = 700
    cursor._emit_state_message()
    mock_connector_manager.update_state_for_stream.assert_called_once()
    mock_repo.emit_message.assert_called_once()


@pytest.mark.parametrize(
    "parent_state",
    [
        pytest.param({}, id="empty_parent_state"),
        pytest.param({"parent": {"updated_at": "2024-01-01"}}, id="non_empty_parent_state"),
    ],
)
def test_global_cursor_emits_interim_state_even_when_parent_state_empty(mocker, parent_state):
    """
    Regression test for OC-12977: a global_substream_cursor stream walking a large
    parent with mostly-empty children must still emit its throttled checkpoint STATE
    to keep the Airbyte source heartbeat alive, even when the parent state is empty
    (no ``incremental_dependency``). The previous empty-parent guard suppressed this
    interim emission, causing >24h silence and a heartbeat timeout.
    """
    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=MagicMock(),
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=MagicMock(),
        cursor_field=MagicMock(),
    )

    cursor._use_global_cursor = True
    cursor._parent_state = parent_state
    cursor._last_emission_time = 0

    mock_time = mocker.patch("time.time")
    # Exceed the 600s throttle so emission is not throttled.
    mock_time.return_value = 700

    cursor._emit_state_message()

    cursor._connector_state_manager.update_state_for_stream.assert_called_once()
    cursor._message_repository.emit_message.assert_called_once()


def test_given_no_partitions_processed_when_close_partition_then_no_state_update():
    mock_cursor = MagicMock()
    # No slices for no partitions
    mock_cursor.stream_slices.side_effect = [iter([])]
    mock_cursor.state = {}  # Empty state for no partitions

    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.return_value = mock_cursor

    connector_state_converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=connector_state_converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )
    partition_router = cursor._partition_router
    partition_router.stream_slices.return_value = iter([])
    partition_router.get_stream_state.return_value = {}

    slices = list(cursor.stream_slices())  # Call once
    for slice in slices:
        cursor.close_partition(
            DeclarativePartition(
                stream_name="test_stream",
                schema_loader=_EMPTY_SCHEMA_LOADER,
                retriever=MagicMock(),
                message_repository=MagicMock(),
                max_records_limit=None,
                stream_slice=slice,
                record_counter=RecordCounter(),
            )
        )

    assert cursor.state == {
        "use_global_cursor": False,
        "lookback_window": 0,
        "states": [],
    }
    assert len(cursor._cursor_per_partition) == 0
    assert len(cursor._semaphore_per_partition) == 0
    assert len(cursor._partition_parent_state_map) == 0
    assert mock_cursor.stream_slices.call_count == 0  # No calls since no partitions


def test_given_unfinished_first_parent_partition_no_parent_state_update():
    # Create two mock cursors with different states for each partition
    mock_cursor_1 = MagicMock()
    mock_cursor_1.stream_slices.return_value = iter(
        [
            {"slice1": "data1"},
            {"slice2": "data1"},  # First partition slices
        ]
    )
    mock_cursor_1.state = {"updated_at": "2024-01-01T00:00:00Z"}  # State for partition "1"

    mock_cursor_2 = MagicMock()
    mock_cursor_2.stream_slices.return_value = iter(
        [
            {"slice2": "data2"},
            {"slice2": "data2"},  # Second partition slices
        ]
    )
    mock_cursor_2.state = {"updated_at": "2024-01-02T00:00:00Z"}  # State for partition "2"

    # Configure cursor factory to return different mock cursors based on partition
    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.side_effect = [mock_cursor_1, mock_cursor_2]

    connector_state_converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={
            "states": [
                {"partition": {"id": "1"}, "cursor": {"updated_at": "2024-01-01T00:00:00Z"}}
            ],
            "state": {"updated_at": "2024-01-01T00:00:00Z"},
            "lookback_window": 86400,
            "parent_state": {"posts": {"updated_at": "2024-01-01T00:00:00Z"}},
        },
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=connector_state_converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )
    partition_router = cursor._partition_router
    all_partitions = [
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "2"}, cursor_slice={}, extra_fields={}),  # New partition
    ]
    partition_router.stream_slices.return_value = iter(all_partitions)
    partition_router.get_stream_state.side_effect = [
        {"posts": {"updated_at": "2024-01-04T00:00:00Z"}},  # Initial parent state
        {"posts": {"updated_at": "2024-01-05T00:00:00Z"}},  # Updated parent state for new partition
    ]

    slices = list(cursor.stream_slices())
    # Close all partitions except from the first one
    for slice in slices[1:]:
        cursor.close_partition(
            DeclarativePartition(
                stream_name="test_stream",
                schema_loader=_EMPTY_SCHEMA_LOADER,
                retriever=MagicMock(),
                message_repository=MagicMock(),
                max_records_limit=None,
                stream_slice=slice,
                record_counter=RecordCounter(),
            )
        )
    cursor.ensure_at_least_one_state_emitted()

    state = cursor.state
    assert state == {
        "use_global_cursor": False,
        "states": [
            {"partition": {"id": "1"}, "cursor": {"updated_at": "2024-01-01T00:00:00Z"}},
            {"partition": {"id": "2"}, "cursor": {"updated_at": "2024-01-02T00:00:00Z"}},
        ],
        "state": {"updated_at": "2024-01-01T00:00:00Z"},
        "lookback_window": 86400,
        "parent_state": {"posts": {"updated_at": "2024-01-01T00:00:00Z"}},
    }
    assert mock_cursor_1.stream_slices.call_count == 1  # Called once for each partition
    assert mock_cursor_2.stream_slices.call_count == 1  # Called once for each partition

    assert len(cursor._semaphore_per_partition) == 1
    assert len(cursor._partitions_done_generating_stream_slices) == 1
    assert len(cursor._processing_partitions_indexes) == 1
    assert len(cursor._partition_key_to_index) == 1


def test_given_unfinished_last_parent_partition_with_partial_parent_state_update():
    # Create two mock cursors with different states for each partition
    mock_cursor_1 = MagicMock()
    mock_cursor_1.stream_slices.return_value = iter(
        [
            {"slice1": "data1"},
            {"slice2": "data1"},  # First partition slices
        ]
    )
    mock_cursor_1.state = {"updated_at": "2024-01-02T00:00:00Z"}  # State for partition "1"

    mock_cursor_2 = MagicMock()
    mock_cursor_2.stream_slices.return_value = iter(
        [
            {"slice2": "data2"},
            {"slice2": "data2"},  # Second partition slices
        ]
    )
    mock_cursor_2.state = {"updated_at": "2024-01-01T00:00:00Z"}  # State for partition "2"

    # Configure cursor factory to return different mock cursors based on partition
    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.side_effect = [mock_cursor_1, mock_cursor_2]

    connector_state_converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={
            "states": [
                {"partition": {"id": "1"}, "cursor": {"updated_at": "2024-01-01T00:00:00Z"}}
            ],
            "state": {"updated_at": "2024-01-01T00:00:00Z"},
            "lookback_window": 86400,
            "parent_state": {"posts": {"updated_at": "2024-01-01T00:00:00Z"}},
        },
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=connector_state_converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )
    partition_router = cursor._partition_router
    all_partitions = [
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "2"}, cursor_slice={}, extra_fields={}),  # New partition
    ]
    partition_router.stream_slices.return_value = iter(all_partitions)
    partition_router.get_stream_state.side_effect = [
        {"posts": {"updated_at": "2024-01-04T00:00:00Z"}},  # Initial parent state
        {"posts": {"updated_at": "2024-01-05T00:00:00Z"}},  # Updated parent state for new partition
    ]

    slices = list(cursor.stream_slices())
    # Close all partitions except from the first one
    for slice in slices[:-1]:
        cursor.close_partition(
            DeclarativePartition(
                stream_name="test_stream",
                schema_loader=_EMPTY_SCHEMA_LOADER,
                retriever=MagicMock(),
                message_repository=MagicMock(),
                max_records_limit=None,
                stream_slice=slice,
                record_counter=RecordCounter(),
            )
        )
    cursor.ensure_at_least_one_state_emitted()

    state = cursor.state
    assert state == {
        "use_global_cursor": False,
        "states": [
            {"partition": {"id": "1"}, "cursor": {"updated_at": "2024-01-02T00:00:00Z"}},
            {"partition": {"id": "2"}, "cursor": {"updated_at": "2024-01-01T00:00:00Z"}},
        ],
        "state": {"updated_at": "2024-01-01T00:00:00Z"},
        "lookback_window": 86400,
        "parent_state": {"posts": {"updated_at": "2024-01-04T00:00:00Z"}},
    }
    assert mock_cursor_1.stream_slices.call_count == 1  # Called once for each partition
    assert mock_cursor_2.stream_slices.call_count == 1  # Called once for each partition

    assert len(cursor._semaphore_per_partition) == 1
    assert len(cursor._partitions_done_generating_stream_slices) == 1
    assert len(cursor._processing_partitions_indexes) == 1
    assert len(cursor._partition_key_to_index) == 1


def test_given_all_partitions_finished_when_close_partition_then_final_state_emitted():
    mock_cursor = MagicMock()
    # Simulate one slice per cursor
    mock_cursor.stream_slices.side_effect = [
        iter(
            [
                {"slice1": "data"},  # First slice for partition 1
            ]
        ),
        iter(
            [
                {"slice2": "data"},  # First slice for partition 2
            ]
        ),
    ]
    mock_cursor.state = {"updated_at": "2024-01-02T00:00:00Z"}  # Set cursor state (latest)

    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.return_value = mock_cursor

    connector_state_converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={
            "states": [
                {"partition": {"id": "1"}, "cursor": {"updated_at": "2024-01-01T00:00:00Z"}},
                {"partition": {"id": "2"}, "cursor": {"updated_at": "2024-01-02T00:00:00Z"}},
            ],
            "state": {"updated_at": "2024-01-02T00:00:00Z"},
            "lookback_window": 86400,
            "parent_state": {"posts": {"updated_at": "2024-01-03T00:00:00Z"}},
        },
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=connector_state_converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )
    partition_router = cursor._partition_router
    partitions = [
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "2"}, cursor_slice={}, extra_fields={}),
    ]
    partition_router.stream_slices.return_value = iter(partitions)
    partition_router.get_stream_state.return_value = {
        "posts": {"updated_at": "2024-01-06T00:00:00Z"}
    }

    slices = list(cursor.stream_slices())
    for slice in slices:
        cursor.close_partition(
            DeclarativePartition(
                stream_name="test_stream",
                schema_loader=_EMPTY_SCHEMA_LOADER,
                retriever=MagicMock(),
                message_repository=MagicMock(),
                max_records_limit=None,
                stream_slice=slice,
                record_counter=RecordCounter(),
            )
        )

    cursor.ensure_at_least_one_state_emitted()

    final_state = cursor.state
    assert final_state["use_global_cursor"] is False
    assert len(final_state["states"]) == 2
    assert final_state["state"]["updated_at"] == "2024-01-02T00:00:00Z"
    assert final_state["parent_state"] == {"posts": {"updated_at": "2024-01-06T00:00:00Z"}}
    assert final_state["lookback_window"] == 86400
    assert cursor._message_repository.emit_message.call_count == 2
    assert mock_cursor.stream_slices.call_count == 2  # Called once for each partition

    # Checks that all internal variables are cleaned up
    assert len(cursor._semaphore_per_partition) == 0
    assert len(cursor._partitions_done_generating_stream_slices) == 0
    assert len(cursor._processing_partitions_indexes) == 0
    assert len(cursor._partition_key_to_index) == 0


def test_given_partition_limit_exceeded_when_close_partition_then_switch_to_global_cursor():
    mock_cursor = MagicMock()
    # Simulate one slice per cursor
    mock_cursor.stream_slices.side_effect = [iter([{"slice" + str(i): "data"}]) for i in range(3)]
    mock_cursor.state = {"updated_at": "2024-01-01T00:00:00Z"}  # Set cursor state

    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.return_value = mock_cursor

    connector_state_converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=connector_state_converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )
    # Override default limit for testing
    cursor.DEFAULT_MAX_PARTITIONS_NUMBER = 2
    cursor.SWITCH_TO_GLOBAL_LIMIT = 1

    partition_router = cursor._partition_router
    partitions = [
        StreamSlice(partition={"id": str(i)}, cursor_slice={}, extra_fields={}) for i in range(3)
    ]  # 3 partitions
    partition_router.stream_slices.return_value = iter(partitions)
    partition_router.get_stream_state.side_effect = [
        {"updated_at": "2024-01-02T00:00:00Z"},
        {"updated_at": "2024-01-03T00:00:00Z"},
        {"updated_at": "2024-01-04T00:00:00Z"},
        {"updated_at": "2024-01-04T00:00:00Z"},
    ]

    slices = list(cursor.stream_slices())
    for slice in slices:
        cursor.close_partition(
            DeclarativePartition(
                stream_name="test_stream",
                schema_loader=_EMPTY_SCHEMA_LOADER,
                retriever=MagicMock(),
                message_repository=MagicMock(),
                max_records_limit=None,
                stream_slice=slice,
                record_counter=RecordCounter(),
            )
        )
    cursor.ensure_at_least_one_state_emitted()

    final_state = cursor.state
    assert len(slices) == 3
    assert final_state["use_global_cursor"] is True
    assert len(final_state.get("states", [])) == 0  # No per-partition states
    assert final_state["parent_state"] == {"updated_at": "2024-01-04T00:00:00Z"}
    assert "lookback_window" in final_state
    assert len(cursor._cursor_per_partition) <= cursor.DEFAULT_MAX_PARTITIONS_NUMBER
    assert mock_cursor.stream_slices.call_count == 3  # Called once for each partition


def test_semaphore_cleanup():
    # Create two mock cursors with different states for each partition
    mock_cursor_1 = MagicMock()
    mock_cursor_1.stream_slices.return_value = iter(
        [
            {"slice1": "data1"},
            {"slice2": "data1"},  # First partition slices
        ]
    )
    mock_cursor_1.state = {"updated_at": "2024-01-02T00:00:00Z"}  # State for partition "1"

    mock_cursor_2 = MagicMock()
    mock_cursor_2.stream_slices.return_value = iter(
        [
            {"slice2": "data2"},
            {"slice2": "data2"},  # Second partition slices
        ]
    )
    mock_cursor_2.state = {"updated_at": "2024-01-03T00:00:00Z"}  # State for partition "2"

    # Configure cursor factory to return different mock cursors based on partition
    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.side_effect = [mock_cursor_1, mock_cursor_2]

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=MagicMock(),
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )

    # Simulate partitions with unique parent states
    slices = [
        StreamSlice(partition={"id": "1"}, cursor_slice={}),
        StreamSlice(partition={"id": "2"}, cursor_slice={}),
    ]
    cursor._partition_router.stream_slices.return_value = iter(slices)
    # Simulate unique parent states for each partition
    cursor._partition_router.get_stream_state.side_effect = [
        {"parent": {"state": "state1"}},  # Parent state for partition "1"
        {"parent": {"state": "state2"}},  # Parent state for partition "2"
    ]

    # Generate slices to populate semaphores and parent states
    generated_slices = list(
        cursor.stream_slices()
    )  # Populate _semaphore_per_partition and _partition_parent_state_map

    # Verify initial state
    assert len(cursor._semaphore_per_partition) == 2
    assert len(cursor._partition_parent_state_map) == 2
    assert len(cursor._processing_partitions_indexes) == 2
    assert len(cursor._partition_key_to_index) == 2
    assert cursor._partition_parent_state_map['{"id":"1"}'][0] == {"parent": {"state": "state1"}}
    assert cursor._partition_parent_state_map['{"id":"2"}'][0] == {"parent": {"state": "state2"}}

    # Close partitions to acquire semaphores (value back to 0)
    for s in generated_slices:
        cursor.close_partition(
            DeclarativePartition(
                stream_name="test_stream",
                schema_loader=_EMPTY_SCHEMA_LOADER,
                retriever=MagicMock(),
                message_repository=MagicMock(),
                max_records_limit=None,
                stream_slice=s,
                record_counter=RecordCounter(),
            )
        )

    # Check state after closing partitions
    assert len(cursor._partitions_done_generating_stream_slices) == 0
    assert len(cursor._semaphore_per_partition) == 0
    assert len(cursor._processing_partitions_indexes) == 0
    assert len(cursor._partition_key_to_index) == 0
    assert len(cursor._partition_parent_state_map) == 0  # All parent states should be popped
    assert cursor._parent_state == {"parent": {"state": "state2"}}  # Last parent state


def test_given_global_state_when_read_then_state_is_not_per_partition() -> None:
    manifest = deepcopy(SUBSTREAM_MANIFEST)
    manifest["definitions"]["post_comments_stream"]["incremental_sync"][
        "global_substream_cursor"
    ] = True
    manifest["streams"].remove({"$ref": "#/definitions/post_comment_votes_stream"})
    record = {
        "id": 9,
        "post_id": 1,
        "updated_at": COMMENT_10_UPDATED_AT,
    }
    mock_requests = [
        (
            f"https://api.example.com/community/posts?per_page=100&start_time={START_DATE}",
            {
                "posts": [
                    {"id": 1, "updated_at": POST_1_UPDATED_AT},
                ],
            },
        ),
        # Fetch the first page of comments for post 1
        (
            "https://api.example.com/community/posts/1/comments?per_page=100",
            {
                "comments": [record],
            },
        ),
    ]

    run_mocked_test(
        mock_requests,
        manifest,
        CONFIG,
        "post_comments",
        {},
        [record],
        {
            "lookback_window": 1,
            "parent_state": {"posts": {"updated_at": "2024-01-30T00:00:00Z"}},
            "state": {"updated_at": "2024-01-25T00:00:00Z"},
            "use_global_cursor": True,  # ensures that it is running the Concurrent CDK version as this is not populated in the declarative implementation
        },  # this state does have per partition which would be under `states`
    )


def _make_inner_cursor(ts: str) -> MagicMock:
    """Return an inner cursor that yields exactly one slice and has a proper state."""
    inner = MagicMock()
    inner.stream_slices.side_effect = lambda: iter([{"dummy": "slice"}])
    inner.state = {"updated_at": ts}
    inner.close_partition.return_value = None
    inner.observe.return_value = None
    return inner


def test_duplicate_partition_after_closing_partition_cursor_deleted():
    inner_cursors = [
        _make_inner_cursor("2024-01-01T00:00:00Z"),  # for first "1"
        _make_inner_cursor("2024-01-02T00:00:00Z"),  # for "2"
        _make_inner_cursor("2024-01-03T00:00:00Z"),  # for second "1"
    ]
    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.side_effect = inner_cursors

    converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="dup_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )

    cursor.DEFAULT_MAX_PARTITIONS_NUMBER = 1

    # ── Partition sequence: 1 → 2 → 1 ──────────────────────────────────
    partitions = [
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "2"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
    ]
    pr = cursor._partition_router
    pr.stream_slices.return_value = iter(partitions)
    pr.get_stream_state.return_value = {}

    # Iterate lazily so that the first "1" gets cleaned before
    # the second "1" arrives.
    slice_gen = cursor.stream_slices()

    first_1 = next(slice_gen)
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=first_1,
            record_counter=RecordCounter(),
        )
    )

    two = next(slice_gen)
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=two,
            record_counter=RecordCounter(),
        )
    )

    second_1 = next(slice_gen)
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=second_1,
            record_counter=RecordCounter(),
        )
    )

    assert cursor._IS_PARTITION_DUPLICATION_LOGGED is False  # No duplicate detected
    assert len(cursor._semaphore_per_partition) == 0
    assert len(cursor._processing_partitions_indexes) == 0
    assert len(cursor._partition_key_to_index) == 0
    assert len(cursor._partitions_done_generating_stream_slices) == 0


def test_duplicate_partition_after_closing_partition_cursor_exists():
    inner_cursors = [
        _make_inner_cursor("2024-01-01T00:00:00Z"),  # for first "1"
        _make_inner_cursor("2024-01-02T00:00:00Z"),  # for "2"
        _make_inner_cursor("2024-01-03T00:00:00Z"),  # for second "1"
    ]
    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.side_effect = inner_cursors

    converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory_mock,
        partition_router=MagicMock(),
        stream_name="dup_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )

    # ── Partition sequence: 1 → 2 → 1 ──────────────────────────────────
    partitions = [
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "2"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
    ]
    pr = cursor._partition_router
    pr.stream_slices.return_value = iter(partitions)
    pr.get_stream_state.return_value = {}

    # Iterate lazily so that the first "1" gets cleaned before
    # the second "1" arrives.
    slice_gen = cursor.stream_slices()

    first_1 = next(slice_gen)
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=first_1,
            record_counter=RecordCounter(),
        )
    )

    two = next(slice_gen)
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=two,
            record_counter=RecordCounter(),
        )
    )

    # Second “1” should appear because the semaphore was cleaned up
    second_1 = next(slice_gen)
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=second_1,
            record_counter=RecordCounter(),
        )
    )

    with pytest.raises(StopIteration):
        next(slice_gen)

    assert cursor._IS_PARTITION_DUPLICATION_LOGGED is False  # no duplicate warning
    assert len(cursor._cursor_per_partition) == 2  # only “1” & “2” kept
    assert len(cursor._semaphore_per_partition) == 0  # all semaphores cleaned
    assert len(cursor._processing_partitions_indexes) == 0
    assert len(cursor._partition_key_to_index) == 0
    assert len(cursor._partitions_done_generating_stream_slices) == 0


def test_duplicate_partition_while_processing():
    inner_cursors = [
        _make_inner_cursor("2024-01-01T00:00:00Z"),  # first “1”
        _make_inner_cursor("2024-01-02T00:00:00Z"),  # “2”
        _make_inner_cursor("2024-01-03T00:00:00Z"),  # for second "1"
    ]

    factory = MagicMock()
    factory.create.side_effect = inner_cursors

    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=factory,
        partition_router=MagicMock(),
        stream_name="dup_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=MagicMock(),
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )

    partitions = [
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "2"}, cursor_slice={}, extra_fields={}),
        StreamSlice(partition={"id": "1"}, cursor_slice={}, extra_fields={}),
    ]
    pr = cursor._partition_router
    pr.stream_slices.return_value = iter(partitions)
    pr.get_stream_state.return_value = {}

    generated = list(cursor.stream_slices())
    # Only “1” and “2” emitted – duplicate “1” skipped
    assert len(generated) == 2

    # Close “2” first
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=generated[1],
            record_counter=RecordCounter(),
        )
    )
    # Now close the initial “1”
    cursor.close_partition(
        DeclarativePartition(
            stream_name="dup_stream",
            schema_loader=_EMPTY_SCHEMA_LOADER,
            retriever=MagicMock(),
            message_repository=MagicMock(),
            max_records_limit=None,
            stream_slice=generated[0],
            record_counter=RecordCounter(),
        )
    )

    assert cursor._IS_PARTITION_DUPLICATION_LOGGED is True  # warning emitted
    assert len(cursor._cursor_per_partition) == 2
    assert len(cursor._semaphore_per_partition) == 0
    assert len(cursor._processing_partitions_indexes) == 0
    assert len(cursor._partition_key_to_index) == 0
    assert len(cursor._partitions_done_generating_stream_slices) == 0


def test_given_record_with_bad_cursor_value_the_global_state_parsing_does_not_break_sync():
    cursor_factory_mock = MagicMock()
    cursor_factory_mock.create.side_effect = [_make_inner_cursor("2024-01-01T00:00:00Z")]
    cursor = ConcurrentPerPartitionCursor(
        cursor_factory=MagicMock(),
        partition_router=ListPartitionRouter(
            values=["1"], cursor_field="partition_id", config={}, parameters={}
        ),
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=MagicMock(),
        connector_state_manager=MagicMock(),
        connector_state_converter=CustomFormatConcurrentStreamStateConverter(
            datetime_format="%Y-%m-%dT%H:%M:%SZ",
            input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
            is_sequential_state=True,
            cursor_granularity=timedelta(0),
        ),
        cursor_field=CursorField(cursor_field_key="updated_at"),
    )

    cursor.observe(
        Record(
            data={"updated_at": ""},
            stream_name="test_stream",
            associated_slice=StreamSlice(partition={"partition_id": "1"}, cursor_slice={}),
        )
    )
