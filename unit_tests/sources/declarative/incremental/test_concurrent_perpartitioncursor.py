# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
import copy
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, List, Mapping, MutableMapping, Optional, Union
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
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read

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
                        "condition": "{{ record['updated_at'] >= stream_state.get('updated_at', config.get('start_date')) }}"
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
CONFIG = {
    "start_date": "2024-01-01T00:00:01Z",
    "credentials": {"email": "email", "api_token": "api_token"},
}

SUBSTREAM_MANIFEST_NO_DEPENDENCY = deepcopy(SUBSTREAM_MANIFEST)
# Disable incremental_dependency
SUBSTREAM_MANIFEST_NO_DEPENDENCY["definitions"]["post_comments_stream"]["retriever"][
    "partition_router"
]["parent_stream_configs"][0]["incremental_dependency"] = False
SUBSTREAM_MANIFEST_NO_DEPENDENCY["definitions"]["post_comment_votes_stream"]["retriever"][
    "partition_router"
]["parent_stream_configs"][0]["incremental_dependency"] = False

import orjson
import requests_mock


def run_mocked_test(
    mock_requests, manifest, config, stream_name, initial_state, expected_records, expected_state
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

        # Verify state
        final_state = output.state_messages[-1].state.stream.stream_state
        assert final_state.__dict__ == expected_state

        # Verify that each request was made exactly once
        for url, _ in mock_requests:
            request_count = len(
                [req for req in m.request_history if unquote(req.url) == unquote(url)]
            )
            assert (
                request_count == 1
            ), f"URL {url} was called {request_count} times, expected exactly once."


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


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
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
        ),
    ],
)
def test_incremental_parent_state_no_incremental_dependency(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
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
    )


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
            assert (
                sorted(cumulative_records_state_deduped, key=lambda x: x["id"])
                == sorted(expected_records_set, key=lambda x: x["id"])
            ), f"Records mismatch with intermediate state {state}. Expected {expected_records}, got {cumulative_records_state_deduped}"

            # Store the final state after each intermediate read
            final_state_intermediate = [
                message.state.stream.stream_state.__dict__
                for message in output_intermediate.state_messages
            ]
            final_states.append(final_state_intermediate[-1])

        # Assert that the final state matches the expected state for all runs
        for i, final_state in enumerate(final_states):
            assert (
                final_state in expected_states
            ), f"Final state mismatch at run {i + 1}. Expected {expected_states}, got {final_state}"


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
                            "https://api.example.com/community/posts/1/comments"
                            "?per_page=100&page=2"
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
                            "https://api.example.com/community/posts/2/comments"
                            "?per_page=100&page=2"
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
    ],
    ids=["legacy_python_format", "low_code_per_partition_state", "low_code_global_format"],
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
                                "cursor": {"updated_at": PARENT_COMMENT_CURSOR_PARTITION_1},
                            }
                        ],
                        "state": {},
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
                "lookback_window": 1,
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
                # The global state, lookback window and the parent state are the same because sync failed for comment 20
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
                        "condition": "{{ record['updated_at'] >= stream_state.get('updated_at', config.get('start_date')) }}"
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
