# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import copy
from copy import deepcopy
from typing import Any, List, Mapping, MutableMapping, Optional, Union

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
                        }
                    ],
                },
            },
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

        output = _run_read(manifest, config, stream_name, initial_state)

        # Verify records
        assert sorted(
            [r.record.data for r in output.records], key=lambda x: orjson.dumps(x)
        ) == sorted(expected_records, key=lambda x: orjson.dumps(x))

        # Verify state
        final_state = output.state_messages[-1].state.stream.stream_state
        assert final_state == AirbyteStateBlob(expected_state)


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


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST_NO_DEPENDENCY,
            [
                # Fetch the first page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {
                        "posts": [
                            {"id": 1, "updated_at": "2024-01-30T00:00:00Z"},
                            {"id": 2, "updated_at": "2024-01-29T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-01T00:00:01Z&page=2",
                    {"posts": [{"id": 3, "updated_at": "2024-01-28T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": "2024-01-23T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {
                        "votes": [
                            {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 200, "comment_id": 20, "created_at": "2024-01-12T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"}
                        ]
                    },
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    "https://api.example.com/community/posts/3/comments/30/votes?per_page=100",
                    {
                        "votes": [
                            {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"}
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"},
                {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"},
                {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"},
                {"id": 200, "comment_id": 20, "created_at": "2024-01-12T00:00:00Z"},
                {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"},
                {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"},
            ],
            # Initial state
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(
                            name="post_comment_votes", namespace=None
                        ),
                        stream_state=AirbyteStateBlob(
                            {
                                # This should not happen since parent state is disabled, but I've added this to validate that and
                                # incoming parent_state is ignored when the parent stream's incremental_dependency is disabled
                                "parent_state": {
                                    "post_comments": {
                                        "states": [
                                            {
                                                "partition": {"id": 1, "parent_slice": {}},
                                                "cursor": {"updated_at": "2023-01-04T00:00:00Z"},
                                            }
                                        ],
                                        "parent_state": {
                                            "posts": {"updated_at": "2024-01-05T00:00:00Z"}
                                        },
                                    }
                                },
                                "states": [
                                    {
                                        "partition": {
                                            "id": 10,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                                    },
                                    {
                                        "partition": {
                                            "id": 11,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                                    },
                                ],
                                "lookback_window": 86400,
                            }
                        ),
                    ),
                )
            ],
            # Expected state
            {
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-15T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-13T00:00:00Z"},
                    },
                    {
                        "cursor": {"created_at": "2024-01-01T00:00:01Z"},
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-12T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-12T00:00:15Z"},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-10T00:00:00Z"},
                    },
                ],
                "lookback_window": 1,
                "parent_state": {},
                "state": {"created_at": "2024-01-15T00:00:00Z"},
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
    - post comment votes: (parent comment 10 with ids: 100, 101; parent comment 11 with id: 102;
      parent comment 20 with id: 200; parent comment 21 with id: 201, parent comment 30 with id: 300)

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
    manifest, mock_requests, expected_records, initial_state, expected_states
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
        initial_state (list): The initial state to start the read operation.
        expected_states (list): A list of expected final states after the read operation.
    """
    _stream_name = "post_comment_votes"
    config = {
        "start_date": "2024-01-01T00:00:01Z",
        "credentials": {"email": "email", "api_token": "api_token"},
    }
    expected_states = [AirbyteStateBlob(s) for s in expected_states]

    with requests_mock.Mocker() as m:
        for url, response in mock_requests:
            m.get(url, json=response)

        # Run the initial read
        output = _run_read(manifest, config, _stream_name, initial_state)

        # Assert that output_data equals expected_records
        assert sorted(
            [r.record.data for r in output.records], key=lambda x: orjson.dumps(x)
        ) == sorted(expected_records, key=lambda x: orjson.dumps(x))

        # Collect the intermediate states and records produced before each state
        cumulative_records = []
        intermediate_states = []
        final_states = []  # To store the final state after each read

        # Store the final state after the initial read
        final_states.append(output.state_messages[-1].state.stream.stream_state)

        for message in output.records_and_state_messages:
            if message.type.value == "RECORD":
                record_data = message.record.data
                cumulative_records.append(record_data)
            elif message.type.value == "STATE":
                # Record the state and the records produced before this state
                state = message.state
                records_before_state = cumulative_records.copy()
                intermediate_states.append((state, records_before_state))

        # For each intermediate state, perform another read starting from that state
        for state, records_before_state in intermediate_states[:-1]:
            output_intermediate = _run_read(manifest, config, _stream_name, [state])
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
                sorted(cumulative_records_state_deduped, key=lambda x: orjson.dumps(x))
                == sorted(expected_records_set, key=lambda x: orjson.dumps(x))
            ), f"Records mismatch with intermediate state {state}. Expected {expected_records}, got {cumulative_records_state_deduped}"

            # Store the final state after each intermediate read
            final_state_intermediate = [
                orjson.loads(orjson.dumps(message.state.stream.stream_state))
                for message in output_intermediate.state_messages
            ]
            final_states.append(final_state_intermediate[-1])

        # Assert that the final state matches the expected state for all runs
        for i, final_state in enumerate(final_states):
            assert (
                final_state in expected_states
            ), f"Final state mismatch at run {i + 1}. Expected {expected_states}, got {final_state}"


@pytest.mark.parametrize(
    "test_name, manifest, mock_requests, expected_records, initial_state, expected_state",
    [
        (
            "test_incremental_parent_state",
            SUBSTREAM_MANIFEST,
            [
                # Fetch the first page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z",
                    {
                        "posts": [
                            {"id": 1, "updated_at": "2024-01-30T00:00:00Z"},
                            {"id": 2, "updated_at": "2024-01-29T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    {"posts": [{"id": 3, "updated_at": "2024-01-28T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": "2024-01-23T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {
                        "votes": [
                            {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 200, "comment_id": 20, "created_at": "2024-01-12T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"}
                        ]
                    },
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    "https://api.example.com/community/posts/3/comments/30/votes?per_page=100",
                    {
                        "votes": [
                            {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"}
                        ]
                    },
                ),
                # Requests with intermediate states
                # Fetch votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time=2024-01-15T00:00:00Z",
                    {
                        "votes": [
                            {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"}
                        ],
                    },
                ),
                # Fetch votes for comment 11 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time=2024-01-13T00:00:00Z",
                    {
                        "votes": [
                            {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"}
                        ],
                    },
                ),
                # Fetch votes for comment 12 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time=2024-01-15T00:00:00Z",
                    {
                        "votes": [],
                    },
                ),
                # Fetch votes for comment 20 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time=2024-01-12T00:00:00Z",
                    {
                        "votes": [
                            {"id": 200, "comment_id": 20, "created_at": "2024-01-12T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch votes for comment 21 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time=2024-01-12T00:00:15Z",
                    {
                        "votes": [
                            {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"}
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"},
                {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"},
                {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"},
                {"id": 200, "comment_id": 20, "created_at": "2024-01-12T00:00:00Z"},
                {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"},
                {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"},
            ],
            # Initial state
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(
                            name="post_comment_votes", namespace=None
                        ),
                        stream_state=AirbyteStateBlob(
                            {
                                "parent_state": {
                                    "post_comments": {
                                        "states": [
                                            {
                                                "partition": {"id": 1, "parent_slice": {}},
                                                "cursor": {"updated_at": "2023-01-04T00:00:00Z"},
                                            }
                                        ],
                                        "parent_state": {
                                            "posts": {"updated_at": "2024-01-05T00:00:00Z"}
                                        },
                                    }
                                },
                                "states": [
                                    {
                                        "partition": {
                                            "id": 10,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                                    },
                                    {
                                        "partition": {
                                            "id": 11,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                                    },
                                ],
                                "lookback_window": 86400,
                            }
                        ),
                    ),
                )
            ],
            # Expected state
            {
                "state": {"created_at": "2024-01-15T00:00:00Z"},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": "2024-01-25T00:00:00Z"},
                        "parent_state": {"posts": {"updated_at": "2024-01-30T00:00:00Z"}},
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-25T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-22T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 3, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-09T00:00:00Z"},
                            },
                        ],
                    }
                },
                "lookback_window": 1,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-15T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-13T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-01T00:00:01Z"},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-12T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-12T00:00:15Z"},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-10T00:00:00Z"},
                    },
                ],
            },
        ),
    ],
)
def test_incremental_parent_state(
    test_name, manifest, mock_requests, expected_records, initial_state, expected_state
):
    additional_expected_state = copy.deepcopy(expected_state)
    # State for empty partition (comment 12), when the global cursor is used for intermediate states
    empty_state = {
        "cursor": {"created_at": "2024-01-01T00:00:01Z"},
        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
    }
    additional_expected_state["states"].append(empty_state)
    run_incremental_parent_state_test(
        manifest,
        mock_requests,
        expected_records,
        initial_state,
        [expected_state, additional_expected_state],
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
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "posts": [
                            {"id": 1, "updated_at": "2024-01-30T00:00:00Z"},
                            {"id": 2, "updated_at": "2024-01-29T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts?per_page=100&start_time=2024-01-02T00:00:00Z&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-02T00:00:00Z&page=2",
                    {"posts": [{"id": 3, "updated_at": "2024-01-28T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": "2024-01-23T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-02T00:00:00Z",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 200, "comment_id": 20, "created_at": "2024-01-12T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"}
                        ]
                    },
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    "https://api.example.com/community/posts/3/comments/30/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"}
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"},
                {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"},
                {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"},
                {"id": 200, "comment_id": 20, "created_at": "2024-01-12T00:00:00Z"},
                {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"},
                {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"},
            ],
            # Initial state
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(
                            name="post_comment_votes", namespace=None
                        ),
                        stream_state=AirbyteStateBlob({"created_at": "2024-01-02T00:00:00Z"}),
                    ),
                )
            ],
            # Expected state
            {
                "state": {"created_at": "2024-01-15T00:00:00Z"},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": "2024-01-25T00:00:00Z"},
                        "parent_state": {"posts": {"updated_at": "2024-01-30T00:00:00Z"}},
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-25T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-22T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 3, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-09T00:00:00Z"},
                            },
                        ],
                    }
                },
                "lookback_window": 1,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-15T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-13T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-12T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-12T00:00:15Z"},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-10T00:00:00Z"},
                    },
                ],
            },
        ),
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
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z",
                    {
                        "posts": [],
                        "next_page": "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    {"posts": []},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": []},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [],
                        "next_page": "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": []},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": []},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    "https://api.example.com/community/posts/3/comments/30/votes?per_page=100",
                    {"votes": []},
                ),
            ],
            # Expected records
            [],
            # Initial state
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(
                            name="post_comment_votes", namespace=None
                        ),
                        stream_state=AirbyteStateBlob(
                            {
                                "parent_state": {
                                    "post_comments": {
                                        "states": [
                                            {
                                                "partition": {"id": 1, "parent_slice": {}},
                                                "cursor": {"updated_at": "2023-01-04T00:00:00Z"},
                                            }
                                        ],
                                        "parent_state": {
                                            "posts": {"updated_at": "2024-01-05T00:00:00Z"}
                                        },
                                    }
                                },
                                "states": [
                                    {
                                        "partition": {
                                            "id": 10,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                                    },
                                    {
                                        "partition": {
                                            "id": 11,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                                    },
                                ],
                                "state": {"created_at": "2024-01-03T00:00:00Z"},
                                "lookback_window": 1,
                            }
                        ),
                    ),
                )
            ],
            # Expected state
            {
                "lookback_window": 1,
                "state": {"created_at": "2024-01-03T00:00:00Z"},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {},
                        "parent_state": {"posts": {"updated_at": "2024-01-05T00:00:00Z"}},
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": "2023-01-04T00:00:00Z"},
                            }
                        ],
                    }
                },
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                    },
                ],
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
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z",
                    {
                        "posts": [
                            {"id": 1, "updated_at": "2024-01-30T00:00:00Z"},
                            {"id": 2, "updated_at": "2024-01-29T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    {"posts": [{"id": 3, "updated_at": "2024-01-28T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": "2024-01-23T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [],
                        "next_page": "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {"votes": []},
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    "https://api.example.com/community/posts/3/comments/30/votes?per_page=100",
                    {"votes": []},
                ),
            ],
            # Expected records
            [],
            # Initial state
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(
                            name="post_comment_votes", namespace=None
                        ),
                        stream_state=AirbyteStateBlob(
                            {
                                "parent_state": {
                                    "post_comments": {
                                        "states": [
                                            {
                                                "partition": {"id": 1, "parent_slice": {}},
                                                "cursor": {"updated_at": "2023-01-04T00:00:00Z"},
                                            }
                                        ],
                                        "parent_state": {
                                            "posts": {"updated_at": "2024-01-05T00:00:00Z"}
                                        },
                                    }
                                },
                                "states": [
                                    {
                                        "partition": {
                                            "id": 10,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                                    },
                                    {
                                        "partition": {
                                            "id": 11,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                                    },
                                ],
                                "use_global_cursor": True,
                                "state": {"created_at": "2024-01-03T00:00:00Z"},
                                "lookback_window": 0,
                            }
                        ),
                    ),
                )
            ],
            # Expected state
            {
                "lookback_window": 1,
                "state": {"created_at": "2024-01-03T00:00:00Z"},
                "states": [
                    {
                        "partition": {
                            "id": 10,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                    },
                    {
                        "partition": {
                            "id": 11,
                            "parent_slice": {"id": 1, "parent_slice": {}},
                        },
                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                    },
                    {
                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                    },
                    {
                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                    },
                    {
                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                    },
                    {
                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                    },
                ],
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": "2024-01-25T00:00:00Z"},
                        "parent_state": {"posts": {"updated_at": "2024-01-30T00:00:00Z"}},
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-25T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-22T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 3, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-09T00:00:00Z"},
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
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z",
                    {
                        "posts": [
                            {"id": 1, "updated_at": "2024-01-30T00:00:00Z"},
                            {"id": 2, "updated_at": "2024-01-29T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    },
                ),
                # Fetch the second page of posts
                (
                    "https://api.example.com/community/posts?per_page=100&start_time=2024-01-05T00:00:00Z&page=2",
                    {"posts": [{"id": 3, "updated_at": "2024-01-28T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": "2024-01-23T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&start_time=2024-01-02T00:00:00Z",
                    {
                        "votes": [
                            {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    },
                ),
                # Fetch the second page of votes for comment 10 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/10/votes?per_page=100&page=2&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 11 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/11/votes?per_page=100&start_time=2024-01-03T00:00:00Z",
                    {
                        "votes": [
                            {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"}
                        ]
                    },
                ),
                # Fetch the first page of votes for comment 12 of post 1
                (
                    "https://api.example.com/community/posts/1/comments/12/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {"votes": []},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 20 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/20/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {
                        "status_code": 500,
                        "json": {"error": "Internal Server Error"},
                    },
                ),
                # Fetch the first page of votes for comment 21 of post 2
                (
                    "https://api.example.com/community/posts/2/comments/21/votes?per_page=100&start_time=2024-01-01T00:00:01Z",
                    {
                        "votes": [
                            {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"}
                        ]
                    },
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
                    {"comments": [{"id": 30, "post_id": 3, "updated_at": "2024-01-09T00:00:00Z"}]},
                ),
                # Fetch the first page of votes for comment 30 of post 3
                (
                    "https://api.example.com/community/posts/3/comments/30/votes?per_page=100",
                    {
                        "votes": [
                            {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"}
                        ]
                    },
                ),
            ],
            # Expected records
            [
                {"id": 100, "comment_id": 10, "created_at": "2024-01-15T00:00:00Z"},
                {"id": 101, "comment_id": 10, "created_at": "2024-01-14T00:00:00Z"},
                {"id": 102, "comment_id": 11, "created_at": "2024-01-13T00:00:00Z"},
                {"id": 201, "comment_id": 21, "created_at": "2024-01-12T00:00:15Z"},
                {"id": 300, "comment_id": 30, "created_at": "2024-01-10T00:00:00Z"},
            ],
            # Initial state
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(
                            name="post_comment_votes", namespace=None
                        ),
                        stream_state=AirbyteStateBlob(
                            {
                                "parent_state": {
                                    "post_comments": {
                                        "states": [
                                            {
                                                "partition": {"id": 1, "parent_slice": {}},
                                                "cursor": {"updated_at": "2023-01-04T00:00:00Z"},
                                            }
                                        ],
                                        "parent_state": {
                                            "posts": {"updated_at": "2024-01-05T00:00:00Z"}
                                        },
                                    }
                                },
                                "states": [
                                    {
                                        "partition": {
                                            "id": 10,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-02T00:00:00Z"},
                                    },
                                    {
                                        "partition": {
                                            "id": 11,
                                            "parent_slice": {"id": 1, "parent_slice": {}},
                                        },
                                        "cursor": {"created_at": "2024-01-03T00:00:00Z"},
                                    },
                                ],
                            }
                        ),
                    ),
                )
            ],
            # Expected state
            {
                "state": {"created_at": "2024-01-15T00:00:00Z"},
                "parent_state": {
                    "post_comments": {
                        "use_global_cursor": False,
                        "state": {"updated_at": "2024-01-25T00:00:00Z"},
                        "parent_state": {"posts": {"updated_at": "2024-01-30T00:00:00Z"}},
                        "lookback_window": 1,
                        "states": [
                            {
                                "partition": {"id": 1, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-25T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 2, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-22T00:00:00Z"},
                            },
                            {
                                "partition": {"id": 3, "parent_slice": {}},
                                "cursor": {"updated_at": "2024-01-09T00:00:00Z"},
                            },
                        ],
                    }
                },
                "lookback_window": 1,
                "states": [
                    {
                        "partition": {"id": 10, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-15T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 11, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-13T00:00:00Z"},
                    },
                    {
                        "partition": {"id": 12, "parent_slice": {"id": 1, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-01T00:00:01Z"},
                    },
                    {
                        "partition": {"id": 20, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-01T00:00:01Z"},
                    },
                    {
                        "partition": {"id": 21, "parent_slice": {"id": 2, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-12T00:00:15Z"},
                    },
                    {
                        "partition": {"id": 30, "parent_slice": {"id": 3, "parent_slice": {}}},
                        "cursor": {"created_at": "2024-01-10T00:00:00Z"},
                    },
                ],
            },
        ),
    ],
)
def test_incremental_error__parent_state(
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
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    {"comments": [{"id": 12, "post_id": 1, "updated_at": "2024-01-23T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
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
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(name="post_comments", namespace=None),
                        stream_state=AirbyteStateBlob(
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
                            }
                        ),
                    ),
                )
            ],
            # Expected state
            {
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
                    "https://api.example.com/community/posts/1/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 9, "post_id": 1, "updated_at": "2023-01-01T00:00:00Z"},
                            {"id": 10, "post_id": 1, "updated_at": "2024-01-25T00:00:00Z"},
                            {"id": 11, "post_id": 1, "updated_at": "2024-01-24T00:00:00Z"},
                        ],
                        "next_page": "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    },
                ),
                # Error response for the second page of comments for post 1
                (
                    "https://api.example.com/community/posts/1/comments?per_page=100&page=2",
                    None,  # Simulate a network error or an empty response
                ),
                # Fetch the first page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100",
                    {
                        "comments": [
                            {"id": 20, "post_id": 2, "updated_at": "2024-01-22T00:00:00Z"}
                        ],
                        "next_page": "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    },
                ),
                # Fetch the second page of comments for post 2
                (
                    "https://api.example.com/community/posts/2/comments?per_page=100&page=2",
                    {"comments": [{"id": 21, "post_id": 2, "updated_at": "2024-01-21T00:00:00Z"}]},
                ),
                # Fetch the first page of comments for post 3
                (
                    "https://api.example.com/community/posts/3/comments?per_page=100",
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
            [
                AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(name="post_comments", namespace=None),
                        stream_state=AirbyteStateBlob(
                            {
                                "state": {"updated_at": "2024-01-08T00:00:00Z"},
                                "states": [
                                    {
                                        "cursor": {"updated_at": "2024-01-20T00:00:00Z"},
                                        "partition": {"id": "1"},
                                    },
                                    {
                                        "cursor": {"updated_at": "2024-01-22T00:00:00Z"},
                                        "partition": {"id": "2"},
                                    },
                                ],
                                "use_global_cursor": False,
                            }
                        ),
                    ),
                )
            ],
            # Expected state
            {
                "lookback_window": 0,
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
