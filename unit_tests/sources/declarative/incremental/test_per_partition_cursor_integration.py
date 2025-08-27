#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from unittest.mock import MagicMock, patch

import orjson

from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    StreamDescriptor,
    SyncMode,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.incremental import ConcurrentPerPartitionCursor
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.types import Record, StreamSlice

CURSOR_FIELD = "cursor_field"
SYNC_MODE = SyncMode.incremental


class ManifestBuilder:
    def __init__(self):
        self._incremental_sync = {}
        self._partition_router = {}
        self._substream_partition_router = {}

    def with_list_partition_router(self, stream_name, cursor_field, partitions):
        self._partition_router[stream_name] = {
            "type": "ListPartitionRouter",
            "cursor_field": cursor_field,
            "values": partitions,
        }
        return self

    def with_substream_partition_router(self, stream_name):
        self._substream_partition_router[stream_name] = {
            "type": "SubstreamPartitionRouter",
            "parent_stream_configs": [
                {
                    "type": "ParentStreamConfig",
                    "stream": "#/definitions/Rates",
                    "parent_key": "id",
                    "partition_field": "parent_id",
                }
            ],
        }
        return self

    def with_incremental_sync(
        self,
        stream_name,
        start_datetime,
        end_datetime,
        datetime_format,
        cursor_field,
        step,
        cursor_granularity,
    ):
        self._incremental_sync[stream_name] = {
            "type": "DatetimeBasedCursor",
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "datetime_format": datetime_format,
            "cursor_field": cursor_field,
            "step": step,
            "cursor_granularity": cursor_granularity,
        }
        return self

    def build(self):
        manifest = {
            "version": "0.34.2",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["Rates"]},
            "definitions": {
                "AnotherStream": {
                    "type": "DeclarativeStream",
                    "name": "AnotherStream",
                    "primary_key": [],
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "$schema": "http://json-schema.org/schema#",
                            "properties": {"id": {"type": "string"}},
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
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                },
                "Rates": {
                    "type": "DeclarativeStream",
                    "name": "Rates",
                    "primary_key": [],
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "$schema": "http://json-schema.org/schema#",
                            "properties": {},
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
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                },
            },
            "streams": [{"$ref": "#/definitions/Rates"}, {"$ref": "#/definitions/AnotherStream"}],
            "spec": {
                "connection_specification": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "required": [],
                    "properties": {},
                    "additionalProperties": True,
                },
                "documentation_url": "https://example.org",
                "type": "Spec",
            },
        }
        for stream_name, incremental_sync_definition in self._incremental_sync.items():
            manifest["definitions"][stream_name]["incremental_sync"] = incremental_sync_definition
        for stream_name, partition_router_definition in self._partition_router.items():
            manifest["definitions"][stream_name]["retriever"]["partition_router"] = (
                partition_router_definition
            )
        for stream_name, partition_router_definition in self._substream_partition_router.items():
            manifest["definitions"][stream_name]["retriever"]["partition_router"] = (
                partition_router_definition
            )
        return manifest


def test_given_state_for_only_some_partition_when_stream_slices_then_create_slices_using_state_or_start_from_start_datetime():
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="Rates", namespace=None),
                stream_state=AirbyteStateBlob(
                    {
                        "states": [
                            {
                                "partition": {"partition_field": "1"},
                                "cursor": {CURSOR_FIELD: "2022-02-01"},
                            }
                        ]
                    }
                ),
            ),
        )
    ]

    source = ConcurrentDeclarativeSource(
        source_config=ManifestBuilder()
        .with_list_partition_router("Rates", "partition_field", ["1", "2"])
        .with_incremental_sync(
            "Rates",
            start_datetime="2022-01-01",
            end_datetime="2022-02-28",
            datetime_format="%Y-%m-%d",
            cursor_field=CURSOR_FIELD,
            step="P1M",
            cursor_granularity="P1D",
        )
        .build(),
        config={},
        catalog=None,
        state=state,
    )
    stream_instance = source.streams({})[0]

    partitions = stream_instance.generate_partitions()
    slices = [partition.to_slice() for partition in partitions]

    assert slices == [
        StreamSlice(
            partition={"partition_field": "1"},
            cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
        ),
        StreamSlice(
            partition={"partition_field": "2"},
            cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"},
        ),
        StreamSlice(
            partition={"partition_field": "2"},
            cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
        ),
    ]


def test_given_record_for_partition_when_read_then_update_state(caplog):
    configured_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="Rates",
            json_schema={},
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )
    catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

    source = ConcurrentDeclarativeSource(
        source_config=ManifestBuilder()
        .with_list_partition_router("Rates", "partition_field", ["1", "2"])
        .with_incremental_sync(
            "Rates",
            start_datetime="2022-01-01",
            end_datetime="2022-02-28",
            datetime_format="%Y-%m-%d",
            cursor_field=CURSOR_FIELD,
            step="P1M",
            cursor_granularity="P1D",
        )
        .build(),
        config={},
        catalog=catalog,
        state=None,
    )
    logger = MagicMock()

    stream_instance = source.streams({})[0]
    # list(stream_instance.stream_slices(sync_mode=SYNC_MODE))

    stream_slice = [
        StreamSlice(
            partition={"partition_field": "1"},
            cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"},
        ),
        StreamSlice(
            partition={"partition_field": "1"},
            cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
        ),
        StreamSlice(
            partition={"partition_field": "2"},
            cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
        ),
        StreamSlice(
            partition={"partition_field": "2"},
            cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
        ),
    ]

    # with patch.object(
    #     SimpleRetriever,
    #     "_read_pages",
    #     side_effect=[
    #         [Record({"a record key": "a record value", CURSOR_FIELD: "2022-01-15"}, stream_slice)]
    #     ],
    # ):
    #     list(
    #         stream_instance.read(
    #             sync_mode=SYNC_MODE,
    #             stream_slice=stream_slice,
    #             stream_state={"states": []},
    #             cursor_field=CURSOR_FIELD,
    #         )
    #     )

    records = [
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-15"},
                stream_name="Rates",
                associated_slice=stream_slice[0],
            )
        ],
        [],
        [],
        [],
        # [Record(data={"a record key": "a record value", CURSOR_FIELD: "2022-02-15"}, stream_name="Rates", associated_slice=stream_slice[1])],
        # [Record(data={"a record key": "a record value", CURSOR_FIELD: "2022-01-15"}, stream_name="Rates", associated_slice=stream_slice[1])],
        # [Record(data={"a record key": "a record value", CURSOR_FIELD: "2022-02-15"}, stream_name="Rates", associated_slice=stream_slice[1])],
    ]

    # Use caplog to capture logs
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        with patch.object(SimpleRetriever, "_read_pages", side_effect=records):
            output = list(source.read(logger, {}, catalog, None))

    # Since the partition limit is not exceeded, we expect no warnings
    logged_warnings = [record.message for record in caplog.records if record.levelname == "WARNING"]
    assert len(logged_warnings) == 0

    # Proceed with existing assertions
    final_state = [
        orjson.loads(orjson.dumps(message.state.stream.stream_state))
        for message in output
        if message.state
    ]

    assert final_state[-1] == {
        "lookback_window": 1,
        "state": {"cursor_field": "2022-01-15"},
        "use_global_cursor": False,
        "states": [
            {
                "partition": {"partition_field": "1"},
                "cursor": {CURSOR_FIELD: "2022-01-15"},
            },
            {
                "partition": {"partition_field": "2"},
                "cursor": {CURSOR_FIELD: "2022-01-01"},
            },
        ],
    }


def test_substream_without_input_state():
    test_source = ConcurrentDeclarativeSource(
        source_config=ManifestBuilder()
        .with_substream_partition_router("AnotherStream")
        .with_incremental_sync(
            "Rates",
            start_datetime="2022-01-01",
            end_datetime="2022-02-28",
            datetime_format="%Y-%m-%d",
            cursor_field=CURSOR_FIELD,
            step="P1M",
            cursor_granularity="P1D",
        )
        .with_incremental_sync(
            "AnotherStream",
            start_datetime="2022-01-01",
            end_datetime="2022-02-28",
            datetime_format="%Y-%m-%d",
            cursor_field=CURSOR_FIELD,
            step="P1M",
            cursor_granularity="P1D",
        )
        .build(),
        config={},
        catalog=None,
        state=None,
    )

    stream_instance = test_source.streams({})[1]

    parent_stream_slice = StreamSlice(
        partition={}, cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"}
    )

    # This mocks the resulting records of the Rates stream which acts as the parent stream of the SubstreamPartitionRouter being tested
    with patch.object(
        SimpleRetriever,
        "_read_pages",
        side_effect=[
            [Record({"id": "1", CURSOR_FIELD: "2022-01-15"}, parent_stream_slice)],
            [Record({"id": "2", CURSOR_FIELD: "2022-01-15"}, parent_stream_slice)],
        ],
    ):
        slices = [partition.to_slice() for partition in stream_instance.generate_partitions()]
        assert list(slices) == [
            StreamSlice(
                partition={
                    "parent_id": "1",
                    "parent_slice": {},
                },
                cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"},
            ),
            StreamSlice(
                partition={
                    "parent_id": "1",
                    "parent_slice": {},
                },
                cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
            ),
            StreamSlice(
                partition={
                    "parent_id": "2",
                    "parent_slice": {},
                },
                cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"},
            ),
            StreamSlice(
                partition={
                    "parent_id": "2",
                    "parent_slice": {},
                },
                cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
            ),
        ]


def test_partition_limitation(caplog):
    """
    Test that when the number of partitions exceeds the maximum allowed limit in PerPartitionCursor,
    the oldest partitions are dropped, and the state is updated accordingly.

    In this test, we set the maximum number of partitions to 2 and provide 3 partitions.
    We verify that the state only retains information for the two most recent partitions.
    """
    stream_name = "Rates"

    partition_slices = [
        StreamSlice(partition={"partition_field": "1"}, cursor_slice={}),
        StreamSlice(partition={"partition_field": "2"}, cursor_slice={}),
        StreamSlice(partition={"partition_field": "3"}, cursor_slice={}),
    ]

    records_list = [
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-15"},
                associated_slice=partition_slices[0],
                stream_name=stream_name,
            ),
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-16"},
                associated_slice=partition_slices[0],
                stream_name=stream_name,
            ),
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-02-15"},
                associated_slice=partition_slices[0],
                stream_name=stream_name,
            )
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-16"},
                associated_slice=partition_slices[1],
                stream_name=stream_name,
            )
        ],
        [],
        [],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-02-17"},
                associated_slice=partition_slices[2],
                stream_name=stream_name,
            )
        ],
    ]

    configured_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="Rates",
            json_schema={},
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )
    catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

    initial_state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="post_comment_votes", namespace=None),
                stream_state=AirbyteStateBlob(
                    {
                        "states": [
                            {
                                "partition": {"partition_field": "1"},
                                "cursor": {CURSOR_FIELD: "2022-01-01"},
                            },
                            {
                                "partition": {"partition_field": "2"},
                                "cursor": {CURSOR_FIELD: "2022-01-02"},
                            },
                            {
                                "partition": {"partition_field": "3"},
                                "cursor": {CURSOR_FIELD: "2022-01-03"},
                            },
                        ]
                    }
                ),
            ),
        )
    ]
    logger = MagicMock()

    source = ConcurrentDeclarativeSource(
        source_config=ManifestBuilder()
        .with_list_partition_router(
            stream_name=stream_name, cursor_field="partition_field", partitions=["1", "2", "3"]
        )
        .with_incremental_sync(
            stream_name=stream_name,
            start_datetime="2022-01-01",
            end_datetime="2022-02-28",
            datetime_format="%Y-%m-%d",
            cursor_field=CURSOR_FIELD,
            step="P1M",
            cursor_granularity="P1D",
        )
        .build(),
        config={},
        catalog=catalog,
        state=initial_state,
    )

    # Use caplog to capture logs
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        with patch.object(SimpleRetriever, "_read_pages", side_effect=records_list):
            with patch.object(ConcurrentPerPartitionCursor, "DEFAULT_MAX_PARTITIONS_NUMBER", 2):
                output = list(source.read(logger, {}, catalog, initial_state))

    # Check if the warning was logged
    logged_messages = [record.message for record in caplog.records if record.levelname == "WARNING"]
    warning_message = 'The maximum number of partitions has been reached. Dropping the oldest partition: {"partition_field":"1"}. Over limit: 1.'
    assert warning_message in logged_messages

    final_state = [
        orjson.loads(orjson.dumps(message.state.stream.stream_state))
        for message in output
        if message.state
    ]
    assert final_state[-1] == {
        "lookback_window": 1,
        "state": {"cursor_field": "2022-02-17"},
        "use_global_cursor": False,
        "states": [
            {
                "partition": {"partition_field": "2"},
                "cursor": {CURSOR_FIELD: "2022-01-16"},
            },
            {
                "partition": {"partition_field": "3"},
                "cursor": {CURSOR_FIELD: "2022-02-17"},
            },
        ],
    }


def test_perpartition_with_fallback(caplog):
    """
    Test that when the number of partitions exceeds the limit in PerPartitionCursor,
    the cursor falls back to using the global cursor for state management.

    This test also checks that the appropriate warning logs are emitted when the partition limit is exceeded.
    """
    stream_name = "Rates"

    partition_slices = [
        StreamSlice(partition={"partition_field": str(i)}, cursor_slice={}) for i in range(1, 7)
    ]

    records_list = [
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-15"},
                associated_slice=partition_slices[0],
                stream_name=stream_name,
            ),
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-16"},
                associated_slice=partition_slices[0],
                stream_name=stream_name,
            ),
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-02-15"},
                associated_slice=partition_slices[0],
                stream_name=stream_name,
            )
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-16"},
                associated_slice=partition_slices[1],
                stream_name=stream_name,
            )
        ],
        [],
        [],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-02-17"},
                associated_slice=partition_slices[2],
                stream_name=stream_name,
            )
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-17"},
                associated_slice=partition_slices[3],
                stream_name=stream_name,
            )
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-02-19"},
                associated_slice=partition_slices[3],
                stream_name=stream_name,
            )
        ],
        [],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-02-18"},
                associated_slice=partition_slices[4],
                stream_name=stream_name,
            )
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-01-13"},
                associated_slice=partition_slices[3],
                stream_name=stream_name,
            )
        ],
        [
            Record(
                data={"a record key": "a record value", CURSOR_FIELD: "2022-02-18"},
                associated_slice=partition_slices[3],
                stream_name=stream_name,
            )
        ],
    ]

    configured_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name=stream_name,
            json_schema={},
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )
    catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

    initial_state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=stream_name, namespace=None),
                stream_state=AirbyteStateBlob(
                    {
                        "states": [
                            {
                                "partition": {"partition_field": "1"},
                                "cursor": {CURSOR_FIELD: "2022-01-01"},
                            },
                            {
                                "partition": {"partition_field": "2"},
                                "cursor": {CURSOR_FIELD: "2022-01-02"},
                            },
                            {
                                "partition": {"partition_field": "3"},
                                "cursor": {CURSOR_FIELD: "2022-01-03"},
                            },
                        ]
                    }
                ),
            ),
        )
    ]
    logger = MagicMock()

    source = ConcurrentDeclarativeSource(
        source_config=ManifestBuilder()
        .with_list_partition_router("Rates", "partition_field", ["1", "2", "3", "4", "5", "6"])
        .with_incremental_sync(
            stream_name=stream_name,
            start_datetime="2022-01-01",
            end_datetime="2022-02-28",
            datetime_format="%Y-%m-%d",
            cursor_field=CURSOR_FIELD,
            step="P1M",
            cursor_granularity="P1D",
        )
        .build(),
        config={},
        catalog=catalog,
        state=initial_state,
    )

    # Use caplog to capture logs
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        with patch.object(SimpleRetriever, "_read_pages", side_effect=records_list):
            with patch.object(ConcurrentPerPartitionCursor, "DEFAULT_MAX_PARTITIONS_NUMBER", 2):
                with patch.object(ConcurrentPerPartitionCursor, "SWITCH_TO_GLOBAL_LIMIT", 1):
                    output = list(source.read(logger, {}, catalog, initial_state))

    # Check if the warnings were logged
    logged_messages = [record.message for record in caplog.records if record.levelname == "WARNING"]
    warning_message = (
        "The maximum number of partitions has been reached. Dropping the oldest partition:"
    )
    expected_warning_over_limit_messages = [
        "Over limit: 1",
        "Over limit: 2",
        "Over limit: 3",
    ]

    for logged_message in logged_messages:
        assert warning_message in logged_message

    for expected_warning_over_limit_message in expected_warning_over_limit_messages:
        assert (
            len(
                [
                    logged_message
                    for logged_message in logged_messages
                    if expected_warning_over_limit_message in logged_message
                ]
            )
            > 0
        )

    # Proceed with existing assertions
    final_state = [
        orjson.loads(orjson.dumps(message.state.stream.stream_state))
        for message in output
        if message.state
    ]
    assert final_state[-1] == {
        "use_global_cursor": True,
        "state": {"cursor_field": "2022-02-19"},
        "lookback_window": 1,
    }


def test_per_partition_cursor_within_limit(caplog):
    """
    Test that the PerPartitionCursor correctly updates the state for each partition
    when the number of partitions is within the allowed limit.

    This test also checks that no warning logs are emitted when the partition limit is not exceeded.
    """
    partition_slices = [
        StreamSlice(partition={"partition_field": str(i)}, cursor_slice={}) for i in range(1, 4)
    ]
    slice_range_2022_01_01_partition_1 = StreamSlice(
        partition={"partition_field": "1"},
        cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"},
    )
    slice_range_2022_02_01_partition_1 = StreamSlice(
        partition={"partition_field": "1"},
        cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
    )
    slice_range_2022_03_01_partition_1 = StreamSlice(
        partition={"partition_field": "1"},
        cursor_slice={"start_time": "2022-03-01", "end_time": "2022-03-31"},
    )
    slice_range_2022_01_01_partition_2 = StreamSlice(
        partition={"partition_field": "2"},
        cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"},
    )
    slice_range_2022_02_01_partition_2 = StreamSlice(
        partition={"partition_field": "2"},
        cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
    )
    slice_range_2022_03_01_partition_2 = StreamSlice(
        partition={"partition_field": "2"},
        cursor_slice={"start_time": "2022-03-01", "end_time": "2022-03-31"},
    )
    slice_range_2022_01_01_partition_3 = StreamSlice(
        partition={"partition_field": "3"},
        cursor_slice={"start_time": "2022-01-01", "end_time": "2022-01-31"},
    )
    slice_range_2022_02_01_partition_3 = StreamSlice(
        partition={"partition_field": "3"},
        cursor_slice={"start_time": "2022-02-01", "end_time": "2022-02-28"},
    )
    slice_range_2022_03_01_partition_3 = StreamSlice(
        partition={"partition_field": "3"},
        cursor_slice={"start_time": "2022-03-01", "end_time": "2022-03-31"},
    )

    records_list = [
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-01-15"},
                stream_name="Rates",
                associated_slice=slice_range_2022_01_01_partition_1,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-02-20"},
                stream_name="Rates",
                associated_slice=slice_range_2022_02_01_partition_1,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-03-25"},
                stream_name="Rates",
                associated_slice=slice_range_2022_03_01_partition_1,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-01-16"},
                stream_name="Rates",
                associated_slice=slice_range_2022_01_01_partition_2,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-02-18"},
                stream_name="Rates",
                associated_slice=slice_range_2022_02_01_partition_2,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-03-28"},
                stream_name="Rates",
                associated_slice=slice_range_2022_03_01_partition_2,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-01-17"},
                stream_name="Rates",
                associated_slice=slice_range_2022_01_01_partition_3,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-02-19"},
                stream_name="Rates",
                associated_slice=slice_range_2022_02_01_partition_3,
            )
        ],
        [
            Record(
                {"a record key": "a record value", CURSOR_FIELD: "2022-03-29"},
                stream_name="Rates",
                associated_slice=slice_range_2022_03_01_partition_3,
            )
        ],
    ]

    configured_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="Rates",
            json_schema={},
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )
    catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

    initial_state = {}
    logger = MagicMock()

    source = ConcurrentDeclarativeSource(
        source_config=ManifestBuilder()
        .with_list_partition_router("Rates", "partition_field", ["1", "2", "3"])
        .with_incremental_sync(
            "Rates",
            start_datetime="2022-01-01",
            end_datetime="2022-03-31",
            datetime_format="%Y-%m-%d",
            cursor_field=CURSOR_FIELD,
            step="P1M",
            cursor_granularity="P1D",
        )
        .build(),
        config={},
        catalog=catalog,
        state=initial_state,
    )

    # Use caplog to capture logs
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        with patch.object(SimpleRetriever, "_read_pages", side_effect=records_list):
            with patch.object(ConcurrentPerPartitionCursor, "DEFAULT_MAX_PARTITIONS_NUMBER", 5):
                output = list(source.read(logger, {}, catalog, initial_state))

    # Since the partition limit is not exceeded, we expect no warnings
    logged_warnings = [record.message for record in caplog.records if record.levelname == "WARNING"]
    assert len(logged_warnings) == 0

    # Proceed with existing assertions
    final_state = [
        orjson.loads(orjson.dumps(message.state.stream.stream_state))
        for message in output
        if message.state
    ]
    assert final_state[-1] == {
        "lookback_window": 1,
        "state": {"cursor_field": "2022-03-29"},
        "use_global_cursor": False,
        "states": [
            {
                "partition": {"partition_field": "1"},
                "cursor": {CURSOR_FIELD: "2022-03-25"},
            },
            {
                "partition": {"partition_field": "2"},
                "cursor": {CURSOR_FIELD: "2022-03-28"},
            },
            {
                "partition": {"partition_field": "3"},
                "cursor": {CURSOR_FIELD: "2022-03-29"},
            },
        ],
    }
