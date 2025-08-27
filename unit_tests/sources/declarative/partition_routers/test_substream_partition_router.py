#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterable, List, Mapping, Optional
from unittest.mock import Mock

import pytest as pytest
from airbyte_protocol_dataclasses.models import AirbyteStream

from airbyte_cdk.sources.declarative.incremental import (
    ConcurrentCursorFactory,
    ConcurrentPerPartitionCursor,
)
from airbyte_cdk.sources.declarative.partition_routers import (
    CartesianProductStreamSlicer,
    ListPartitionRouter,
)
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    ParentStreamConfig,
    SubstreamPartitionRouter,
)
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.sources.streams.checkpoint import Cursor
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream
from airbyte_cdk.sources.streams.concurrent.availability_strategy import StreamAvailability
from airbyte_cdk.sources.streams.concurrent.cursor import (
    ConcurrentCursor,
    CursorField,
    FinalStateCursor,
)
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import (
    CustomFormatConcurrentStreamStateConverter,
)
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.utils.datetime_helpers import ab_datetime_now, ab_datetime_parse
from unit_tests.sources.streams.concurrent.scenarios.thread_based_concurrent_stream_source_builder import (
    InMemoryPartition,
)

parent_records = [{"id": 1, "data": "data1"}, {"id": 2, "data": "data2"}]
more_records = [
    {"id": 10, "data": "data10", "slice": "second_parent"},
    {"id": 20, "data": "data20", "slice": "second_parent"},
]

data_first_parent_slice = [
    {"id": 0, "slice": "first", "data": "A"},
    {"id": 1, "slice": "first", "data": "B"},
]
data_second_parent_slice = [{"id": 2, "slice": "second", "data": "C"}]
data_third_parent_slice = []
all_parent_data = data_first_parent_slice + data_second_parent_slice + data_third_parent_slice
parent_slices = [
    StreamSlice(partition={"slice": "first"}, cursor_slice={}),
    StreamSlice(partition={"slice": "second"}, cursor_slice={}),
    StreamSlice(partition={"slice": "third"}, cursor_slice={}),
]
parent_slices_with_cursor = [
    StreamSlice(
        partition={"slice": "first"}, cursor_slice={"start": "2021-01-01", "end": "2023-01-01"}
    ),
    StreamSlice(
        partition={"slice": "second"}, cursor_slice={"start": "2021-01-01", "end": "2023-01-01"}
    ),
    StreamSlice(
        partition={"slice": "third"}, cursor_slice={"start": "2021-01-01", "end": "2023-01-01"}
    ),
]
second_parent_stream_slice = [StreamSlice(partition={"slice": "second_parent"}, cursor_slice={})]

data_first_parent_slice_with_cursor = [
    {"id": 0, "slice": "first", "data": "A", "cursor": "2021-01-01"},
    {"id": 1, "slice": "first", "data": "B", "cursor": "2021-01-02"},
]
data_second_parent_slice_with_cursor = [
    {"id": 2, "slice": "second", "data": "C", "cursor": "2022-01-01"}
]
all_parent_data_with_cursor = (
    data_first_parent_slice_with_cursor + data_second_parent_slice_with_cursor
)
_EMPTY_SLICE = StreamSlice(partition={}, cursor_slice={})
_ANY_STREAM = None


def _build_records_for_slice(records: List[Mapping[str, Any]], _slice: StreamSlice):
    return [Record(record, "stream_name", _slice) for record in records]


class MockStream(AbstractStream):
    def __init__(self, partitions, name, cursor_field="", cursor=None):
        self._partitions = partitions
        self._stream_cursor_field = cursor_field
        self._name = name
        self._state = {"states": []}
        self._cursor = cursor if cursor else FinalStateCursor(self._name, None, Mock())

    def generate_partitions(self) -> Iterable[Partition]:
        list(self._cursor.stream_slices())
        return self._partitions

    @property
    def name(self) -> str:
        return self._name

    @property
    def cursor_field(self) -> Optional[str]:
        return self._stream_cursor_field

    def get_json_schema(self) -> Mapping[str, Any]:
        return {}

    def as_airbyte_stream(self) -> AirbyteStream:
        raise NotImplementedError()

    def log_stream_sync_configuration(self) -> None:
        raise NotImplementedError()

    @property
    def cursor(self) -> Cursor:
        return self._cursor

    def check_availability(self) -> StreamAvailability:
        raise NotImplementedError()


@pytest.mark.parametrize(
    "parent_stream_configs, expected_slices",
    [
        ([], None),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [InMemoryPartition("partition_name", "first_stream", _EMPTY_SLICE, [])],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    parameters={},
                    config={},
                )
            ],
            [],
        ),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_name",
                                "first_stream",
                                _EMPTY_SLICE,
                                _build_records_for_slice(parent_records, _EMPTY_SLICE),
                            )
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    parameters={},
                    config={},
                )
            ],
            [
                {"first_stream_id": 1, "parent_slice": {}},
                {"first_stream_id": 2, "parent_slice": {}},
            ],
        ),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_1",
                                "first_stream",
                                parent_slices[0],
                                _build_records_for_slice(data_first_parent_slice, parent_slices[0]),
                            ),
                            InMemoryPartition(
                                "partition_2",
                                "first_stream",
                                parent_slices[1],
                                _build_records_for_slice(
                                    data_second_parent_slice, parent_slices[1]
                                ),
                            ),
                            InMemoryPartition(
                                "partition_3",
                                "first_stream",
                                parent_slices[2],
                                _build_records_for_slice(data_third_parent_slice, parent_slices[2]),
                            ),
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    parameters={},
                    config={},
                )
            ],
            [
                {"parent_slice": {"slice": "first"}, "first_stream_id": 0},
                {"parent_slice": {"slice": "first"}, "first_stream_id": 1},
                {"parent_slice": {"slice": "second"}, "first_stream_id": 2},
            ],
        ),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_1",
                                "first_stream",
                                parent_slices[0],
                                _build_records_for_slice(data_first_parent_slice, parent_slices[0]),
                            ),
                            InMemoryPartition(
                                "partition_2",
                                "first_stream",
                                parent_slices[1],
                                _build_records_for_slice(
                                    data_second_parent_slice, parent_slices[1]
                                ),
                            ),
                            InMemoryPartition(
                                "partition_3",
                                "first_stream",
                                parent_slices[2],
                                _build_records_for_slice(data_third_parent_slice, parent_slices[2]),
                            ),
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    parameters={},
                    config={},
                )
            ],
            [
                {"parent_slice": {"slice": "first"}, "first_stream_id": 0},
                {"parent_slice": {"slice": "first"}, "first_stream_id": 1},
                {"parent_slice": {"slice": "second"}, "first_stream_id": 2},
            ],
        ),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_1",
                                "first_stream",
                                parent_slices[0],
                                _build_records_for_slice(data_first_parent_slice, parent_slices[0]),
                            ),
                            InMemoryPartition(
                                "partition_2",
                                "first_stream",
                                parent_slices[1],
                                _build_records_for_slice(
                                    data_second_parent_slice, parent_slices[1]
                                ),
                            ),
                            InMemoryPartition(
                                "partition_3",
                                "first_stream",
                                parent_slices[2],
                                [],
                            ),
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    parameters={},
                    config={},
                ),
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_1",
                                "first_stream",
                                second_parent_stream_slice[0],
                                _build_records_for_slice(
                                    more_records, second_parent_stream_slice[0]
                                ),
                            ),
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="second_stream_id",
                    parameters={},
                    config={},
                ),
            ],
            [
                {"parent_slice": {"slice": "first"}, "first_stream_id": 0},
                {"parent_slice": {"slice": "first"}, "first_stream_id": 1},
                {"parent_slice": {"slice": "second"}, "first_stream_id": 2},
                {"parent_slice": {"slice": "second_parent"}, "second_stream_id": 10},
                {"parent_slice": {"slice": "second_parent"}, "second_stream_id": 20},
            ],
        ),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_1",
                                "first_stream",
                                _EMPTY_SLICE,
                                _build_records_for_slice(
                                    [{"id": 0}, {"id": 1}, {"_id": 2}, {"id": 3}], _EMPTY_SLICE
                                ),
                            ),
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    parameters={},
                    config={},
                )
            ],
            [
                {"first_stream_id": 0, "parent_slice": {}},
                {"first_stream_id": 1, "parent_slice": {}},
                {"first_stream_id": 3, "parent_slice": {}},
            ],
        ),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_1",
                                "first_stream",
                                _EMPTY_SLICE,
                                _build_records_for_slice(
                                    [
                                        {"a": {"b": 0}},
                                        {"a": {"b": 1}},
                                        {"a": {"c": 2}},
                                        {"a": {"b": 3}},
                                    ],
                                    _EMPTY_SLICE,
                                ),
                            ),
                        ],
                        "first_stream",
                    ),
                    parent_key="a/b",
                    partition_field="first_stream_id",
                    parameters={},
                    config={},
                )
            ],
            [
                {"first_stream_id": 0, "parent_slice": {}},
                {"first_stream_id": 1, "parent_slice": {}},
                {"first_stream_id": 3, "parent_slice": {}},
            ],
        ),
    ],
    ids=[
        "test_no_parents",
        "test_single_parent_slices_no_records",
        "test_single_parent_slices_with_records",
        "test_with_parent_slices_and_records",
        "test_cursor_values_are_removed_from_parent_slices",
        "test_multiple_parent_streams",
        "test_missed_parent_key",
        "test_dpath_extraction",
    ],
)
def test_substream_partition_router(parent_stream_configs, expected_slices):
    if expected_slices is None:
        try:
            SubstreamPartitionRouter(
                parent_stream_configs=parent_stream_configs, parameters={}, config={}
            )
            assert False
        except ValueError:
            return
    partition_router = SubstreamPartitionRouter(
        parent_stream_configs=parent_stream_configs, parameters={}, config={}
    )
    slices = [s for s in partition_router.stream_slices()]
    assert slices == expected_slices


@pytest.mark.parametrize(
    "parent_stream_request_parameters, expected_req_params, expected_headers, expected_body_json, expected_body_data",
    [
        (
            [
                RequestOption(
                    inject_into=RequestOptionType.request_parameter,
                    parameters={},
                    field_name="first_stream",
                ),
                RequestOption(
                    inject_into=RequestOptionType.request_parameter,
                    parameters={},
                    field_name="second_stream",
                ),
            ],
            {"first_stream": "1234", "second_stream": "4567"},
            {},
            {},
            {},
        ),
        (
            [
                RequestOption(
                    inject_into=RequestOptionType.header, parameters={}, field_name="first_stream"
                ),
                RequestOption(
                    inject_into=RequestOptionType.header, parameters={}, field_name="second_stream"
                ),
            ],
            {},
            {"first_stream": "1234", "second_stream": "4567"},
            {},
            {},
        ),
        (
            [
                RequestOption(
                    inject_into=RequestOptionType.request_parameter,
                    parameters={},
                    field_name="first_stream",
                ),
                RequestOption(
                    inject_into=RequestOptionType.header, parameters={}, field_name="second_stream"
                ),
            ],
            {"first_stream": "1234"},
            {"second_stream": "4567"},
            {},
            {},
        ),
        (
            [
                RequestOption(
                    inject_into=RequestOptionType.body_json,
                    parameters={},
                    field_name="first_stream",
                ),
                RequestOption(
                    inject_into=RequestOptionType.body_json,
                    parameters={},
                    field_name="second_stream",
                ),
            ],
            {},
            {},
            {"first_stream": "1234", "second_stream": "4567"},
            {},
        ),
        (
            [
                RequestOption(
                    inject_into=RequestOptionType.body_data,
                    parameters={},
                    field_name="first_stream",
                ),
                RequestOption(
                    inject_into=RequestOptionType.body_data,
                    parameters={},
                    field_name="second_stream",
                ),
            ],
            {},
            {},
            {},
            {"first_stream": "1234", "second_stream": "4567"},
        ),
    ],
    ids=[
        "test_request_option_in_request_param",
        "test_request_option_in_header",
        "test_request_option_in_param_and_header",
        "test_request_option_in_body_json",
        "test_request_option_in_body_data",
    ],
)
def test_request_option(
    parent_stream_request_parameters,
    expected_req_params,
    expected_headers,
    expected_body_json,
    expected_body_data,
):
    partition_router = SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=_ANY_STREAM,
                parent_key="id",
                partition_field="first_stream_id",
                parameters={},
                config={},
                request_option=parent_stream_request_parameters[0],
            ),
            ParentStreamConfig(
                stream=_ANY_STREAM,
                parent_key="id",
                partition_field="second_stream_id",
                parameters={},
                config={},
                request_option=parent_stream_request_parameters[1],
            ),
        ],
        parameters={},
        config={},
    )
    stream_slice = {"first_stream_id": "1234", "second_stream_id": "4567"}

    assert partition_router.get_request_params(stream_slice=stream_slice) == expected_req_params
    assert partition_router.get_request_headers(stream_slice=stream_slice) == expected_headers
    assert partition_router.get_request_body_json(stream_slice=stream_slice) == expected_body_json
    assert partition_router.get_request_body_data(stream_slice=stream_slice) == expected_body_data


@pytest.mark.parametrize(
    "parent_stream_config, expected_state",
    [
        (
            ParentStreamConfig(
                stream=MockStream(
                    [
                        InMemoryPartition(
                            "partition_1",
                            "first_stream",
                            parent_slices_with_cursor[0],
                            _build_records_for_slice(
                                data_first_parent_slice_with_cursor, parent_slices_with_cursor[0]
                            ),
                        ),
                        InMemoryPartition(
                            "partition_2",
                            "first_stream",
                            parent_slices_with_cursor[1],
                            _build_records_for_slice(
                                data_second_parent_slice_with_cursor, parent_slices_with_cursor[1]
                            ),
                        ),
                        InMemoryPartition(
                            "partition_3",
                            "first_stream",
                            parent_slices_with_cursor[2],
                            _build_records_for_slice([], parent_slices_with_cursor[2]),
                        ),
                    ],
                    "first_stream",
                    cursor=ConcurrentPerPartitionCursor(
                        cursor_factory=ConcurrentCursorFactory(
                            lambda stream_state, runtime_lookback_window: ConcurrentCursor(
                                stream_name="first_stream",
                                stream_namespace=None,
                                stream_state=stream_state,
                                message_repository=Mock(),
                                connector_state_manager=Mock(),
                                connector_state_converter=CustomFormatConcurrentStreamStateConverter(
                                    "%Y-%m-%d"
                                ),
                                cursor_field=CursorField("cursor"),
                                slice_boundary_fields=("start", "end"),
                                start=ab_datetime_parse("2021-01-01").to_datetime(),
                                end_provider=lambda: ab_datetime_parse("2023-01-01").to_datetime(),
                                lookback_window=runtime_lookback_window,
                            ),
                        ),
                        partition_router=ListPartitionRouter(
                            values=["first", "second", "third"],
                            cursor_field="slice",
                            config={},
                            parameters={},
                        ),
                        stream_name="first_stream",
                        stream_namespace=None,
                        stream_state={},
                        message_repository=Mock(),
                        connector_state_manager=Mock(),
                        connector_state_converter=CustomFormatConcurrentStreamStateConverter(
                            "%Y-%m-%d"
                        ),
                        cursor_field=CursorField("cursor"),
                    ),
                ),
                parent_key="id",
                partition_field="first_stream_id",
                parameters={},
                config={},
                incremental_dependency=True,
            ),
            {
                "first_stream": {
                    "lookback_window": 0,
                    "states": [
                        {"cursor": {"cursor": "2021-01-02"}, "partition": {"slice": "first"}},
                        {"cursor": {"cursor": "2022-01-01"}, "partition": {"slice": "second"}},
                        {"cursor": {"cursor": "2021-01-01"}, "partition": {"slice": "third"}},
                    ],
                    "use_global_cursor": False,
                }
            },
        ),
    ],
    ids=[
        "test_incremental_dependency_state_update_with_cursor",
    ],
)
def test_substream_slicer_parent_state_update_with_cursor(parent_stream_config, expected_state):
    partition_router = SubstreamPartitionRouter(
        parent_stream_configs=[parent_stream_config], parameters={}, config={}
    )

    # Simulate reading the records and updating the state
    for _ in partition_router.stream_slices():
        pass  # This will process the slices and should update the parent state

    # Check if the parent state has been updated correctly
    parent_state = partition_router.get_stream_state()
    assert parent_state == expected_state


@pytest.mark.parametrize(
    "field_name_first_stream, field_name_second_stream, expected_request_params",
    [
        (
            "{{parameters['field_name_first_stream']}}",
            "{{parameters['field_name_second_stream']}}",
            {"parameter_first_stream_id": "1234", "parameter_second_stream_id": "4567"},
        ),
        (
            "{{config['field_name_first_stream']}}",
            "{{config['field_name_second_stream']}}",
            {"config_first_stream_id": "1234", "config_second_stream_id": "4567"},
        ),
    ],
    ids=[
        "parameters_interpolation",
        "config_interpolation",
    ],
)
def test_request_params_interpolation_for_parent_stream(
    field_name_first_stream: str, field_name_second_stream: str, expected_request_params: dict
):
    config = {
        "field_name_first_stream": "config_first_stream_id",
        "field_name_second_stream": "config_second_stream_id",
    }
    parameters = {
        "field_name_first_stream": "parameter_first_stream_id",
        "field_name_second_stream": "parameter_second_stream_id",
    }
    partition_router = SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=MockStream(
                    parent_slices,
                    data_first_parent_slice + data_second_parent_slice,
                    "first_stream",
                ),
                parent_key="id",
                partition_field="first_stream_id",
                parameters=parameters,
                config=config,
                request_option=RequestOption(
                    inject_into=RequestOptionType.request_parameter,
                    parameters=parameters,
                    field_name=field_name_first_stream,
                ),
            ),
            ParentStreamConfig(
                stream=MockStream(second_parent_stream_slice, more_records, "second_stream"),
                parent_key="id",
                partition_field="second_stream_id",
                parameters=parameters,
                config=config,
                request_option=RequestOption(
                    inject_into=RequestOptionType.request_parameter,
                    parameters=parameters,
                    field_name=field_name_second_stream,
                ),
            ),
        ],
        parameters=parameters,
        config=config,
    )
    stream_slice = {"first_stream_id": "1234", "second_stream_id": "4567"}

    assert partition_router.get_request_params(stream_slice=stream_slice) == expected_request_params


def test_substream_using_incremental_parent_stream():
    mock_slices = [
        StreamSlice(
            cursor_slice={"start_time": "2024-04-27", "end_time": "2024-05-27"}, partition={}
        ),
        StreamSlice(
            cursor_slice={"start_time": "2024-05-27", "end_time": "2024-06-27"}, partition={}
        ),
    ]

    expected_slices = [
        {"partition_field": "may_record_0", "parent_slice": {}},
        {"partition_field": "may_record_1", "parent_slice": {}},
        {"partition_field": "jun_record_0", "parent_slice": {}},
        {"partition_field": "jun_record_1", "parent_slice": {}},
    ]

    partition_router = SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=MockStream(
                    [
                        InMemoryPartition(
                            "partition_1",
                            "first_stream",
                            mock_slices[0],
                            [
                                Record(
                                    {"id": "may_record_0", "updated_at": "2024-05-15"},
                                    "first_stream",
                                    mock_slices[0],
                                ),
                                Record(
                                    {"id": "may_record_1", "updated_at": "2024-05-16"},
                                    "first_stream",
                                    mock_slices[0],
                                ),
                            ],
                        ),
                        InMemoryPartition(
                            "partition_1",
                            "first_stream",
                            mock_slices[1],
                            [
                                Record(
                                    {"id": "jun_record_0", "updated_at": "2024-06-15"},
                                    "first_stream",
                                    mock_slices[1],
                                ),
                                Record(
                                    {"id": "jun_record_1", "updated_at": "2024-06-16"},
                                    "first_stream",
                                    mock_slices[1],
                                ),
                            ],
                        ),
                    ],
                    "first_stream",
                ),
                parent_key="id",
                partition_field="partition_field",
                parameters={},
                config={},
            )
        ],
        parameters={},
        config={},
    )

    actual_slices = list(partition_router.stream_slices())
    assert actual_slices == expected_slices


def test_substream_checkpoints_after_each_parent_partition():
    """
    This test validates the specific behavior that when getting all parent records for a substream,
    we are still updating state so that the parent stream's state is updated after we finish getting all
    parent records for the parent slice (not just the substream)
    """
    mock_slices = [
        StreamSlice(
            cursor_slice={"start_time": "2024-04-27", "end_time": "2024-05-27"}, partition={}
        ),
        StreamSlice(
            cursor_slice={"start_time": "2024-05-27", "end_time": "2024-06-27"}, partition={}
        ),
    ]

    expected_slices = [
        {"partition_field": "may_record_0", "parent_slice": {}},
        {"partition_field": "may_record_1", "parent_slice": {}},
        {"partition_field": "jun_record_0", "parent_slice": {}},
        {"partition_field": "jun_record_1", "parent_slice": {}},
    ]

    expected_parent_state = [
        {"first_stream": {"updated_at": mock_slices[0]["start_time"]}},
        {"first_stream": {"updated_at": "2024-05-16"}},
        {"first_stream": {"updated_at": "2024-05-16"}},
        {"first_stream": {"updated_at": "2024-06-16"}},
        {"first_stream": {"updated_at": "2024-06-16"}},
    ]

    partition_router = SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=MockStream(
                    [
                        InMemoryPartition(
                            "partition_1",
                            "first_stream",
                            mock_slices[0],
                            [
                                Record(
                                    {"id": "may_record_0", "updated_at": "2024-05-15"},
                                    "first_stream",
                                    mock_slices[0],
                                ),
                                Record(
                                    {"id": "may_record_1", "updated_at": "2024-05-16"},
                                    "first_stream",
                                    mock_slices[0],
                                ),
                            ],
                        ),
                        InMemoryPartition(
                            "partition_1",
                            "first_stream",
                            mock_slices[1],
                            [
                                Record(
                                    {"id": "jun_record_0", "updated_at": "2024-06-15"},
                                    "first_stream",
                                    mock_slices[1],
                                ),
                                Record(
                                    {"id": "jun_record_1", "updated_at": "2024-06-16"},
                                    "first_stream",
                                    mock_slices[1],
                                ),
                            ],
                        ),
                    ],
                    "first_stream",
                    "updated_at",
                    ConcurrentCursor(
                        stream_name="first_stream",
                        stream_namespace=None,
                        stream_state={},
                        message_repository=Mock(),
                        connector_state_manager=Mock(),
                        connector_state_converter=CustomFormatConcurrentStreamStateConverter(
                            "%Y-%m-%d"
                        ),
                        cursor_field=CursorField("updated_at"),
                        slice_boundary_fields=("start_time", "end_time"),
                        start=ab_datetime_parse(mock_slices[0]["start_time"]).to_datetime(),
                        end_provider=lambda: ab_datetime_parse("2023-01-01").to_datetime(),
                    ),
                ),
                incremental_dependency=True,
                parent_key="id",
                partition_field="partition_field",
                parameters={},
                config={},
            )
        ],
        parameters={},
        config={},
    )

    expected_counter = 0
    for actual_slice in partition_router.stream_slices():
        assert actual_slice == expected_slices[expected_counter]
        assert partition_router.get_stream_state() == expected_parent_state[expected_counter]
        expected_counter += 1
    assert partition_router.get_stream_state() == expected_parent_state[-1]


@pytest.mark.parametrize(
    "parent_stream_configs, expected_slices",
    [
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_name",
                                "first_stream",
                                _EMPTY_SLICE,
                                _build_records_for_slice(
                                    [
                                        {
                                            "id": 1,
                                            "field_1": "value_1",
                                            "field_2": {"nested_field": "nested_value_1"},
                                        },
                                        {
                                            "id": 2,
                                            "field_1": "value_2",
                                            "field_2": {"nested_field": "nested_value_2"},
                                        },
                                    ],
                                    _EMPTY_SLICE,
                                ),
                            )
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    extra_fields=[["field_1"], ["field_2", "nested_field"]],
                    parameters={},
                    config={},
                )
            ],
            [
                {"field_1": "value_1", "field_2.nested_field": "nested_value_1"},
                {"field_1": "value_2", "field_2.nested_field": "nested_value_2"},
            ],
        ),
        (
            [
                ParentStreamConfig(
                    stream=MockStream(
                        [
                            InMemoryPartition(
                                "partition_name",
                                "first_stream",
                                _EMPTY_SLICE,
                                _build_records_for_slice(
                                    [
                                        {"id": 1, "field_1": "value_1"},
                                        {"id": 2, "field_1": "value_2"},
                                    ],
                                    _EMPTY_SLICE,
                                ),
                            )
                        ],
                        "first_stream",
                    ),
                    parent_key="id",
                    partition_field="first_stream_id",
                    extra_fields=[["field_1"]],
                    parameters={},
                    config={},
                )
            ],
            [{"field_1": "value_1"}, {"field_1": "value_2"}],
        ),
    ],
    ids=[
        "test_with_nested_extra_keys",
        "test_with_single_extra_key",
    ],
)
def test_substream_partition_router_with_extra_keys(parent_stream_configs, expected_slices):
    partition_router = SubstreamPartitionRouter(
        parent_stream_configs=parent_stream_configs, parameters={}, config={}
    )
    slices = [s.extra_fields for s in partition_router.stream_slices()]
    assert slices == expected_slices


@pytest.mark.parametrize(
    "stream_slicers, expect_warning",
    [
        # Case with two ListPartitionRouters, no warning expected
        (
            [
                ListPartitionRouter(
                    values=["1", "2", "3"], cursor_field="partition_field", config={}, parameters={}
                ),
                ListPartitionRouter(
                    values=["1", "2", "3"], cursor_field="partition_field", config={}, parameters={}
                ),
            ],
            False,
        ),
        # Case with a SubstreamPartitionRouter, warning expected
        (
            [
                ListPartitionRouter(
                    values=["1", "2", "3"], cursor_field="partition_field", config={}, parameters={}
                ),
                SubstreamPartitionRouter(
                    parent_stream_configs=[
                        ParentStreamConfig(
                            stream=MockStream(
                                [{}],
                                [
                                    {"a": {"b": 0}},
                                    {"a": {"b": 1}},
                                    {"a": {"c": 2}},
                                    {"a": {"b": 3}},
                                ],
                                "first_stream",
                            ),
                            parent_key="a/b",
                            partition_field="first_stream_id",
                            parameters={},
                            config={},
                        )
                    ],
                    parameters={},
                    config={},
                ),
            ],
            True,
        ),
        # Case with nested CartesianProductStreamSlicer containing a SubstreamPartitionRouter, warning expected
        (
            [
                ListPartitionRouter(
                    values=["1", "2", "3"], cursor_field="partition_field", config={}, parameters={}
                ),
                CartesianProductStreamSlicer(
                    stream_slicers=[
                        ListPartitionRouter(
                            values=["1", "2", "3"],
                            cursor_field="partition_field",
                            config={},
                            parameters={},
                        ),
                        SubstreamPartitionRouter(
                            parent_stream_configs=[
                                ParentStreamConfig(
                                    stream=MockStream(
                                        [{}],
                                        [
                                            {"a": {"b": 0}},
                                            {"a": {"b": 1}},
                                            {"a": {"c": 2}},
                                            {"a": {"b": 3}},
                                        ],
                                        "first_stream",
                                    ),
                                    parent_key="a/b",
                                    partition_field="first_stream_id",
                                    parameters={},
                                    config={},
                                )
                            ],
                            parameters={},
                            config={},
                        ),
                    ],
                    parameters={},
                ),
            ],
            True,
        ),
    ],
)
def test_cartesian_product_stream_slicer_warning_log_message(
    caplog, stream_slicers, expect_warning
):
    """Test that a warning is logged when SubstreamPartitionRouter is used within a CartesianProductStreamSlicer."""
    warning_message = "Parent state handling is not supported for CartesianProductStreamSlicer."

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        CartesianProductStreamSlicer(stream_slicers=stream_slicers, parameters={})

    logged_warnings = [record.message for record in caplog.records if record.levelname == "WARNING"]

    if expect_warning:
        assert warning_message in logged_warnings
    else:
        assert warning_message not in logged_warnings
