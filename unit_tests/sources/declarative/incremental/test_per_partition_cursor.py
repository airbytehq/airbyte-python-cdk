#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections import OrderedDict
from unittest.mock import Mock

import pytest

from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.incremental.global_substream_cursor import (
    GlobalSubstreamCursor,
)
from airbyte_cdk.sources.declarative.incremental.per_partition_cursor import (
    PerPartitionCursor,
    PerPartitionKeySerializer,
    StreamSlice,
)
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.types import Record

PARTITION = {
    "partition_key string": "partition value",
    "partition_key int": 1,
    "partition_key list str": ["list item 1", "list item 2"],
    "partition_key list dict": [
        {
            "dict within list key 1-1": "dict within list value 1-1",
            "dict within list key 1-2": "dict within list value 1-2",
        },
        {"dict within list key 2": "dict within list value 2"},
    ],
    "partition_key nested dict": {
        "nested_partition_key 1": "a nested value",
        "nested_partition_key 2": "another nested value",
    },
}

CURSOR_SLICE_FIELD = "cursor slice field"
CURSOR_STATE_KEY = "cursor state"
CURSOR_STATE = {CURSOR_STATE_KEY: "a state value"}
NOT_CONSIDERED_BECAUSE_MOCKED_CURSOR_HAS_NO_STATE = "any"
STATE = {
    "states": [
        {
            "partition": {
                "partition_router_field_1": "X1",
                "partition_router_field_2": "Y1",
            },
            "cursor": {"cursor state field": 1},
        },
        {
            "partition": {
                "partition_router_field_1": "X2",
                "partition_router_field_2": "Y2",
            },
            "cursor": {"cursor state field": 2},
        },
    ]
}


def test_partition_serialization():
    serializer = PerPartitionKeySerializer()
    assert serializer.to_partition(serializer.to_partition_key(PARTITION)) == PARTITION


def test_partition_with_different_key_orders():
    ordered_dict = OrderedDict({"1": 1, "2": 2})
    same_dict_with_different_order = OrderedDict({"2": 2, "1": 1})
    serializer = PerPartitionKeySerializer()

    assert serializer.to_partition_key(ordered_dict) == serializer.to_partition_key(
        same_dict_with_different_order
    )


def test_given_tuples_in_json_then_deserialization_convert_to_list():
    """
    This is a known issue with the current implementation. However, the assumption is that this wouldn't be a problem as we only use the
    immutability and we expect stream slices to be immutable anyway
    """
    serializer = PerPartitionKeySerializer()
    partition_with_tuple = {"key": (1, 2, 3)}

    assert partition_with_tuple != serializer.to_partition(
        serializer.to_partition_key(partition_with_tuple)
    )


def test_stream_slice_merge_dictionaries():
    stream_slice = StreamSlice(
        partition={"partition key": "partition value"}, cursor_slice={"cursor key": "cursor value"}
    )
    assert stream_slice == {"partition key": "partition value", "cursor key": "cursor value"}


def test_overlapping_slice_keys_raise_error():
    with pytest.raises(ValueError):
        StreamSlice(
            partition={"overlapping key": "partition value"},
            cursor_slice={"overlapping key": "cursor value"},
        )


class MockedCursorBuilder:
    def __init__(self):
        self._stream_slices = []
        self._stream_state = {}

    def with_stream_slices(self, stream_slices):
        self._stream_slices = stream_slices
        return self

    def with_stream_state(self, stream_state):
        self._stream_state = stream_state
        return self

    def build(self):
        cursor = Mock(spec=DeclarativeCursor)
        cursor.get_stream_state.return_value = self._stream_state
        cursor.stream_slices.return_value = self._stream_slices
        return cursor


@pytest.fixture()
def mocked_partition_router():
    return Mock(spec=PartitionRouter)


@pytest.fixture()
def mocked_cursor_factory():
    cursor_factory = Mock()
    cursor_factory.create.return_value = MockedCursorBuilder().build()
    return cursor_factory


def test_given_no_partition_when_stream_slices_then_no_slices(
    mocked_cursor_factory, mocked_partition_router
):
    mocked_partition_router.stream_slices.return_value = []
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)

    slices = cursor.stream_slices()

    assert not next(slices, None)


def test_given_partition_router_without_state_has_one_partition_then_return_one_slice_per_cursor_slice(
    mocked_cursor_factory, mocked_partition_router
):
    partition = StreamSlice(
        partition={"partition_field_1": "a value", "partition_field_2": "another value"},
        cursor_slice={},
    )
    mocked_partition_router.stream_slices.return_value = [partition]
    cursor_slices = [{"start_datetime": 1}, {"start_datetime": 2}]
    mocked_cursor_factory.create.return_value = (
        MockedCursorBuilder().with_stream_slices(cursor_slices).build()
    )
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)

    slices = cursor.stream_slices()

    assert list(slices) == [
        StreamSlice(partition=partition, cursor_slice=cursor_slice)
        for cursor_slice in cursor_slices
    ]


def test_given_partition_associated_with_state_when_stream_slices_then_do_not_recreate_cursor(
    mocked_cursor_factory, mocked_partition_router
):
    partition = StreamSlice(
        partition={"partition_field_1": "a value", "partition_field_2": "another value"},
        cursor_slice={},
    )
    mocked_partition_router.stream_slices.return_value = [partition]
    cursor_slices = [{"start_datetime": 1}]
    mocked_cursor_factory.create.return_value = (
        MockedCursorBuilder().with_stream_slices(cursor_slices).build()
    )
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)

    cursor.set_initial_state(
        {"states": [{"partition": partition.partition, "cursor": CURSOR_STATE}]}
    )
    mocked_cursor_factory.create.assert_called_once()
    slices = list(cursor.stream_slices())

    mocked_cursor_factory.create.assert_called_once()
    assert len(slices) == 1


def test_given_multiple_partitions_then_each_have_their_state(
    mocked_cursor_factory, mocked_partition_router
):
    first_partition = {"first_partition_key": "first_partition_value"}
    mocked_partition_router.stream_slices.return_value = [
        StreamSlice(partition=first_partition, cursor_slice={}),
        StreamSlice(partition={"second_partition_key": "second_partition_value"}, cursor_slice={}),
    ]
    first_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    second_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "second slice cursor value"}])
        .build()
    )
    mocked_cursor_factory.create.side_effect = [first_cursor, second_cursor]
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)

    cursor.set_initial_state({"states": [{"partition": first_partition, "cursor": CURSOR_STATE}]})
    slices = list(cursor.stream_slices())

    first_cursor.stream_slices.assert_called_once()
    second_cursor.stream_slices.assert_called_once()
    assert slices == [
        StreamSlice(
            partition={"first_partition_key": "first_partition_value"},
            cursor_slice={CURSOR_SLICE_FIELD: "first slice cursor value"},
        ),
        StreamSlice(
            partition={"second_partition_key": "second_partition_value"},
            cursor_slice={CURSOR_SLICE_FIELD: "second slice cursor value"},
        ),
    ]


def test_given_stream_slices_when_get_stream_state_then_return_updated_state(
    mocked_cursor_factory, mocked_partition_router
):
    mocked_cursor_factory.create.side_effect = [
        MockedCursorBuilder()
        .with_stream_state({CURSOR_STATE_KEY: "first slice cursor value"})
        .build(),
        MockedCursorBuilder()
        .with_stream_state({CURSOR_STATE_KEY: "second slice cursor value"})
        .build(),
    ]
    mocked_partition_router.stream_slices.return_value = [
        StreamSlice(partition={"partition key": "first partition"}, cursor_slice={}),
        StreamSlice(partition={"partition key": "second partition"}, cursor_slice={}),
    ]

    # Mock the get_parent_state method to return the parent state
    mocked_partition_router.get_stream_state.return_value = {}

    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    list(cursor.stream_slices())
    assert cursor.get_stream_state() == {
        "states": [
            {
                "partition": {"partition key": "first partition"},
                "cursor": {CURSOR_STATE_KEY: "first slice cursor value"},
            },
            {
                "partition": {"partition key": "second partition"},
                "cursor": {CURSOR_STATE_KEY: "second slice cursor value"},
            },
        ]
    }


def test_when_get_stream_state_then_delegate_to_underlying_cursor(
    mocked_cursor_factory, mocked_partition_router
):
    underlying_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    mocked_cursor_factory.create.side_effect = [underlying_cursor]
    mocked_partition_router.stream_slices.return_value = [
        StreamSlice(partition={"partition key": "first partition"}, cursor_slice={})
    ]
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    first_slice = list(cursor.stream_slices())[0]

    cursor.should_be_synced(
        Record(data={}, associated_slice=first_slice, stream_name="test_stream")
    )

    underlying_cursor.should_be_synced.assert_called_once_with(
        Record(data={}, associated_slice=first_slice.cursor_slice, stream_name="test_stream")
    )


def test_close_slice(mocked_cursor_factory, mocked_partition_router):
    underlying_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    mocked_cursor_factory.create.side_effect = [underlying_cursor]
    stream_slice = StreamSlice(partition={"partition key": "first partition"}, cursor_slice={})
    mocked_partition_router.stream_slices.return_value = [stream_slice]
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    list(cursor.stream_slices())  # generate internal state

    cursor.close_slice(stream_slice)

    underlying_cursor.close_slice.assert_called_once_with(stream_slice.cursor_slice)


def test_given_no_last_record_when_close_slice_then_do_not_raise_error(
    mocked_cursor_factory, mocked_partition_router
):
    underlying_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    mocked_cursor_factory.create.side_effect = [underlying_cursor]
    stream_slice = StreamSlice(partition={"partition key": "first partition"}, cursor_slice={})
    mocked_partition_router.stream_slices.return_value = [stream_slice]
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    list(cursor.stream_slices())  # generate internal state

    cursor.close_slice(stream_slice)

    underlying_cursor.close_slice.assert_called_once_with(stream_slice.cursor_slice)


def test_given_unknown_partition_when_close_slice_then_raise_error():
    any_cursor_factory = Mock()
    any_partition_router = Mock()
    cursor = PerPartitionCursor(any_cursor_factory, any_partition_router)
    stream_slice = StreamSlice(partition={"unknown_partition": "unknown"}, cursor_slice={})
    with pytest.raises(ValueError):
        cursor.close_slice(stream_slice)


def test_given_unknown_partition_when_should_be_synced_then_raise_error():
    any_cursor_factory = Mock()
    any_partition_router = Mock()
    cursor = PerPartitionCursor(any_cursor_factory, any_partition_router)
    with pytest.raises(ValueError):
        cursor.should_be_synced(
            Record({}, StreamSlice(partition={"unknown_partition": "unknown"}, cursor_slice={}))
        )


@pytest.mark.parametrize(
    "stream_slice, expected_output",
    [
        pytest.param(
            StreamSlice(partition={"partition key": "first partition"}, cursor_slice={}),
            {"cursor": "params", "router": "params"},
            id="first partition",
        ),
        pytest.param(None, None, id="first partition"),
    ],
)
def test_get_request_params(
    mocked_cursor_factory, mocked_partition_router, stream_slice, expected_output
):
    underlying_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    underlying_cursor.get_request_params.return_value = {"cursor": "params"}
    mocked_cursor_factory.create.side_effect = [underlying_cursor]
    mocked_partition_router.stream_slices.return_value = [stream_slice]
    mocked_partition_router.get_request_params.return_value = {"router": "params"}
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    if stream_slice:
        cursor.set_initial_state(
            {"states": [{"partition": stream_slice.partition, "cursor": CURSOR_STATE}]}
        )
        params = cursor.get_request_params(stream_slice=stream_slice)
        assert params == expected_output
        mocked_partition_router.get_request_params.assert_called_once_with(
            stream_state=None, stream_slice=stream_slice, next_page_token=None
        )
        underlying_cursor.get_request_params.assert_called_once_with(
            stream_state=None, stream_slice={}, next_page_token=None
        )
    else:
        with pytest.raises(ValueError):
            cursor.get_request_params(stream_slice=stream_slice)


@pytest.mark.parametrize(
    "stream_slice, expected_output",
    [
        pytest.param(
            StreamSlice(partition={"partition key": "first partition"}, cursor_slice={}),
            {"cursor": "params", "router": "params"},
            id="first partition",
        ),
        pytest.param(None, None, id="first partition"),
    ],
)
def test_get_request_headers(
    mocked_cursor_factory, mocked_partition_router, stream_slice, expected_output
):
    underlying_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    underlying_cursor.get_request_headers.return_value = {"cursor": "params"}
    mocked_cursor_factory.create.side_effect = [underlying_cursor]
    mocked_partition_router.stream_slices.return_value = [stream_slice]
    mocked_partition_router.get_request_headers.return_value = {"router": "params"}
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    if stream_slice:
        cursor.set_initial_state(
            {"states": [{"partition": stream_slice.partition, "cursor": CURSOR_STATE}]}
        )
        params = cursor.get_request_headers(stream_slice=stream_slice)
        assert params == expected_output
        mocked_partition_router.get_request_headers.assert_called_once_with(
            stream_state=None, stream_slice=stream_slice, next_page_token=None
        )
        underlying_cursor.get_request_headers.assert_called_once_with(
            stream_state=None, stream_slice={}, next_page_token=None
        )
    else:
        with pytest.raises(ValueError):
            cursor.get_request_headers(stream_slice=stream_slice)


@pytest.mark.parametrize(
    "stream_slice, expected_output",
    [
        pytest.param(
            StreamSlice(partition={"partition key": "first partition"}, cursor_slice={}),
            {"cursor": "params", "router": "params"},
            id="first partition",
        ),
        pytest.param(None, None, id="first partition"),
    ],
)
def test_get_request_body_data(
    mocked_cursor_factory, mocked_partition_router, stream_slice, expected_output
):
    underlying_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    underlying_cursor.get_request_body_data.return_value = {"cursor": "params"}
    mocked_cursor_factory.create.side_effect = [underlying_cursor]
    mocked_partition_router.stream_slices.return_value = [stream_slice]
    mocked_partition_router.get_request_body_data.return_value = {"router": "params"}
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    if stream_slice:
        cursor.set_initial_state(
            {"states": [{"partition": stream_slice.partition, "cursor": CURSOR_STATE}]}
        )
        params = cursor.get_request_body_data(stream_slice=stream_slice)
        assert params == expected_output
        mocked_partition_router.get_request_body_data.assert_called_once_with(
            stream_state=None, stream_slice=stream_slice, next_page_token=None
        )
        underlying_cursor.get_request_body_data.assert_called_once_with(
            stream_state=None, stream_slice={}, next_page_token=None
        )
    else:
        with pytest.raises(ValueError):
            cursor.get_request_body_data(stream_slice=stream_slice)


@pytest.mark.parametrize(
    "stream_slice, expected_output",
    [
        pytest.param(
            StreamSlice(partition={"partition key": "first partition"}, cursor_slice={}),
            {"cursor": "params", "router": "params"},
            id="first partition",
        ),
        pytest.param(None, None, id="first partition"),
    ],
)
def test_get_request_body_json(
    mocked_cursor_factory, mocked_partition_router, stream_slice, expected_output
):
    underlying_cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )
    underlying_cursor.get_request_body_json.return_value = {"cursor": "params"}
    mocked_cursor_factory.create.side_effect = [underlying_cursor]
    mocked_partition_router.stream_slices.return_value = [stream_slice]
    mocked_partition_router.get_request_body_json.return_value = {"router": "params"}
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    if stream_slice:
        cursor.set_initial_state(
            {"states": [{"partition": stream_slice.partition, "cursor": CURSOR_STATE}]}
        )
        params = cursor.get_request_body_json(stream_slice=stream_slice)
        assert params == expected_output
        mocked_partition_router.get_request_body_json.assert_called_once_with(
            stream_state=None, stream_slice=stream_slice, next_page_token=None
        )
        underlying_cursor.get_request_body_json.assert_called_once_with(
            stream_state=None, stream_slice={}, next_page_token=None
        )
    else:
        with pytest.raises(ValueError):
            cursor.get_request_body_json(stream_slice=stream_slice)


def test_parent_state_is_set_for_per_partition_cursor(
    mocked_cursor_factory, mocked_partition_router
):
    # Define the parent state to be used in the test
    parent_state = {"parent_cursor": "parent_state_value"}

    # Mock the partition router to return a stream slice
    partition = StreamSlice(
        partition={"partition_field_1": "a value", "partition_field_2": "another value"},
        cursor_slice={},
    )
    mocked_partition_router.stream_slices.return_value = [partition]

    # Mock the cursor factory to create cursors with specific states
    mocked_cursor_factory.create.side_effect = [
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .with_stream_state(CURSOR_STATE)
        .build(),
    ]

    # Mock the get_parent_state method to return the parent state
    mocked_partition_router.get_stream_state.return_value = parent_state

    # Initialize the PerPartitionCursor with the mocked cursor factory and partition router
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)

    # Set the initial state, including the parent state
    initial_state = {
        "states": [{"partition": partition.partition, "cursor": CURSOR_STATE}],
        "parent_state": parent_state,
    }
    cursor.set_initial_state(initial_state)

    # Verify that the parent state has been set correctly
    assert cursor.get_stream_state()["parent_state"] == parent_state

    # Verify that set_parent_state was called on the partition router with the initial state
    mocked_partition_router.set_initial_state.assert_called_once_with(initial_state)


def test_get_stream_state_includes_parent_state(mocked_cursor_factory, mocked_partition_router):
    # Define the parent state to be used in the test
    parent_state = {"parent_cursor": "parent_state_value"}

    # Define the expected cursor states
    cursor_state_1 = {CURSOR_STATE_KEY: "first slice cursor value"}
    cursor_state_2 = {CURSOR_STATE_KEY: "second slice cursor value"}

    # Mock the partition router to return stream slices
    partition_1 = {"partition_field_1": "a value", "partition_field_2": "another value"}
    partition_2 = {"partition_field_1": "another value", "partition_field_2": "yet another value"}
    mocked_partition_router.stream_slices.return_value = [
        StreamSlice(partition=partition_1, cursor_slice={}),
        StreamSlice(partition=partition_2, cursor_slice={}),
    ]

    # Mock the cursor factory to create cursors with specific states
    mocked_cursor_factory.create.side_effect = [
        MockedCursorBuilder().with_stream_state(cursor_state_1).build(),
        MockedCursorBuilder().with_stream_state(cursor_state_2).build(),
    ]

    # Mock the get_parent_state method to return the parent state
    mocked_partition_router.get_stream_state.return_value = parent_state

    # Initialize the PerPartitionCursor with the mocked cursor factory and partition router
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)

    # Simulate reading the records to initialize the internal state
    list(cursor.stream_slices())

    # Get the combined stream state
    stream_state = cursor.get_stream_state()

    # Verify that the combined state includes both partition states and the parent state
    expected_state = {
        "states": [
            {"partition": partition_1, "cursor": cursor_state_1},
            {"partition": partition_2, "cursor": cursor_state_2},
        ],
        "parent_state": parent_state,
    }
    assert stream_state == expected_state


def test_per_partition_state_when_set_initial_global_state(
    mocked_cursor_factory, mocked_partition_router
) -> None:
    first_partition = {"first_partition_key": "first_partition_value"}
    second_partition = {"second_partition_key": "second_partition_value"}
    global_state = {"global_state_format_key": "global_state_format_value"}

    mocked_partition_router.stream_slices.return_value = [
        StreamSlice(partition=first_partition, cursor_slice={}),
        StreamSlice(partition=second_partition, cursor_slice={}),
    ]
    mocked_cursor_factory.create.side_effect = [
        MockedCursorBuilder().with_stream_state(global_state).build(),
        MockedCursorBuilder().with_stream_state(global_state).build(),
    ]
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)
    global_state = {"global_state_format_key": "global_state_format_value"}
    cursor.set_initial_state(global_state)
    assert cursor._state_to_migrate_from == global_state
    list(cursor.stream_slices())
    assert (
        cursor._cursor_per_partition[
            '{"first_partition_key":"first_partition_value"}'
        ].set_initial_state.call_count
        == 1
    )
    assert cursor._cursor_per_partition[
        '{"first_partition_key":"first_partition_value"}'
    ].set_initial_state.call_args[0] == ({"global_state_format_key": "global_state_format_value"},)
    assert (
        cursor._cursor_per_partition[
            '{"second_partition_key":"second_partition_value"}'
        ].set_initial_state.call_count
        == 1
    )
    assert cursor._cursor_per_partition[
        '{"second_partition_key":"second_partition_value"}'
    ].set_initial_state.call_args[0] == ({"global_state_format_key": "global_state_format_value"},)
    expected_state = [
        {
            "cursor": {"global_state_format_key": "global_state_format_value"},
            "partition": {"first_partition_key": "first_partition_value"},
        },
        {
            "cursor": {"global_state_format_key": "global_state_format_value"},
            "partition": {"second_partition_key": "second_partition_value"},
        },
    ]
    assert cursor.get_stream_state()["states"] == expected_state


def test_per_partition_cursor_partition_router_extra_fields(
    mocked_cursor_factory, mocked_partition_router
):
    first_partition = {"first_partition_key": "first_partition_value"}
    mocked_partition_router.stream_slices.return_value = [
        StreamSlice(
            partition=first_partition, cursor_slice={}, extra_fields={"extra_field": "extra_value"}
        ),
    ]
    cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )

    mocked_cursor_factory.create.return_value = cursor
    cursor = PerPartitionCursor(mocked_cursor_factory, mocked_partition_router)

    cursor.set_initial_state({"states": [{"partition": first_partition, "cursor": CURSOR_STATE}]})
    slices = list(cursor.stream_slices())

    assert slices[0].extra_fields == {"extra_field": "extra_value"}
    assert slices == [
        StreamSlice(
            partition={"first_partition_key": "first_partition_value"},
            cursor_slice={CURSOR_SLICE_FIELD: "first slice cursor value"},
            extra_fields={"extra_field": "extra_value"},
        )
    ]


def test_global_cursor_partition_router_extra_fields(
    mocked_cursor_factory, mocked_partition_router
):
    first_partition = {"first_partition_key": "first_partition_value"}
    mocked_partition_router.stream_slices.return_value = [
        StreamSlice(
            partition=first_partition, cursor_slice={}, extra_fields={"extra_field": "extra_value"}
        ),
    ]
    cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )

    global_cursor = GlobalSubstreamCursor(cursor, mocked_partition_router)

    slices = list(global_cursor.stream_slices())

    assert slices[0].extra_fields == {"extra_field": "extra_value"}
    assert slices == [
        StreamSlice(
            partition=first_partition,
            cursor_slice={CURSOR_SLICE_FIELD: "first slice cursor value"},
            extra_fields={"extra_field": "extra_value"},
        )
    ]
