#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from unittest.mock import MagicMock, Mock, call

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk.sources.declarative.extractors.record_filter import RecordFilter
from airbyte_cdk.sources.declarative.extractors.record_selector import RecordSelector
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer


@pytest.mark.parametrize(
    "test_name, field_path, filter_template, body, expected_data",
    [
        (
            "test_with_extractor_and_filter",
            ["data"],
            "{{ record['created_at'] > stream_state['created_at'] }}",
            {
                "data": [
                    {"id": 1, "created_at": "06-06-21"},
                    {"id": 2, "created_at": "06-07-21"},
                    {"id": 3, "created_at": "06-08-21"},
                ]
            },
            [{"id": 2, "created_at": "06-07-21"}, {"id": 3, "created_at": "06-08-21"}],
        ),
        (
            "test_no_record_filter_returns_all_records",
            ["data"],
            None,
            {"data": [{"id": 1, "created_at": "06-06-21"}, {"id": 2, "created_at": "06-07-21"}]},
            [{"id": 1, "created_at": "06-06-21"}, {"id": 2, "created_at": "06-07-21"}],
        ),
        (
            "test_with_extractor_and_filter_with_parameters",
            ["{{ parameters['parameters_field'] }}"],
            "{{ record['created_at'] > parameters['created_at'] }}",
            {
                "data": [
                    {"id": 1, "created_at": "06-06-21"},
                    {"id": 2, "created_at": "06-07-21"},
                    {"id": 3, "created_at": "06-08-21"},
                ]
            },
            [{"id": 3, "created_at": "06-08-21"}],
        ),
        (
            "test_read_single_record",
            ["data"],
            None,
            {"data": {"id": 1, "created_at": "06-06-21"}},
            [{"id": 1, "created_at": "06-06-21"}],
        ),
        (
            "test_no_record",
            ["data"],
            None,
            {"data": []},
            [],
        ),
        (
            "test_no_record_from_root",
            [],
            None,
            [],
            [],
        ),
    ],
)
def test_record_filter(test_name, field_path, filter_template, body, expected_data):
    config = {"response_override": "stop_if_you_see_me"}
    parameters = {"parameters_field": "data", "created_at": "06-07-21"}
    stream_state = {"created_at": "06-06-21"}
    stream_slice = StreamSlice(partition={}, cursor_slice={"last_seen": "06-10-21"})
    next_page_token = {"last_seen_id": 14}
    schema = create_schema()
    first_transformation = Mock(spec=RecordTransformation)
    second_transformation = Mock(spec=RecordTransformation)
    transformations = [first_transformation, second_transformation]

    response = create_response(body)
    decoder = JsonDecoder(parameters={})
    extractor = DpathExtractor(
        field_path=field_path, decoder=decoder, config=config, parameters=parameters
    )
    if filter_template is None:
        record_filter = None
    else:
        record_filter = RecordFilter(
            config=config, condition=filter_template, parameters=parameters
        )
    record_selector = RecordSelector(
        extractor=extractor,
        record_filter=record_filter,
        transformations=transformations,
        config=config,
        parameters=parameters,
        schema_normalization=TypeTransformer(TransformConfig.NoTransform),
    )

    actual_records = list(
        record_selector.select_records(
            response=response,
            records_schema=schema,
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )
    )
    assert actual_records == [
        Record(data=data, associated_slice=stream_slice, stream_name="") for data in expected_data
    ]

    calls = []
    for record in expected_data:
        calls.append(
            call(record, config=config, stream_state=stream_state, stream_slice=stream_slice)
        )
    for transformation in transformations:
        assert transformation.transform.call_count == len(expected_data)
        transformation.transform.assert_has_calls(calls)


@pytest.mark.parametrize(
    "test_name, schema, schema_transformation, body, expected_data",
    [
        (
            "test_with_empty_schema",
            {},
            TransformConfig.NoTransform,
            {
                "data": [
                    {"id": 1, "created_at": "06-06-21", "field_int": "100", "field_float": "123.3"}
                ]
            },
            [{"id": 1, "created_at": "06-06-21", "field_int": "100", "field_float": "123.3"}],
        ),
        (
            "test_with_schema_none_normalizer",
            {},
            TransformConfig.NoTransform,
            {
                "data": [
                    {"id": 1, "created_at": "06-06-21", "field_int": "100", "field_float": "123.3"}
                ]
            },
            [{"id": 1, "created_at": "06-06-21", "field_int": "100", "field_float": "123.3"}],
        ),
        (
            "test_with_schema_and_default_normalizer",
            {},
            TransformConfig.DefaultSchemaNormalization,
            {
                "data": [
                    {"id": 1, "created_at": "06-06-21", "field_int": "100", "field_float": "123.3"}
                ]
            },
            [{"id": "1", "created_at": "06-06-21", "field_int": 100, "field_float": 123.3}],
        ),
    ],
)
def test_schema_normalization(test_name, schema, schema_transformation, body, expected_data):
    config = {"response_override": "stop_if_you_see_me"}
    parameters = {"parameters_field": "data", "created_at": "06-07-21"}
    stream_state = {"created_at": "06-06-21"}
    stream_slice = {"last_seen": "06-10-21"}
    next_page_token = {"last_seen_id": 14}

    response = create_response(body)
    schema = create_schema()
    decoder = JsonDecoder(parameters={})
    extractor = DpathExtractor(
        field_path=["data"], decoder=decoder, config=config, parameters=parameters
    )
    record_selector = RecordSelector(
        extractor=extractor,
        record_filter=None,
        transformations=[],
        config=config,
        parameters=parameters,
        schema_normalization=TypeTransformer(schema_transformation),
    )

    actual_records = list(
        record_selector.select_records(
            response=response,
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
            records_schema=schema,
        )
    )

    assert actual_records == [Record(data, stream_slice) for data in expected_data]


def create_response(body):
    response = requests.Response()
    response._content = json.dumps(body).encode("utf-8")
    return response


def create_schema():
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "created_at": {"type": "string"},
            "field_int": {"type": "integer"},
            "field_float": {"type": "number"},
        },
    }


@pytest.mark.parametrize("transform_before_filtering", [True, False])
def test_transform_before_filtering(transform_before_filtering):
    """
    Verify that when transform_before_filtering=True, records are modified before
    filtering. When False, the filter sees the original record data first.
    """

    # 1) Our response body with 'myfield' set differently
    #    The first record has myfield=0 (needs transformation to pass)
    #    The second record has myfield=999 (already passes the filter)
    body = {"data": [{"id": 1, "myfield": 0}, {"id": 2, "myfield": 999}]}

    # 2) A response object
    response = requests.Response()
    response._content = json.dumps(body).encode("utf-8")

    # 3) A simple extractor pulling records from 'data'
    extractor = DpathExtractor(
        field_path=["data"], decoder=JsonDecoder(parameters={}), config={}, parameters={}
    )

    # 4) A filter that keeps only records whose 'myfield' == 999
    #    i.e.: "{{ record['myfield'] == 999 }}"
    record_filter = RecordFilter(
        config={},
        condition="{{ record['myfield'] == 999 }}",
        parameters={},
    )

    # 5) A transformation that sets 'myfield' to 999
    #    We'll attach it to a mock so we can confirm how many times it was called
    transformation_mock = MagicMock(spec=RecordTransformation)

    def transformation_side_effect(record, config, stream_state, stream_slice):
        record["myfield"] = 999

    transformation_mock.transform.side_effect = transformation_side_effect

    # 6) Create a RecordSelector with transform_before_filtering set from our param
    record_selector = RecordSelector(
        extractor=extractor,
        config={},
        name="test_stream",
        record_filter=record_filter,
        transformations=[transformation_mock],
        schema_normalization=TypeTransformer(TransformConfig.NoTransform),
        transform_before_filtering=transform_before_filtering,
        parameters={},
    )

    # 7) Collect records
    stream_slice = StreamSlice(partition={}, cursor_slice={})
    actual_records = list(
        record_selector.select_records(
            response=response,
            records_schema={},  # not using schema in this test
            stream_state={},
            stream_slice=stream_slice,
            next_page_token=None,
        )
    )

    # 8) Assert how many records survive
    if transform_before_filtering:
        # Both records become myfield=999 BEFORE the filter => both pass
        assert len(actual_records) == 2
        # The transformation should be called 2x (once per record)
        assert transformation_mock.transform.call_count == 2
    else:
        # The first record is myfield=0 when the filter sees it => filter excludes it
        # The second record is myfield=999 => filter includes it
        assert len(actual_records) == 1
        # The transformation occurs only on that single surviving record
        #   (the filter is done first, so the first record is already dropped)
        assert transformation_mock.transform.call_count == 1

    # 9) Check final record data
    #    If transform_before_filtering=True => we have records [1,2] both with myfield=999
    #    If transform_before_filtering=False => we have record [2] with myfield=999
    final_record_data = [r.data for r in actual_records]
    if transform_before_filtering:
        assert all(record["myfield"] == 999 for record in final_record_data)
        assert sorted([r["id"] for r in final_record_data]) == [1, 2]
    else:
        assert final_record_data[0]["id"] == 2
        assert final_record_data[0]["myfield"] == 999
