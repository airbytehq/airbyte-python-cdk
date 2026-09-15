#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.transformations import AddFields, ForEach, RemoveFields
from airbyte_cdk.sources.declarative.transformations.add_fields import AddedFieldDefinition
from airbyte_cdk.sources.declarative.transformations.keys_to_lower_transformation import (
    KeysToLowerTransformation,
)
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


def _add_fields(path: list[str], value: str, condition: str = "") -> AddFields:
    return AddFields(
        fields=[
            AddedFieldDefinition(path=path, value=value, value_type=None, parameters={}),
        ],
        condition=condition,
        parameters={},
    )


def _for_each(
    field_path: list[str],
    transformations: list[Any],
    config: Mapping[str, Any] | None = None,
    parameters: Mapping[str, Any] | None = None,
) -> ForEach:
    return ForEach(
        config=config or {},
        field_path=field_path,
        transformations=transformations,
        parameters=parameters or {},
    )


def test_monday_use_case_copies_display_value_into_text():
    """
    The `MondayTransformation` custom component, expressed as a ForEach. See
    airbyte-integrations/connectors/source-monday/components.py:463-472 in the airbyte monorepo.
    """
    transformation = _for_each(
        field_path=["column_values"],
        transformations=[
            _add_fields(
                path=["text"],
                value="{{ record['display_value'] }}",
                condition="{{ record.get('display_value') and not record.get('text') }}",
            )
        ],
    )
    record = {
        "id": 1,
        "column_values": [
            {"id": 11, "text": None, "display_value": "Hola amigo!"},
            {"id": 12, "text": "already set", "display_value": "ignored"},
            {"id": 13, "text": None},
        ],
    }

    transformation.transform(record)

    assert record["column_values"] == [
        {"id": 11, "text": "Hola amigo!", "display_value": "Hola amigo!"},
        {"id": 12, "text": "already set", "display_value": "ignored"},
        {"id": 13, "text": None},
    ]


def test_transform_returns_none():
    transformation = _for_each(
        field_path=["items"], transformations=[_add_fields(["added"], "value")]
    )

    assert transformation.transform({"items": [{}]}) is None


def test_mutation_is_visible_on_the_parent_record_without_copying():
    transformation = _for_each(
        field_path=["items"], transformations=[_add_fields(["added"], "value")]
    )
    element = {"id": 1}
    collection = [element]
    record = {"items": collection}

    transformation.transform(record)

    # the very same objects were mutated, nothing was copied and written back
    assert record["items"] is collection
    assert record["items"][0] is element
    assert element == {"id": 1, "added": "value"}


def test_nested_collection_is_reached_through_a_multi_segment_path():
    transformation = _for_each(
        field_path=["data", "assets"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"data": {"assets": [{"id": 1}, {"id": 2}]}}

    transformation.transform(record)

    assert record == {
        "data": {"assets": [{"id": 1, "added": "value"}, {"id": 2, "added": "value"}]}
    }


def test_wildcard_segment_iterates_every_matching_collection():
    transformation = _for_each(
        field_path=["data", "*", "items"], transformations=[_add_fields(["added"], "value")]
    )
    record = {
        "data": {
            "first": {"items": [{"id": 1}]},
            "second": {"items": [{"id": 2}]},
            "third": {"other": [{"id": 3}]},
        }
    }

    transformation.transform(record)

    assert record == {
        "data": {
            "first": {"items": [{"id": 1, "added": "value"}]},
            "second": {"items": [{"id": 2, "added": "value"}]},
            "third": {"other": [{"id": 3}]},
        }
    }


@pytest.mark.parametrize(
    "record",
    [
        pytest.param({"id": 1}, id="field_path_is_absent"),
        pytest.param({"id": 1, "items": None}, id="field_path_resolves_to_null"),
        pytest.param({"id": 1, "items": "a string"}, id="field_path_resolves_to_a_string"),
        pytest.param({"id": 1, "items": 42}, id="field_path_resolves_to_a_number"),
        pytest.param({"id": 1, "items": []}, id="field_path_resolves_to_an_empty_list"),
        pytest.param({"id": 1, "other": [{"a": 1}]}, id="a_sibling_field_exists"),
    ],
)
def test_unresolvable_or_non_collection_field_path_is_a_no_op(record):
    transformation = _for_each(
        field_path=["items"], transformations=[_add_fields(["added"], "value")]
    )
    expected = {key: value for key, value in record.items()}

    transformation.transform(record)

    assert record == expected


def test_missing_nested_field_path_is_a_no_op():
    transformation = _for_each(
        field_path=["data", "items"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"data": {"something_else": 1}}

    transformation.transform(record)

    assert record == {"data": {"something_else": 1}}


def test_object_target_is_treated_as_a_collection_of_one():
    transformation = _for_each(
        field_path=["item"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"item": {"id": 1}}

    transformation.transform(record)

    assert record == {"item": {"id": 1, "added": "value"}}


def test_non_object_element_raises_a_config_error_naming_the_field_path():
    transformation = _for_each(
        field_path=["tags"],
        transformations=[_add_fields(["added"], "value")],
        parameters={"name": "the_stream"},
    )
    record = {"tags": ["a", "b"]}

    with pytest.raises(AirbyteTracedException) as exc_info:
        transformation.transform(record)

    assert exc_info.value.failure_type == FailureType.config_error
    assert "the_stream" in exc_info.value.message
    assert "['tags']" in exc_info.value.message
    assert "str" in exc_info.value.message
    # nothing was mutated before the error was raised
    assert record == {"tags": ["a", "b"]}


def test_non_object_element_error_without_a_stream_name():
    transformation = _for_each(
        field_path=["tags"], transformations=[_add_fields(["added"], "value")]
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        transformation.transform({"tags": [1]})

    assert "['tags']" in exc_info.value.message
    assert "int" in exc_info.value.message


def test_nested_transformations_receive_config_and_stream_slice():
    transformation = _for_each(
        field_path=["items"],
        transformations=[
            _add_fields(["shop_id"], "{{ config['shop_id'] }}"),
            _add_fields(["partition_id"], "{{ stream_slice['partition_id'] }}"),
        ],
    )
    record = {"items": [{"id": 1}]}

    transformation.transform(
        record,
        config={"shop_id": "my-shop"},
        stream_slice=StreamSlice(partition={"partition_id": "p1"}, cursor_slice={}),
    )

    assert record == {"items": [{"id": 1, "shop_id": "my-shop", "partition_id": "p1"}]}


def test_field_path_is_interpolated():
    transformation = _for_each(
        field_path=["{{ config['collection_field'] }}"],
        transformations=[_add_fields(["added"], "value")],
        config={"collection_field": "items"},
    )
    record = {"items": [{"id": 1}]}

    transformation.transform(record)

    assert record == {"items": [{"id": 1, "added": "value"}]}


def test_remove_fields_inside_the_loop():
    """
    RemoveFields mutates through `dpath.delete` rather than `dpath.new`, so it exercises a different
    mutation path than AddFields.
    """
    transformation = _for_each(
        field_path=["assets"],
        transformations=[
            _add_fields(["uploader_id"], "{{ record['uploader']['id'] }}"),
            RemoveFields(field_pointers=[["uploader"]], parameters={}),
        ],
    )
    record = {"assets": [{"id": 1, "uploader": {"id": 7}}, {"id": 2, "uploader": {"id": 8}}]}

    transformation.transform(record)

    assert record == {"assets": [{"id": 1, "uploader_id": 7}, {"id": 2, "uploader_id": 8}]}


def test_transformation_that_ignores_the_config_still_works_inside_the_loop():
    transformation = _for_each(field_path=["items"], transformations=[KeysToLowerTransformation()])
    record = {"items": [{"ID": 1}, {"Name": "a"}]}

    transformation.transform(record)

    assert record == {"items": [{"id": 1}, {"name": "a"}]}


def test_for_each_nested_inside_for_each():
    inner = _for_each(
        field_path=["column_values"],
        transformations=[_add_fields(["added"], "{{ config['marker'] }}")],
    )
    outer = _for_each(field_path=["items"], transformations=[inner])
    record = {
        "items": [
            {"id": 1, "column_values": [{"c": 1}, {"c": 2}]},
            {"id": 2, "column_values": [{"c": 3}]},
            {"id": 3},
        ]
    }

    outer.transform(record, config={"marker": "x"})

    assert record == {
        "items": [
            {"id": 1, "column_values": [{"c": 1, "added": "x"}, {"c": 2, "added": "x"}]},
            {"id": 2, "column_values": [{"c": 3, "added": "x"}]},
            {"id": 3},
        ]
    }


def test_multiple_nested_transformations_are_applied_in_order():
    transformation = _for_each(
        field_path=["items"],
        transformations=[
            _add_fields(["first"], "a"),
            _add_fields(["second"], "{{ record['first'] }}b"),
        ],
    )
    record = {"items": [{}]}

    transformation.transform(record)

    assert record == {"items": [{"first": "a", "second": "ab"}]}
