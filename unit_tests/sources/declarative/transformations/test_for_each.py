#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import copy
import time
from typing import Any, Mapping
from unittest.mock import patch

import dpath
import pytest
from jsonschema import ValidationError, validate

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    _get_declarative_component_schema,
)
from airbyte_cdk.sources.declarative.transformations import (
    AddFields,
    ForEach,
    RecordTransformation,
    RemoveFields,
)
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
    expected = copy.deepcopy(record)

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


def test_null_element_inside_the_collection_is_skipped():
    """
    A `null` in an array is ordinary API payload, not a manifest mistake. The Python transformations
    this component replaces (for example `MondayTransformation`) skip it, so ForEach must too.
    """
    transformation = _for_each(
        field_path=["items"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"items": [{"id": 1}, None, {"id": 2}]}

    transformation.transform(record)

    assert record == {"items": [{"id": 1, "added": "value"}, None, {"id": 2, "added": "value"}]}


def test_collection_of_only_nulls_is_a_no_op():
    transformation = _for_each(
        field_path=["items"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"items": [None, None]}

    transformation.transform(record)

    assert record == {"items": [None, None]}


@pytest.mark.parametrize(
    "collection, expected_type_name",
    [
        pytest.param(["a", "b"], "str", id="strings"),
        pytest.param([1], "int", id="ints"),
        pytest.param([[{"id": 1}]], "list", id="nested_lists"),
    ],
)
def test_non_object_element_raises_a_system_error_naming_the_field_path(
    collection, expected_type_name
):
    """
    Unlike a `null`, a string or a number in the collection means `field_path` points at the wrong
    thing. That is a connector-development fault, so it is a `system_error` rather than something the
    user is told to fix in their connection configuration.
    """
    transformation = _for_each(
        field_path=["tags"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"tags": collection}

    with pytest.raises(AirbyteTracedException) as exc_info:
        transformation.transform(record)

    assert exc_info.value.failure_type == FailureType.system_error
    assert "['tags']" in exc_info.value.message
    assert expected_type_name in exc_info.value.message
    assert record == {"tags": collection}


def test_the_error_message_does_not_promise_a_stream_name():
    """
    `name` is a DeclarativeStream field, not a `$parameter`, so a stream name sourced from
    `$parameters` never materialises for a modern manifest. The message must not claim one.
    """
    transformation = _for_each(
        field_path=["tags"],
        transformations=[_add_fields(["added"], "value")],
        parameters={"name": "the_stream"},
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        transformation.transform({"tags": ["a"]})

    assert "the_stream" not in exc_info.value.message
    assert "stream" not in exc_info.value.message


def test_nothing_is_mutated_when_a_later_element_of_the_same_collection_is_invalid():
    transformation = _for_each(
        field_path=["tags"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"tags": [{"id": 1}, "bad"]}

    with pytest.raises(AirbyteTracedException):
        transformation.transform(record)

    assert record == {"tags": [{"id": 1}, "bad"]}


def test_nothing_is_mutated_when_a_later_collection_under_a_wildcard_is_invalid():
    """
    The whole-record pre-pass has to cover every collection the glob expands to, not just the one
    currently being iterated, or the record is left half-transformed.
    """
    transformation = _for_each(
        field_path=["data", "*", "items"], transformations=[_add_fields(["added"], "value")]
    )
    record = {
        "data": {
            "first": {"items": [{"id": 1}]},
            "second": {"items": [{"id": 2}]},
            "third": {"items": ["bad"]},
        }
    }

    with pytest.raises(AirbyteTracedException):
        transformation.transform(record)

    assert record == {
        "data": {
            "first": {"items": [{"id": 1}]},
            "second": {"items": [{"id": 2}]},
            "third": {"items": ["bad"]},
        }
    }


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


def test_empty_field_path_applies_the_transformations_to_the_record_itself():
    transformation = _for_each(field_path=[], transformations=[_add_fields(["added"], "value")])
    record = {"id": 1}

    transformation.transform(record)

    assert record == {"id": 1, "added": "value"}


class _RecordingTransformation(RecordTransformation):
    def __init__(self):
        self.calls = []

    def transform(self, record, config=None, stream_state=None, stream_slice=None) -> None:
        self.calls.append(
            {
                "record": record,
                "config": config,
                "stream_state": stream_state,
                "stream_slice": stream_slice,
            }
        )


def test_stream_state_and_stream_slice_are_forwarded_to_the_nested_transformations():
    recorder = _RecordingTransformation()
    transformation = _for_each(field_path=["items"], transformations=[recorder], config={"a": 1})
    element = {"id": 1}
    stream_slice = StreamSlice(partition={"partition_id": "p1"}, cursor_slice={})

    transformation.transform(
        {"items": [element]}, stream_state={"cursor": "2024-01-01"}, stream_slice=stream_slice
    )

    assert recorder.calls == [
        {
            "record": element,
            "config": {"a": 1},
            "stream_state": {"cursor": "2024-01-01"},
            "stream_slice": stream_slice,
        }
    ]


def test_field_path_is_interpolated_from_parameters():
    transformation = _for_each(
        field_path=["{{ parameters['collection_field'] }}"],
        transformations=[_add_fields(["added"], "value")],
        parameters={"collection_field": "items"},
    )
    record = {"items": [{"id": 1}]}

    transformation.transform(record)

    assert record == {"items": [{"id": 1, "added": "value"}]}


@pytest.mark.parametrize(
    "config",
    [
        pytest.param({}, id="segment_interpolates_to_an_empty_string"),
        pytest.param({"collection_field": 1.5}, id="segment_interpolates_to_a_float"),
        pytest.param({"collection_field": True}, id="segment_interpolates_to_a_boolean"),
        pytest.param({"collection_field": None}, id="segment_interpolates_to_null"),
        pytest.param({"collection_field": ["items"]}, id="segment_interpolates_to_a_list"),
    ],
)
def test_a_field_path_segment_that_is_not_a_non_empty_string_is_a_config_error(config):
    """
    Without this check the component is a silent no-op for the entire sync, which is the hardest
    failure mode to diagnose.
    """
    transformation = _for_each(
        field_path=["{{ config['collection_field'] }}"],
        transformations=[_add_fields(["added"], "value")],
        config=config,
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        transformation.transform({"items": [{"id": 1}]})

    assert exc_info.value.failure_type == FailureType.config_error


@pytest.mark.parametrize(
    "field_path",
    [
        pytest.param(["**", "items"], id="recursive_glob"),
        pytest.param(["data", "?", "items"], id="single_character_glob"),
        pytest.param(["data", "item*"], id="partial_glob"),
    ],
)
def test_non_star_globs_match_several_collections_instead_of_raising(field_path):
    """
    `dpath.get` raises a bare `ValueError` as soon as a glob matches more than one leaf. Routing every
    glob through `dpath.values` keeps the multi-match case working.
    """
    transformation = _for_each(
        field_path=field_path, transformations=[_add_fields(["added"], "value")]
    )
    record = {"data": {"a": {"items": [{"id": 1}]}, "items": [{"id": 2}], "itemz": [{"id": 3}]}}

    transformation.transform(record)

    assert any(
        element.get("added") == "value"
        for collection in (
            record["data"]["a"]["items"],
            record["data"]["items"],
            record["data"]["itemz"],
        )
        for element in collection
    )


@pytest.mark.parametrize(
    "index, expected_position",
    [
        pytest.param("-1", 2, id="minus_one_is_the_last_element"),
        pytest.param("-2", 1, id="minus_two_is_the_second_to_last_element"),
        pytest.param("-3", 0, id="minus_three_is_the_first_element"),
        pytest.param("-0", 0, id="minus_zero_is_the_first_element"),
        pytest.param("0", 0, id="zero_is_the_first_element"),
        pytest.param("2", 2, id="a_positive_index_still_works"),
    ],
)
def test_a_list_index_segment_selects_a_single_element(index, expected_position):
    """
    `dpath` supports negative list indices, so the plain-walk fast path has to as well, or the same
    `field_path` resolves differently depending on whether the path happens to contain a glob.
    """
    transformation = _for_each(
        field_path=["a", index], transformations=[_add_fields(["added"], "value")]
    )
    record = {"a": [{"id": 0}, {"id": 1}, {"id": 2}]}

    transformation.transform(record)

    assert record["a"][expected_position] == {"id": expected_position, "added": "value"}
    assert [element for element in record["a"] if "added" in element] == [
        {"id": expected_position, "added": "value"}
    ]


@pytest.mark.parametrize(
    "segment",
    [
        pytest.param("-4", id="negative_index_past_the_start"),
        pytest.param("3", id="positive_index_past_the_end"),
        pytest.param("\u00b2", id="superscript_two_is_a_digit_that_int_rejects"),
        pytest.param("\u2155", id="vulgar_fraction_is_numeric_but_not_an_index"),
        pytest.param("abc", id="a_plain_word_against_a_list"),
    ],
)
def test_a_segment_that_is_not_an_in_range_list_index_is_a_no_op(segment):
    """
    `"\u00b2".isdigit()` is `True` but `int("\u00b2")` raises, so classifying list segments with
    `isdigit()` turned a documented no-op into an uncaught `ValueError`.
    """
    transformation = _for_each(
        field_path=["a", segment], transformations=[_add_fields(["added"], "value")]
    )
    record = {"a": [{"id": 0}, {"id": 1}, {"id": 2}]}
    expected = copy.deepcopy(record)

    transformation.transform(record)

    assert record == expected


@pytest.mark.parametrize(
    "segment",
    [
        "-4",
        "-3",
        "-1",
        "-0",
        "0",
        "2",
        "3",
        "+1",
        " 1",
        "1_0",
        "\u00b2",
        "\u2155",
        "abc",
        "",
    ],
)
def test_the_plain_walk_resolves_a_list_segment_exactly_like_dpath(segment):
    record = {"a": [{"id": index} for index in range(3)]}

    assert ForEach._resolve_collections(record, ["a", segment]) == list(
        dpath.values(copy.deepcopy(record), ["a", segment])
    )


def test_a_literal_empty_field_path_segment_is_a_manifest_error():
    """
    An empty segment cannot be the render of a template, so it is the manifest's fault, not the
    config's -- the same call `_elements_of` makes for a mis-pointed `field_path`.
    """
    with pytest.raises(AirbyteTracedException) as exc_info:
        _for_each(field_path=["items", ""], transformations=[_add_fields(["added"], "value")])

    assert exc_info.value.failure_type == FailureType.system_error


def test_a_numeric_config_value_is_a_valid_list_index_rather_than_a_config_error():
    transformation = _for_each(
        field_path=["a", "{{ config['index'] }}"],
        transformations=[_add_fields(["added"], "value")],
        config={"index": -1},
    )
    record = {"a": [{"id": 0}, {"id": 1}]}

    transformation.transform(record)

    assert record == {"a": [{"id": 0}, {"id": 1, "added": "value"}]}


def test_a_path_without_a_glob_never_goes_through_dpath():
    """
    `dpath` resolves by folding over the whole object graph, so it costs O(total nodes in the record)
    per call. The plain-walk fast path is what keeps a large unrelated sibling from dominating.
    """
    transformation = _for_each(
        field_path=["data", "items"], transformations=[_add_fields(["added"], "value")]
    )
    record = {"data": {"items": [{"id": 1}]}, "junk": [{"n": n} for n in range(100)]}

    with patch("airbyte_cdk.sources.declarative.transformations.for_each.dpath") as mocked_dpath:
        transformation.transform(record)

    mocked_dpath.get.assert_not_called()
    mocked_dpath.values.assert_not_called()
    assert record["data"]["items"] == [{"id": 1, "added": "value"}]


def test_a_large_unrelated_sibling_does_not_slow_down_the_lookup():
    transformation = _for_each(
        field_path=["items"], transformations=[_add_fields(["added"], "value")]
    )
    junk = [{"n": n} for n in range(20_000)]

    started_at = time.perf_counter()
    for _ in range(500):
        transformation.transform({"items": [{"id": 1}], "junk": junk})
    elapsed = time.perf_counter() - started_at

    # Resolving through `dpath.get` takes ~35 ms per record here, so the same loop takes ~17 s.
    assert elapsed < 2.0, (
        f"resolving `field_path` scaled with the unrelated sibling: {elapsed:.2f}s"
    )


def test_equality_compares_the_configured_fields_and_tolerates_foreign_types():
    def build(field_path):
        return _for_each(field_path=field_path, transformations=[_add_fields(["added"], "value")])

    assert build(["items"]) == build(["items"])
    assert build(["items"]) != build(["other"])
    assert build(["items"]) != 1


_FOR_EACH_MANIFEST_FRAGMENT = {
    "type": "ForEach",
    "field_path": ["column_values"],
    "transformations": [
        {
            "type": "AddFields",
            "fields": [{"type": "AddedFieldDefinition", "path": ["text"], "value": "a value"}],
        },
        {
            "type": "ForEach",
            "field_path": ["nested"],
            "transformations": [{"type": "RemoveFields", "field_pointers": [["uploader"]]}],
        },
        {"type": "CustomTransformation", "class_name": "source_x.components.MyTransformation"},
    ],
}


def _definition_schema(definition_name: str) -> dict[str, Any]:
    schema = _get_declarative_component_schema()
    # The nested `$ref`s are all `#/definitions/...`, so they resolve as long as `definitions` sits
    # at the root of the schema handed to `validate`.
    return {**schema["definitions"][definition_name], "definitions": schema["definitions"]}


def test_a_for_each_component_validates_against_the_declarative_component_schema():
    validate(instance=_FOR_EACH_MANIFEST_FRAGMENT, schema=_definition_schema("ForEach"))


@pytest.mark.parametrize(
    "definition_name, transformations_field",
    [
        pytest.param("DeclarativeStream", "transformations", id="declarative_stream"),
        pytest.param("DynamicSchemaLoader", "schema_transformations", id="dynamic_schema_loader"),
        pytest.param(
            "JsonSchemaPropertySelector", "transformations", id="json_schema_property_selector"
        ),
        pytest.param("ForEach", "transformations", id="for_each"),
    ],
)
def test_for_each_is_accepted_by_every_transformations_slot(definition_name, transformations_field):
    """
    Regression guard for the four `$ref` sites. `create_component` never validates against the JSON
    schema, so dropping `ForEach` from one of these `anyOf` lists would otherwise only surface in a
    real connector.
    """
    schema = _get_declarative_component_schema()
    property_schema = {
        **schema["definitions"][definition_name]["properties"][transformations_field],
        "definitions": schema["definitions"],
    }

    validate(instance=[_FOR_EACH_MANIFEST_FRAGMENT], schema=property_schema)


def test_a_for_each_without_a_field_path_is_rejected_by_the_schema():
    invalid = {
        key: value for key, value in _FOR_EACH_MANIFEST_FRAGMENT.items() if key != "field_path"
    }

    with pytest.raises(ValidationError):
        validate(instance=invalid, schema=_definition_schema("ForEach"))
