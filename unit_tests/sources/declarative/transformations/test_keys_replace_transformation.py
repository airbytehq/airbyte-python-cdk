#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import pytest

from airbyte_cdk.sources.declarative.transformations.keys_replace_transformation import (
    KeysReplaceTransformation,
)

_ANY_VALUE = -1


@pytest.mark.parametrize(
    [
        "input_record",
        "config",
        "stream_state",
        "stream_slice",
        "keys_replace_config",
        "expected_record",
    ],
    [
        pytest.param(
            {"date time": _ANY_VALUE, "customer id": _ANY_VALUE},
            {},
            {},
            {},
            {"old": " ", "new": "_"},
            {"date_time": _ANY_VALUE, "customer_id": _ANY_VALUE},
            id="simple keys replace config",
        ),
        pytest.param(
            {
                "customer_id": 111111,
                "customer_name": "MainCustomer",
                "field_1_111111": _ANY_VALUE,
                "field_2_111111": _ANY_VALUE,
            },
            {},
            {},
            {},
            {"old": '{{ record["customer_id"] }}', "new": '{{ record["customer_name"] }}'},
            {
                "customer_id": 111111,
                "customer_name": "MainCustomer",
                "field_1_MainCustomer": _ANY_VALUE,
                "field_2_MainCustomer": _ANY_VALUE,
            },
            id="keys replace config uses values from record",
        ),
        pytest.param(
            {"customer_id": 111111, "field_1_111111": _ANY_VALUE, "field_2_111111": _ANY_VALUE},
            {},
            {},
            {"customer_name": "MainCustomer"},
            {"old": '{{ record["customer_id"] }}', "new": '{{ stream_slice["customer_name"] }}'},
            {
                "customer_id": 111111,
                "field_1_MainCustomer": _ANY_VALUE,
                "field_2_MainCustomer": _ANY_VALUE,
            },
            id="keys replace config uses values from slice",
        ),
        pytest.param(
            {"customer_id": 111111, "field_1_111111": _ANY_VALUE, "field_2_111111": _ANY_VALUE},
            {"customer_name": "MainCustomer"},
            {},
            {},
            {"old": '{{ record["customer_id"] }}', "new": '{{ config["customer_name"] }}'},
            {
                "customer_id": 111111,
                "field_1_MainCustomer": _ANY_VALUE,
                "field_2_MainCustomer": _ANY_VALUE,
            },
            id="keys replace config uses values from config",
        ),
        pytest.param(
            {
                "date time": _ANY_VALUE,
                "user id": _ANY_VALUE,
                "customer": {
                    "customer name": _ANY_VALUE,
                    "customer id": _ANY_VALUE,
                    "contact info": {"email": _ANY_VALUE, "phone number": _ANY_VALUE},
                },
            },
            {},
            {},
            {},
            {"old": " ", "new": "_"},
            {
                "customer": {
                    "contact_info": {"email": _ANY_VALUE, "phone_number": _ANY_VALUE},
                    "customer_id": _ANY_VALUE,
                    "customer_name": _ANY_VALUE,
                },
                "date_time": _ANY_VALUE,
                "user_id": _ANY_VALUE,
            },
            id="simple keys replace config with nested fields in record",
        ),
    ],
)
def test_transform(
    input_record, config, stream_state, stream_slice, keys_replace_config, expected_record
):
    KeysReplaceTransformation(
        old=keys_replace_config["old"], new=keys_replace_config["new"], parameters={}
    ).transform(
        record=input_record, config=config, stream_state=stream_state, stream_slice=stream_slice
    )
    assert input_record == expected_record


@pytest.mark.parametrize(
    ["input_record", "config", "keys_replace_config", "expected_record"],
    [
        pytest.param(
            {"a b": 1, "nested": {"c d": 2, "deeper": {"e f": 3}}},
            {},
            {"old": " ", "new": "_"},
            {"a_b": 1, "nested": {"c_d": 2, "deeper": {"e_f": 3}}},
            id="defaults keep literal, recursive, rename behavior",
        ),
        pytest.param(
            {"properties": {"hs_v2_date_entered_poetry": 1827, "hs_date_entered_poetry": 9999}},
            {},
            {"old": "hs_v2_date_entered_", "new": "hs_date_entered_"},
            {"properties": {"hs_date_entered_poetry": 9999}},
            id="defaults keep last-write-wins collision behavior",
        ),
        pytest.param(
            {"hs_v2_date_entered_customer": 1, "other": 2},
            {},
            {"old": "hs_v2_date_entered_(.*)", "new": r"hs_lifecyclestage_\1_date", "regex": True},
            {"hs_lifecyclestage_customer_date": 1, "other": 2},
            id="regex with numeric backreference",
        ),
        pytest.param(
            {"hs_v2_date_entered_customer": 1},
            {},
            {
                "old": "hs_v2_date_entered_(?P<stage>.*)",
                "new": r"hs_lifecyclestage_\g<stage>_date",
                "regex": True,
            },
            {"hs_lifecyclestage_customer_date": 1},
            id="regex with named group backreference",
        ),
        pytest.param(
            {"hs_v2_date_entered_customer": 1},
            {},
            {
                "old": "hs_v2_date_entered_(.*)",
                "new": r"hs_lifecyclestage_\g<1>_date",
                "regex": True,
            },
            {"hs_lifecyclestage_customer_date": 1},
            id="regex with g<1> backreference",
        ),
        pytest.param(
            {"hs_v2_date_entered_customer": 1, "hs_v2_date_entered_lead_date": 2},
            {},
            {
                "old": "hs_v2_date_entered_(.*?)(?:_date)?$",
                "new": r"hs_lifecyclestage_\1_date",
                "regex": True,
            },
            {"hs_lifecyclestage_customer_date": 1, "hs_lifecyclestage_lead_date": 2},
            id="regex does not double append a suffix",
        ),
        pytest.param(
            {"id_1": 1, "id_22": 2, "name": 3},
            {},
            {"old": r"_(\d+)$", "new": r"[\1]", "regex": True},
            {"id[1]": 1, "id[22]": 2, "name": 3},
            id="regex with backslash escapes in the pattern",
        ),
        pytest.param(
            {"x(1)": 1},
            {},
            {"old": "(1)", "new": "one", "regex": True},
            {"x(one)": 1},
            id="plain regex is not literal-evaluated by interpolation",
        ),
        pytest.param(
            {"prefix_a": 1, "prefix_b": 2},
            {"source_prefix": "prefix_"},
            {"old": "^{{ config['source_prefix'] }}(.*)", "new": r"\1", "regex": True},
            {"a": 1, "b": 2},
            id="interpolated regex keeps backreferences",
        ),
        pytest.param(
            {"hs_v2_date_entered_x": 1, "unrelated": 2},
            {},
            {"old": "hs_v2_date_entered_", "new": "hs_date_entered_", "keep_original": True},
            {"hs_v2_date_entered_x": 1, "hs_date_entered_x": 1, "unrelated": 2},
            id="keep_original copies matching keys and does not duplicate others",
        ),
        pytest.param(
            {"hs_v2_date_entered_x": 1, "hs_date_entered_x": 9999},
            {},
            {"old": "hs_v2_date_entered_", "new": "hs_date_entered_", "keep_original": True},
            {"hs_v2_date_entered_x": 1, "hs_date_entered_x": 1},
            id="keep_original overwrites an existing target regardless of key order",
        ),
        pytest.param(
            {
                "hs_v2_date_entered_absent": 1,
                "hs_v2_date_entered_null": 2,
                "hs_date_entered_null": None,
                "hs_v2_date_entered_present": 3,
                "hs_date_entered_present": 9999,
            },
            {},
            {
                "old": "hs_v2_date_entered_",
                "new": "hs_date_entered_",
                "keep_original": True,
                "only_if_missing": True,
            },
            {
                "hs_v2_date_entered_absent": 1,
                "hs_date_entered_absent": 1,
                "hs_v2_date_entered_null": 2,
                "hs_date_entered_null": 2,
                "hs_v2_date_entered_present": 3,
                "hs_date_entered_present": 9999,
            },
            id="keep_original with only_if_missing for absent, null and present targets",
        ),
        pytest.param(
            {
                "hs_v2_date_entered_absent": 1,
                "hs_v2_date_entered_null": 2,
                "hs_date_entered_null": None,
                "hs_v2_date_entered_present": 3,
                "hs_date_entered_present": 9999,
            },
            {},
            {"old": "hs_v2_date_entered_", "new": "hs_date_entered_", "only_if_missing": True},
            {
                "hs_date_entered_absent": 1,
                "hs_date_entered_null": 2,
                "hs_v2_date_entered_present": 3,
                "hs_date_entered_present": 9999,
            },
            id="rename with only_if_missing keeps the original key when the target is present",
        ),
        pytest.param(
            {"hs_date_entered_null": None, "hs_v2_date_entered_null": 2},
            {},
            {"old": "hs_v2_date_entered_", "new": "hs_date_entered_", "only_if_missing": True},
            {"hs_date_entered_null": 2},
            id="rename with only_if_missing fills a null target declared before the source key",
        ),
        pytest.param(
            {"a b": 1, "properties": {"c d": 2, "nested": {"e f": 3}}},
            {},
            {"old": " ", "new": "_", "field_path": ["properties"]},
            {"a b": 1, "properties": {"c_d": 2, "nested": {"e_f": 3}}},
            id="field_path scopes the replacement and recurses within the target",
        ),
        pytest.param(
            {"a b": 1, "attrs": {"c d": 2}},
            {"target": "attrs"},
            {"old": " ", "new": "_", "field_path": ["{{ config['target'] }}"]},
            {"a b": 1, "attrs": {"c_d": 2}},
            id="field_path is interpolated with config",
        ),
        pytest.param(
            {"data": [{"attributes": {"c d": 1}}, {"attributes": {"e f": 2}}, {"other": 3}]},
            {},
            {"old": " ", "new": "_", "field_path": ["data", "*", "attributes"]},
            {"data": [{"attributes": {"c_d": 1}}, {"attributes": {"e_f": 2}}, {"other": 3}]},
            id="field_path wildcard applies to every matching object",
        ),
        pytest.param(
            {"a b": 1},
            {},
            {"old": " ", "new": "_", "field_path": ["properties"]},
            {"a b": 1},
            id="field_path missing is a no-op",
        ),
        pytest.param(
            {"a b": 1, "properties": "not a dict"},
            {},
            {"old": " ", "new": "_", "field_path": ["properties"]},
            {"a b": 1, "properties": "not a dict"},
            id="field_path pointing to a non object is a no-op",
        ),
        pytest.param(
            {"properties": {"a b": {"c d": 1}}},
            {},
            {"old": " ", "new": "_", "field_path": ["properties"], "keep_original": True},
            {"properties": {"a b": {"c d": 1, "c_d": 1}, "a_b": {"c d": 1, "c_d": 1}}},
            id="keep_original applies recursively to nested objects",
        ),
    ],
)
def test_transform_with_options(input_record, config, keys_replace_config, expected_record):
    KeysReplaceTransformation(**keys_replace_config, parameters={}).transform(
        record=input_record, config=config
    )
    assert input_record == expected_record


def test_keep_original_does_not_alias_nested_values():
    record = {"a b": {"c": 1}}
    KeysReplaceTransformation(old=" ", new="_", keep_original=True, parameters={}).transform(record)
    record["a_b"]["c"] = 2
    assert record["a b"] == {"c": 1}


def test_new_keys_are_not_processed_again():
    record = {"a": 1}
    KeysReplaceTransformation(old="a", new="aa", keep_original=True, parameters={}).transform(
        record
    )
    assert record == {"a": 1, "aa": 1}


@pytest.mark.parametrize(
    ["keys_replace_config", "expected_error"],
    [
        pytest.param(
            {"old": "(unclosed", "new": "x"},
            "KeysReplace `old` is not a valid regular expression",
            id="invalid pattern",
        ),
        pytest.param(
            {"old": "(a)", "new": r"\2"},
            "KeysReplace `new` is not a valid replacement",
            id="invalid group reference",
        ),
        pytest.param(
            {"old": "(a)", "new": r"\g<missing>"},
            "KeysReplace `new` is not a valid replacement",
            id="unknown group name",
        ),
    ],
)
def test_invalid_regex_fails_at_construction(keys_replace_config, expected_error):
    with pytest.raises(ValueError, match=expected_error):
        KeysReplaceTransformation(**keys_replace_config, regex=True, parameters={})


def test_invalid_interpolated_regex_fails_at_transform():
    transformation = KeysReplaceTransformation(
        old="{{ config['pattern'] }}", new="x", regex=True, parameters={}
    )
    with pytest.raises(ValueError, match="KeysReplace `old` is not a valid regular expression"):
        transformation.transform({"a": 1}, config={"pattern": "(unclosed"})


def test_literal_mode_does_not_validate_regex():
    record = {"a(b": 1}
    KeysReplaceTransformation(old="(", new="_", parameters={}).transform(record)
    assert record == {"a_b": 1}
