import pytest

from airbyte_cdk.sources.declarative.transformations.dpath_flatten_fields import (
    DpathFlattenFields,
    KeyTransformation,
)

_ANY_VALUE = -1
_DELETE_ORIGIN_VALUE = True
_REPLACE_WITH_VALUE = True
_DO_NOT_DELETE_ORIGIN_VALUE = False
_DO_NOT_REPLACE_WITH_VALUE = False
_NO_KEY_PREFIX = None
_NO_KEY_SUFFIX = None
_NO_KEY_TRANSFORMATIONS = None


@pytest.mark.parametrize(
    [
        "input_record",
        "config",
        "field_path",
        "delete_origin_value",
        "replace_record",
        "key_transformation",
        "expected_record",
    ],
    [
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field3": _ANY_VALUE},
            id="flatten by dpath, delete origin value",
        ),
        pytest.param(
            {
                "field1": _ANY_VALUE,
                "field2": {"field3": {"field4": {"field5": _ANY_VALUE}}},
            },
            {},
            ["field2", "*", "field4"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {
                "field1": _ANY_VALUE,
                "field2": {"field3": {"field4": {"field5": _ANY_VALUE}}},
                "field5": _ANY_VALUE,
            },
            id="flatten by dpath with *, don't delete origin value",
        ),
        pytest.param(
            {
                "field1": _ANY_VALUE,
                "field2": {"field3": {"field4": {"field5": _ANY_VALUE}}},
            },
            {},
            ["field2", "*", "field4"],
            _DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field2": {"field3": {}}, "field5": _ANY_VALUE},
            id="flatten by dpath with *, delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {"field_path": "field2"},
            ["{{ config['field_path'] }}"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath from config, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["non-existing-field"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            id="flatten by non-existing dpath, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["*", "non-existing-field"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            id="flatten by non-existing dpath with *, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, not to update when record has field conflicts, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, not to update when record has field conflicts, delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field3": _ANY_VALUE},
            id="flatten by dpath, replace with value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DELETE_ORIGIN_VALUE,
            _REPLACE_WITH_VALUE,
            _NO_KEY_TRANSFORMATIONS,
            {"field3": _ANY_VALUE},
            id="flatten by dpath, delete_origin_value do not affect to replace_record",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _REPLACE_WITH_VALUE,
            ("prefix_", _NO_KEY_SUFFIX),
            {"prefix_field3": _ANY_VALUE},
            id="flatten by dpath, not delete origin value, replace record, add keys prefix",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _REPLACE_WITH_VALUE,
            (_NO_KEY_PREFIX, "_suffix"),
            {"field3_suffix": _ANY_VALUE},
            id="flatten by dpath, not delete origin value, replace record, add keys suffix",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _REPLACE_WITH_VALUE,
            ("prefix_", "_suffix"),
            {"prefix_field3_suffix": _ANY_VALUE},
            id="flatten by dpath, not delete origin value, replace record, add keys prefix and suffix",
        ),
    ],
)
def test_dpath_flatten_lists(
    input_record,
    config,
    field_path,
    delete_origin_value,
    replace_record,
    key_transformation,
    expected_record,
):
    if key_transformation:
        key_transformation = KeyTransformation(config, {}, *key_transformation)

    flattener = DpathFlattenFields(
        field_path=field_path,
        parameters={},
        config=config,
        delete_origin_value=delete_origin_value,
        replace_record=replace_record,
        key_transformation=key_transformation,
    )
    flattener.transform(input_record)
    assert input_record == expected_record
