import pytest

from airbyte_cdk.sources.declarative.transformations.dpath_flatten_fields import DpathFlattenFields

_ANY_VALUE = -1
_DELETE_ORIGIN_VALUE = True
_REPLACE_WITH_VALUE = True
_DO_NOT_DELETE_ORIGIN_VALUE = False
_DO_NOT_REPLACE_WITH_VALUE = False


@pytest.mark.parametrize(
    [
        "input_record",
        "config",
        "field_path",
        "delete_origin_value",
        "replace_record",
        "expected_record",
    ],
    [
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
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
            {"field1": _ANY_VALUE, "field2": {"field3": {}}, "field5": _ANY_VALUE},
            id="flatten by dpath with *, delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {"field_path": "field2"},
            ["{{ config['field_path'] }}"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath from config, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["non-existing-field"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            id="flatten by non-existing dpath, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["*", "non-existing-field"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            id="flatten by non-existing dpath with *, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, not to update when record has field conflicts, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _DO_NOT_REPLACE_WITH_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, not to update when record has field conflicts, delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            _REPLACE_WITH_VALUE,
            {"field3": _ANY_VALUE},
            id="flatten by dpath, replace with value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DELETE_ORIGIN_VALUE,
            _REPLACE_WITH_VALUE,
            {"field3": _ANY_VALUE},
            id="flatten by dpath, delete_origin_value do not affect to replace_record",
        ),
    ],
)
def test_dpath_flatten_lists(
    input_record, config, field_path, delete_origin_value, replace_record, expected_record
):
    flattener = DpathFlattenFields(
        field_path=field_path,
        parameters={},
        config=config,
        delete_origin_value=delete_origin_value,
        replace_record=replace_record,
    )
    flattener.transform(input_record)
    assert input_record == expected_record
