import pytest

from airbyte_cdk.sources.declarative.transformations.dpath_flatten_fields import DpathFlattenFields

_ANY_VALUE = -1
_DELETE_ORIGIN_VALUE = True
_DO_NOT_DELETE_ORIGIN_VALUE = False


@pytest.mark.parametrize(
    [
        "input_record",
        "config",
        "field_path",
        "delete_origin_value",
        "expected_record",
    ],
    [
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["field2"],
            _DELETE_ORIGIN_VALUE,
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
            {"field1": _ANY_VALUE, "field2": {"field3": {}}, "field5": _ANY_VALUE},
            id="flatten by dpath with *, delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {"field_path": "field2"},
            ["{{ config['field_path'] }}"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath from config, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["non-existing-field"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            id="flatten by non-existing dpath, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            {},
            ["*", "non-existing-field"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}},
            id="flatten by non-existing dpath with *, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, not to update when record has field conflicts, don't delete origin value",
        ),
        pytest.param(
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            {},
            ["field2"],
            _DO_NOT_DELETE_ORIGIN_VALUE,
            {"field1": _ANY_VALUE, "field2": {"field3": _ANY_VALUE}, "field3": _ANY_VALUE},
            id="flatten by dpath, not to update when record has field conflicts, delete origin value",
        ),
    ],
)
def test_dpath_flatten_lists(
    input_record, config, field_path, delete_origin_value, expected_record
):
    flattener = DpathFlattenFields(
        field_path=field_path, parameters={}, config=config, delete_origin_value=delete_origin_value
    )
    flattener.transform(input_record)
    assert input_record == expected_record
