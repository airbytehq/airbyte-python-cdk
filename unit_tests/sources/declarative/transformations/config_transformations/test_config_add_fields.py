#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.transformations.config_transformations.add_fields import (
    AddedFieldDefinition,
)
from airbyte_cdk.sources.declarative.transformations.config_transformations.add_fields import (
    ConfigAddFields as AddFields,
)


def test_given_valid_static_value_fields_added():
    transformation = AddFields(
        fields=[
            AddedFieldDefinition(
                path=["new_field"], value="static_value", value_type=None, parameters={}
            ),
            AddedFieldDefinition(
                path=["another_field"], value="another_value", value_type=None, parameters={}
            ),
        ]
    )
    config = {}

    transformation.transform(config)

    assert config == {
        "new_field": "static_value",
        "another_field": "another_value",
    }


def test_given_valid_nested_fields_static_value_added():
    transformation = AddFields(
        fields=[
            AddedFieldDefinition(
                path=["parent", "child", "grandchild"],
                value="nested_value",
                value_type=None,
                parameters={},
            ),
        ]
    )
    config = {}

    transformation.transform(config)

    assert config == {"parent": {"child": {"grandchild": "nested_value"}}}


def test_given_valid_interpolated_input_field_added():
    transformation = AddFields(
        fields=[
            AddedFieldDefinition(
                path=["derived_field"],
                value="{{ config.original_field }}",
                value_type=None,
                parameters={},
            ),
            AddedFieldDefinition(
                path=["expression_result"], value="{{ 2 * 3 }}", value_type=None, parameters={}
            ),
        ]
    )
    config = {"original_field": "original_value"}

    transformation.transform(config)

    assert config == {
        "original_field": "original_value",
        "derived_field": "original_value",
        "expression_result": 6,
    }


def test_given_invalid_field_raises_exception():
    with pytest.raises(ValueError):
        AddFields(
            fields=[AddedFieldDefinition(path=[], value="value", value_type=None, parameters={})]
        )

    with pytest.raises(ValueError):
        AddFields(
            fields=[
                AddedFieldDefinition(path=["valid_path"], value=123, value_type=None, parameters={})
            ]
        )


def test_given_field_already_exists_value_is_overwritten():
    transformation = AddFields(
        fields=[
            AddedFieldDefinition(
                path=["existing_field"], value="new_value", value_type=None, parameters={}
            ),
        ]
    )
    config = {"existing_field": "existing_value"}

    transformation.transform(config)

    assert config["existing_field"] == "new_value"


def test_with_condition_only_adds_fields_when_condition_is_met():
    transformation = AddFields(
        fields=[
            AddedFieldDefinition(
                path=["conditional_field"], value="added_value", value_type=None, parameters={}
            ),
        ],
        condition="{{ config.flag == true }}",
    )

    config_true = {"flag": True}
    transformation.transform(config_true)

    config_false = {"flag": False}
    transformation.transform(config_false)

    assert "conditional_field" in config_true
    assert "conditional_field" not in config_false
