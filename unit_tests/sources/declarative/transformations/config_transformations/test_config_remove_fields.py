#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.declarative.transformations.config_transformations.remove_fields import (
    ConfigRemoveFields as RemoveFields,
)


def test_given_valid_field_pointer_field_is_removed():
    transformation = RemoveFields(
        field_pointers=[
            ["field_to_remove"],
            ["another_field_to_remove"],
        ]
    )
    config = {
        "field_to_remove": "value_to_remove",
        "another_field_to_remove": "another_value_to_remove",
        "field_to_keep": "value_to_keep",
    }

    transformation.transform(config)

    assert "field_to_remove" not in config
    assert "another_field_to_remove" not in config
    assert "field_to_keep" in config
    assert config == {"field_to_keep": "value_to_keep"}


def test_given_valid_nested_field_pointer_field_is_removed():
    transformation = RemoveFields(
        field_pointers=[["parent", "child", "field_to_remove"], ["parent", "another_child"]]
    )
    config = {
        "parent": {
            "child": {
                "field_to_remove": "nested_value_to_remove",
                "field_to_keep": "nested_value_to_keep",
            },
            "another_child": "another_child_value",
            "child_to_keep": "child_value_to_keep",
        },
        "top_level_field": "top_level_value",
    }

    transformation.transform(config)

    assert "field_to_remove" not in config["parent"]["child"]
    assert "another_child" not in config["parent"]
    assert config["parent"]["child"]["field_to_keep"] == "nested_value_to_keep"
    assert config["parent"]["child_to_keep"] == "child_value_to_keep"
    assert config["top_level_field"] == "top_level_value"
    assert config == {
        "parent": {
            "child": {"field_to_keep": "nested_value_to_keep"},
            "child_to_keep": "child_value_to_keep",
        },
        "top_level_field": "top_level_value",
    }


def test_given_valid_field_point_but_field_does_not_exist_no_field_is_removed():
    transformation = RemoveFields(
        field_pointers=[
            ["nonexistent_field"],
            ["parent", "nonexistent_child"],
            ["parent", "child", "nonexistent_grandchild"],
            ["completely", "missing", "path"],
        ]
    )

    config = {"existing_field": "value", "parent": {"child": {"existing_grandchild": "value"}}}

    original_config = config.copy()

    transformation.transform(config)

    assert config == original_config


def test_with_condition_only_removes_fields_when_condition_is_met():
    transformation = RemoveFields(
        field_pointers=[
            ["conditional_field"],
        ],
        condition="{{ config.flag == true }}",
    )

    config_true = {"flag": True, "conditional_field": "this should be removed"}
    transformation.transform(config_true)

    config_false = {"flag": False, "conditional_field": "this should not be removed"}
    transformation.transform(config_false)

    assert "conditional_field" not in config_true
    assert "conditional_field" in config_false
    assert config_false["conditional_field"] == "this should not be removed"
