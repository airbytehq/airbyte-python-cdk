#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Dict, List, Mapping

import pytest

from airbyte_cdk.sources.declarative.expanders import ExtractArrayItems


@pytest.mark.parametrize(
    "input_record,array_path,preserve_parent_fields,parent_field_prefix,expected_records",
    [
        pytest.param(
            {
                "id": "in_123",
                "lines": {"data": [{"id": "il_1", "amount": 100}, {"id": "il_2", "amount": 200}]},
            },
            ["lines", "data"],
            None,
            None,
            [{"id": "il_1", "amount": 100}, {"id": "il_2", "amount": 200}],
            id="basic_array_extraction",
        ),
        pytest.param(
            {
                "id": "in_123",
                "created": 1234567890,
                "lines": {"data": [{"id": "il_1", "amount": 100}, {"id": "il_2", "amount": 200}]},
            },
            ["lines", "data"],
            ["id", "created"],
            "invoice_",
            [
                {"id": "il_1", "amount": 100, "invoice_id": "in_123", "invoice_created": 1234567890},
                {"id": "il_2", "amount": 200, "invoice_id": "in_123", "invoice_created": 1234567890},
            ],
            id="preserve_parent_fields_with_prefix",
        ),
        pytest.param(
            {
                "id": "in_123",
                "created": 1234567890,
                "lines": {"data": [{"id": "il_1", "amount": 100}]},
            },
            ["lines", "data"],
            ["id", "created"],
            None,
            [{"id": "il_1", "amount": 100, "created": 1234567890}],
            id="preserve_parent_fields_without_prefix",
        ),
        pytest.param(
            {"id": "in_123", "lines": {"data": []}},
            ["lines", "data"],
            None,
            None,
            [{"id": "in_123", "lines": {"data": []}}],
            id="empty_array_returns_original_record",
        ),
        pytest.param(
            {"id": "in_123", "lines": {"data": None}},
            ["lines", "data"],
            None,
            None,
            [{"id": "in_123", "lines": {"data": None}}],
            id="null_array_returns_original_record",
        ),
        pytest.param(
            {"id": "in_123", "other_field": "value"},
            ["lines", "data"],
            None,
            None,
            [{"id": "in_123", "other_field": "value"}],
            id="missing_array_path_returns_original_record",
        ),
        pytest.param(
            {"id": "in_123", "lines": "not_an_object"},
            ["lines", "data"],
            None,
            None,
            [{"id": "in_123", "lines": "not_an_object"}],
            id="invalid_nested_structure_returns_original_record",
        ),
        pytest.param(
            {"id": "in_123", "lines": {"data": "not_an_array"}},
            ["lines", "data"],
            None,
            None,
            [{"id": "in_123", "lines": {"data": "not_an_array"}}],
            id="non_array_value_returns_original_record",
        ),
        pytest.param(
            {"id": "in_123", "lines": {"data": [{"id": "il_1"}, "not_a_dict", {"id": "il_2"}]}},
            ["lines", "data"],
            None,
            None,
            [{"id": "il_1"}, {"id": "il_2"}],
            id="skip_non_dict_array_items",
        ),
        pytest.param(
            {
                "id": "in_123",
                "nested": {"deep": {"items": [{"id": "item_1"}, {"id": "item_2"}]}},
            },
            ["nested", "deep", "items"],
            ["id"],
            "parent_",
            [{"id": "item_1", "parent_id": "in_123"}, {"id": "item_2", "parent_id": "in_123"}],
            id="deeply_nested_array_path",
        ),
        pytest.param(
            {
                "id": "in_123",
                "lines": {"data": [{"id": "il_1", "nested": {"value": "a"}}, {"id": "il_2", "nested": {"value": "b"}}]},
            },
            ["lines", "data"],
            None,
            None,
            [{"id": "il_1", "nested": {"value": "a"}}, {"id": "il_2", "nested": {"value": "b"}}],
            id="preserve_nested_structures_in_array_items",
        ),
        pytest.param(
            {"id": "in_123", "missing_field": "value", "lines": {"data": [{"id": "il_1"}]}},
            ["lines", "data"],
            ["id", "nonexistent_field"],
            "invoice_",
            [{"id": "il_1", "invoice_id": "in_123"}],
            id="skip_nonexistent_parent_fields",
        ),
        pytest.param(
            {"items": [{"id": "item_1"}, {"id": "item_2"}]},
            ["items"],
            None,
            None,
            [{"id": "item_1"}, {"id": "item_2"}],
            id="top_level_array",
        ),
    ],
)
def test_extract_array_items_expand(
    input_record: Dict[str, Any],
    array_path: List[str],
    preserve_parent_fields: List[str],
    parent_field_prefix: str,
    expected_records: List[Mapping[str, Any]],
):
    """Test ExtractArrayItems.expand() with various input scenarios."""
    config = {}
    expander = ExtractArrayItems(
        config=config,
        array_path=array_path,
        parameters={},
        preserve_parent_fields=preserve_parent_fields,
        parent_field_prefix=parent_field_prefix,
    )
    
    result = expander.expand(input_record, config=config)
    
    assert result == expected_records


@pytest.mark.parametrize(
    "input_record,array_path,expected_count",
    [
        pytest.param(
            {"lines": {"data": [{"id": "1"}, {"id": "2"}, {"id": "3"}]}},
            ["lines", "data"],
            3,
            id="three_items",
        ),
        pytest.param(
            {"lines": {"data": [{"id": "1"}]}},
            ["lines", "data"],
            1,
            id="single_item",
        ),
        pytest.param(
            {"lines": {"data": []}},
            ["lines", "data"],
            1,
            id="empty_array_returns_one_original",
        ),
    ],
)
def test_extract_array_items_count(
    input_record: Dict[str, Any],
    array_path: List[str],
    expected_count: int,
):
    """Test that ExtractArrayItems returns the correct number of records."""
    config = {}
    expander = ExtractArrayItems(
        config=config,
        array_path=array_path,
        parameters={},
    )
    
    result = expander.expand(input_record, config=config)
    
    assert len(result) == expected_count


def test_extract_array_items_does_not_modify_original_record():
    """Test that ExtractArrayItems does not modify the original input record."""
    config = {}
    original_record = {
        "id": "in_123",
        "lines": {"data": [{"id": "il_1", "amount": 100}]},
    }
    input_record = original_record.copy()
    
    expander = ExtractArrayItems(
        config=config,
        array_path=["lines", "data"],
        parameters={},
    )
    
    expander.expand(input_record, config=config)
    
    assert input_record == original_record


def test_extract_array_items_with_interpolated_prefix():
    """Test that parent_field_prefix supports interpolation."""
    config = {"prefix": "invoice_"}
    expander = ExtractArrayItems(
        config=config,
        array_path=["lines", "data"],
        parameters={},
        preserve_parent_fields=["id"],
        parent_field_prefix="{{ config.prefix }}",
    )
    
    input_record = {
        "id": "in_123",
        "lines": {"data": [{"id": "il_1"}]},
    }
    
    result = expander.expand(input_record, config=config)
    
    assert result == [{"id": "il_1", "invoice_id": "in_123"}]


def test_extract_array_items_equality():
    """Test that ExtractArrayItems instances can be compared for equality."""
    config = {}
    expander1 = ExtractArrayItems(
        config=config,
        array_path=["lines", "data"],
        parameters={},
        preserve_parent_fields=["id"],
        parent_field_prefix="invoice_",
    )
    expander2 = ExtractArrayItems(
        config=config,
        array_path=["lines", "data"],
        parameters={},
        preserve_parent_fields=["id"],
        parent_field_prefix="invoice_",
    )
    expander3 = ExtractArrayItems(
        config=config,
        array_path=["items"],
        parameters={},
    )
    
    assert expander1 == expander2
    assert expander1 != expander3
