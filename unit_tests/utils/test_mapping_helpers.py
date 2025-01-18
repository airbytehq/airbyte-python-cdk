#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.utils.mapping_helpers import combine_mappings


def test_basic_merge():
    mappings = [{"a": 1}, {"b": 2}, {"c": 3}, {}]
    result = combine_mappings(mappings)
    assert result == {"a": 1, "b": 2, "c": 3}


def test_combine_with_string():
    mappings = [{"a": 1}, "option"]
    with pytest.raises(ValueError, match="Cannot combine multiple options if one is a string"):
        combine_mappings(mappings)


def test_overlapping_keys():
    mappings = [{"a": 1, "b": 2}, {"b": 3}]
    with pytest.raises(ValueError, match="Duplicate keys"):
        combine_mappings(mappings)


def test_multiple_strings():
    mappings = ["option1", "option2"]
    with pytest.raises(ValueError, match="Cannot combine multiple string options"):
        combine_mappings(mappings)


def test_handle_none_values():
    mappings = [{"a": 1}, None, {"b": 2}]
    result = combine_mappings(mappings)
    assert result == {"a": 1, "b": 2}


def test_empty_mappings():
    mappings = []
    result = combine_mappings(mappings)
    assert result == {}


def test_single_mapping():
    mappings = [{"a": 1}]
    result = combine_mappings(mappings)
    assert result == {"a": 1}


def test_combine_with_string_and_empty_mappings():
    mappings = ["option", {}]
    result = combine_mappings(mappings)
    assert result == "option"


def test_nested_merge():
    mappings = [{"a": {"b": 1}}, {"c": {"d": 2}}]
    result = combine_mappings(mappings)
    assert result == {"a": {"b": 1}, "c": {"d": 2}}


def test_nested_conflict():
    mappings = [{"a": {"b": 1}}, {"a": {"b": 2}}]
    with pytest.raises(ValueError, match="nested path conflict"):
        combine_mappings(mappings)


def test_nested_path_conflict():
    mappings = [{"a": 1}, {"a": {"b": 2}}]
    with pytest.raises(ValueError, match="nested path conflict"):
        combine_mappings(mappings)


def test_deep_nested_merge():
    mappings = [{"a": {"b": {"c": 1}}}, {"d": {"e": {"f": 2}}}]
    result = combine_mappings(mappings)
    assert result == {"a": {"b": {"c": 1}}, "d": {"e": {"f": 2}}}
