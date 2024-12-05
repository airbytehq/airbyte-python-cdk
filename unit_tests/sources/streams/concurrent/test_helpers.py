import pytest

from airbyte_cdk.sources.streams.concurrent.helpers import get_primary_key_from_stream


def test_given_primary_key_is_list_of_strings_when_get_primary_key_from_stream_then_assume_it_is_composite_key_and_return_as_is():
    result = get_primary_key_from_stream(["composite_id_1", "composite_id_2"])
    assert result == ["composite_id_1", "composite_id_2"]


def test_given_primary_key_is_composite_in_nested_lists_when_get_primary_key_from_stream_then_flatten_lists():
    result = get_primary_key_from_stream([["composite_id_1"], ["composite_id_2"]])
    assert result == ["composite_id_1", "composite_id_2"]


def test_given_nested_key_when_get_primary_key_from_stream_then_raise_error():
    with pytest.raises(ValueError):
        get_primary_key_from_stream([["composite_id_1", "composite_id_2"]])
