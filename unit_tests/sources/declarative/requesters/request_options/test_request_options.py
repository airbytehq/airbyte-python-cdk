from typing import Any, Dict, List, Optional, Type

import pytest

from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)


@pytest.mark.parametrize(
    "field_name, field_path, inject_into, error_type, error_message",
    [
        (
            None,
            None,
            RequestOptionType.body_json,
            ValueError,
            "RequestOption requires either a field_name or field_path",
        ),
        (
            "field",
            ["data", "field"],
            RequestOptionType.body_json,
            ValueError,
            "Only one of field_name or field_path can be provided",
        ),
        (
            None,
            ["data", "field"],
            RequestOptionType.header,
            ValueError,
            "Nested field injection is only supported for body JSON injection.",
        ),
    ],
)
def test_request_option_validation(
    field_name: Optional[str],
    field_path: Any,
    inject_into: RequestOptionType,
    error_type: Type[Exception],
    error_message: str,
):
    """Test various validation cases for RequestOption"""
    with pytest.raises(error_type, match=error_message):
        RequestOption(
            field_name=field_name, field_path=field_path, inject_into=inject_into, parameters={}
        )


@pytest.mark.parametrize(
    "request_option_args, value, expected_result",
    [
        # Basic field_name test
        (
            {
                "field_name": "test_{{ config['base_field'] }}",
                "inject_into": RequestOptionType.body_json,
            },
            "test_value",
            {"test_value": "test_value"},
        ),
        # Basic field_path test
        (
            {
                "field_path": ["data", "nested_{{ config['base_field'] }}", "field"],
                "inject_into": RequestOptionType.body_json,
            },
            "test_value",
            {"data": {"nested_value": {"field": "test_value"}}},
        ),
        # Deep nesting test
        (
            {
                "field_path": ["level1", "level2", "level3", "level4", "field"],
                "inject_into": RequestOptionType.body_json,
            },
            "deep_value",
            {"level1": {"level2": {"level3": {"level4": {"field": "deep_value"}}}}},
        ),
    ],
)
def test_inject_into_request_cases(
    request_option_args: Dict[str, Any], value: Any, expected_result: Dict[str, Any]
):
    """Test various injection cases"""
    config = {"base_field": "value"}
    target: Dict[str, Any] = {}

    request_option = RequestOption(**request_option_args, parameters={})
    request_option.inject_into_request(target, value, config)
    assert target == expected_result


@pytest.mark.parametrize(
    "config, parameters, field_path, expected_structure",
    [
        (
            {"nested": "user"},
            {"type": "profile"},
            ["data", "{{ config['nested'] }}", "{{ parameters['type'] }}"],
            {"data": {"user": {"profile": "test_value"}}},
        ),
        (
            {"user_type": "admin", "section": "profile"},
            {"id": "12345"},
            [
                "data",
                "{{ config['user_type'] }}",
                "{{ parameters['id'] }}",
                "{{ config['section'] }}",
                "details",
            ],
            {"data": {"admin": {"12345": {"profile": {"details": "test_value"}}}}},
        ),
    ],
)
def test_interpolation_cases(
    config: Dict[str, Any],
    parameters: Dict[str, Any],
    field_path: List[str],
    expected_structure: Dict[str, Any],
):
    """Test various interpolation scenarios"""
    request_option = RequestOption(
        field_path=field_path, inject_into=RequestOptionType.body_json, parameters=parameters
    )
    target: Dict[str, Any] = {}
    request_option.inject_into_request(target, "test_value", config)
    assert target == expected_structure


@pytest.mark.parametrize(
    "value, expected_type",
    [
        (42, int),
        (3.14, float),
        (True, bool),
        (["a", "b", "c"], list),
        ({"key": "value"}, dict),
        (None, type(None)),
    ],
)
def test_value_type_handling(value: Any, expected_type: Type):
    """Test handling of different value types"""
    config = {}
    target: Dict[str, Any] = {}
    request_option = RequestOption(
        field_path=["data", "test"], inject_into=RequestOptionType.body_json, parameters={}
    )
    request_option.inject_into_request(target, value, config)
    assert isinstance(target["data"]["test"], expected_type)
    assert target["data"]["test"] == value


@pytest.mark.parametrize(
    "field_name, field_path, inject_into, expected__is_field_path",
    [
        ("field", None, RequestOptionType.body_json, False),
        (None, ["data", "field"], RequestOptionType.body_json, True),
    ],
)
def test__is_field_path(
    field_name: Optional[str],
    field_path: Optional[List[str]],
    inject_into: RequestOptionType,
    expected__is_field_path: bool,
):
    """Test the _is_field_path property"""
    request_option = RequestOption(
        field_name=field_name, field_path=field_path, inject_into=inject_into, parameters={}
    )
    assert request_option._is_field_path == expected__is_field_path


def test_multiple_injections():
    """Test injecting multiple values into the same target dict"""
    config = {"base": "test"}
    target = {"existing": "value"}

    # First injection with field_name
    option1 = RequestOption(
        field_name="field1", inject_into=RequestOptionType.body_json, parameters={}
    )
    option1.inject_into_request(target, "value1", config)

    # Second injection with nested path
    option2 = RequestOption(
        field_path=["data", "nested", "field2"],
        inject_into=RequestOptionType.body_json,
        parameters={},
    )
    option2.inject_into_request(target, "value2", config)

    assert target == {
        "existing": "value",
        "field1": "value1",
        "data": {"nested": {"field2": "value2"}},
    }
