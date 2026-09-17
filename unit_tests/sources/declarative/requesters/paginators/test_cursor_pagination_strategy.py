#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.extractors import DpathExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.cursor_pagination_strategy import (
    CursorPaginationStrategy,
)
from airbyte_cdk.sources.types import Record


@pytest.mark.parametrize(
    "template_string, stop_condition, expected_token, page_size",
    [
        ("token", None, "token", None),
        ("token", None, "token", 5),
        ("{{ config.config_key }}", None, "config_value", None),
        ("{{ last_record.id }}", None, 1, None),
        ("{{ response._metadata.content }}", None, "content_value", None),
        ("{{ parameters.key }}", None, "value", None),
        ("{{ response.invalid_key }}", None, None, None),
        ("token", InterpolatedBoolean("{{False}}", parameters={}), "token", None),
        ("token", InterpolatedBoolean("{{True}}", parameters={}), None, None),
        ("token", "{{True}}", None, None),
        (
            "{{ headers.next }}",
            InterpolatedBoolean("{{ not headers.has_more }}", parameters={}),
            "ready_to_go",
            None,
        ),
        (
            "{{ headers.link.next.url }}",
            InterpolatedBoolean("{{ not headers.link.next.url }}", parameters={}),
            "https://adventure.io/api/v1/records?page=2&per_page=100",
            None,
        ),
    ],
    ids=[
        "test_static_token",
        "test_static_token_with_page_size",
        "test_token_from_config",
        "test_token_from_last_record",
        "test_token_from_response",
        "test_token_from_parameters",
        "test_token_not_found",
        "test_static_token_with_stop_condition_false",
        "test_static_token_with_stop_condition_true",
        "test_static_token_with_string_stop_condition",
        "test_token_from_header",
        "test_token_from_response_header_links",
    ],
)
def test_cursor_pagination_strategy(template_string, stop_condition, expected_token, page_size):
    decoder = JsonDecoder(parameters={})
    config = {"config_key": "config_value"}
    parameters = {"key": "value"}
    strategy = CursorPaginationStrategy(
        page_size=page_size,
        cursor_value=template_string,
        config=config,
        stop_condition=stop_condition,
        decoder=decoder,
        parameters=parameters,
    )

    response = requests.Response()
    link_str = '<https://adventure.io/api/v1/records?page=2&per_page=100>; rel="next"'
    response.headers = {"has_more": True, "next": "ready_to_go", "link": link_str}
    response_body = {
        "_metadata": {"content": "content_value"},
        "accounts": [],
        "end": 99,
        "total": 200,
        "characters": {},
    }
    response._content = json.dumps(response_body).encode("utf-8")
    last_record = Record(data={"id": 1, "more_records": True}, stream_name="stream_name")

    token = strategy.next_page_token(response, 1, last_record)
    assert expected_token == token
    assert page_size == strategy.get_page_size()


@pytest.mark.parametrize(
    "response_results, last_page_size, expected_next_page_token",
    [
        pytest.param(
            [{"id": 1}, {"id": 2}],
            0,
            "next_token",
            id="test_full_page_continues_even_if_all_records_filtered",
        ),
        pytest.param(
            [{"id": 1}, {"id": 2}],
            1,
            "next_token",
            id="test_full_page_continues_even_if_some_records_filtered",
        ),
        pytest.param(
            [{"id": 1}],
            1,
            None,
            id="test_partial_page_stops_pagination",
        ),
        pytest.param(
            [],
            0,
            None,
            id="test_empty_page_stops_pagination",
        ),
    ],
)
def test_cursor_pagination_strategy_with_extractor(
    response_results, last_page_size, expected_next_page_token
):
    extractor = DpathExtractor(field_path=["results"], parameters={}, config={})
    strategy = CursorPaginationStrategy(
        page_size=2,
        cursor_value="{{ response.next_token }}",
        stop_condition="{{ last_page_size < 2 }}",
        extractor=extractor,
        config={},
        parameters={},
    )

    response = requests.Response()
    response._content = json.dumps(
        {"results": response_results, "next_token": "next_token"}
    ).encode("utf-8")
    last_record = (
        Record(data=response_results[-1], stream_name="stream_name") if response_results else None
    )

    next_page_token = strategy.next_page_token(response, last_page_size, last_record)
    assert expected_next_page_token == next_page_token


def test_cursor_pagination_strategy_with_extractor_interpolates_raw_count_in_cursor_value():
    extractor = DpathExtractor(field_path=["results"], parameters={}, config={})
    strategy = CursorPaginationStrategy(
        page_size=2,
        cursor_value="{{ last_page_size }}",
        extractor=extractor,
        config={},
        parameters={},
    )

    response = requests.Response()
    response._content = json.dumps({"results": [{"id": 1}, {"id": 2}]}).encode("utf-8")

    next_page_token = strategy.next_page_token(response, 0, None)
    assert next_page_token == 2


def test_last_record_points_to_the_last_item_in_last_records_array():
    last_records = [{"id": 0, "more_records": True}, {"id": 1, "more_records": True}]
    strategy = CursorPaginationStrategy(
        page_size=1,
        cursor_value="{{ last_record.id }}",
        config={},
        parameters={},
    )

    response = requests.Response()
    next_page_token = strategy.next_page_token(response, 2, last_records[-1])
    assert next_page_token == 1


def test_last_record_is_node_if_no_records():
    strategy = CursorPaginationStrategy(
        page_size=1,
        cursor_value="{{ last_record.id }}",
        config={},
        parameters={},
    )

    response = requests.Response()
    next_page_token = strategy.next_page_token(response, 0, None)
    assert next_page_token is None


@pytest.mark.parametrize(
    "page_size_input, config, expected_page_size",
    [
        pytest.param(100, {}, 100, id="static_integer"),
        pytest.param("100", {}, 100, id="static_string"),
        pytest.param(
            "{{ config['page_size'] }}", {"page_size": 50}, 50, id="interpolated_from_config"
        ),
        pytest.param("{{ config.get('page_size', 100) }}", {}, 100, id="interpolated_with_default"),
        pytest.param(
            "{{ config.get('page_size', 100) }}",
            {"page_size": 200},
            200,
            id="interpolated_override_default",
        ),
        pytest.param(None, {}, None, id="none_page_size"),
    ],
)
def test_interpolated_page_size(page_size_input, config, expected_page_size):
    """Test that page_size supports interpolation from config."""
    strategy = CursorPaginationStrategy(
        page_size=page_size_input,
        cursor_value="token",
        config=config,
        parameters={},
    )
    assert strategy.get_page_size() == expected_page_size


def test_interpolated_page_size_raises_on_non_integer():
    """Test that initialization raises an exception when interpolation resolves to a non-integer."""
    with pytest.raises(Exception, match="is of type .* Expected"):
        CursorPaginationStrategy(
            page_size="{{ config['page_size'] }}",
            cursor_value="token",
            config={"page_size": "invalid"},
            parameters={},
        )
