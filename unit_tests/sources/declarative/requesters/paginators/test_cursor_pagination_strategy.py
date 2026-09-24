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


def test_given_page_size_override_then_token_is_unchanged():
    strategy = CursorPaginationStrategy(
        page_size=100, cursor_value="{{ response.next }}", config={}, parameters={}
    )
    response = requests.Response()
    response._content = json.dumps({"next": "a token"}).encode("utf-8")

    assert (
        strategy.next_page_token(response, 50, None, None, page_size_override=50)
        == strategy.next_page_token(response, 100, None, None)
        == "a token"
    )


def test_given_stop_condition_uses_page_size_and_page_is_full_at_the_reduced_size_then_keep_paginating():
    """
    Regression test for the silent truncation a page size reduction used to cause: a full page at the reduced
    size satisfies `last_page_size < 100` and would end the pagination, dropping the rest of the partition.
    `page_size` holds the size that was actually requested, so the same condition keeps paginating.
    """
    strategy = CursorPaginationStrategy(
        page_size=100,
        cursor_value="{{ response.next }}",
        stop_condition="{{ last_page_size < page_size }}",
        config={},
        parameters={},
    )
    response = requests.Response()
    response._content = json.dumps({"next": "a token"}).encode("utf-8")

    assert strategy.next_page_token(response, 50, None, None, page_size_override=50) == "a token"
    assert strategy.next_page_token(response, 100, None, None) == "a token"


def test_given_stop_condition_uses_page_size_and_page_is_short_then_stop():
    strategy = CursorPaginationStrategy(
        page_size=100,
        cursor_value="{{ response.next }}",
        stop_condition="{{ last_page_size < page_size }}",
        config={},
        parameters={},
    )
    response = requests.Response()
    response._content = json.dumps({"next": "a token"}).encode("utf-8")

    assert strategy.next_page_token(response, 49, None, None, page_size_override=50) is None
    assert strategy.next_page_token(response, 99, None, None) is None


@pytest.mark.parametrize(
    "page_size,page_size_override,expected_token",
    [
        pytest.param(100, None, 100, id="test_configured_page_size_is_bound"),
        pytest.param(100, 50, 50, id="test_reduced_page_size_is_bound"),
    ],
)
def test_page_size_is_bound_in_the_cursor_value_interpolation_context(
    page_size, page_size_override, expected_token
):
    """
    `page_size` has to resolve to the size that was actually requested. The None case alone would also pass if
    the variable were never bound at all, so the two positive cases are what pin it.
    """
    strategy = CursorPaginationStrategy(
        page_size=page_size, cursor_value="{{ page_size }}", config={}, parameters={}
    )
    response = requests.Response()
    response._content = json.dumps({}).encode("utf-8")

    assert (
        strategy.next_page_token(response, 10, None, None, page_size_override=page_size_override)
        == expected_token
    )


@pytest.mark.parametrize(
    "field_name, kwargs",
    [
        pytest.param(
            "stop_condition",
            {
                "cursor_value": "{{ response.next }}",
                "stop_condition": "{{ last_page_size < page_size }}",
            },
            id="test_stop_condition_reads_page_size",
        ),
        pytest.param(
            "cursor_value",
            {"cursor_value": "{{ page_size }}"},
            id="test_cursor_value_reads_page_size",
        ),
        pytest.param(
            "stop_condition",
            {
                "cursor_value": "{{ response.next }}",
                "stop_condition": InterpolatedBoolean(
                    condition="{{ last_page_size < page_size }}", parameters={}
                ),
            },
            id="test_stop_condition_given_as_a_component",
        ),
    ],
)
def test_given_no_page_size_when_an_expression_reads_page_size_then_raise(field_name, kwargs):
    """
    `page_size` is unbound without a declared page size, and an unbound comparison does not fail loudly: Jinja
    raises, the interpolation falls back to the raw template string, and a non-empty string is truthy. A
    `stop_condition` written that way would stop after the first page, so it has to be rejected here.
    """
    with pytest.raises(ValueError) as error:
        CursorPaginationStrategy(config={}, parameters={}, **kwargs)

    assert f"`{field_name}`" in str(error.value)
    assert "declares no `page_size`" in str(error.value)


def test_given_no_page_size_when_the_expression_reads_config_page_size_then_accept():
    """`config['page_size']` is a lookup on the config, not the reduction-aware variable, and is always bound."""
    strategy = CursorPaginationStrategy(
        cursor_value="{{ response.next }}",
        stop_condition="{{ last_page_size < config['page_size'] }}",
        config={"page_size": 100},
        parameters={},
    )
    response = requests.Response()
    response._content = json.dumps({"next": "a token"}).encode("utf-8")

    assert strategy.next_page_token(response, 100, None, None) == "a token"


@pytest.mark.parametrize(
    "page_size",
    [
        pytest.param(100, id="test_literal_page_size"),
        pytest.param("{{ config['page_size'] }}", id="test_interpolated_page_size"),
    ],
)
def test_given_a_page_size_when_an_expression_reads_page_size_then_accept(page_size):
    strategy = CursorPaginationStrategy(
        cursor_value="{{ response.next }}",
        stop_condition="{{ last_page_size < page_size }}",
        page_size=page_size,
        config={"page_size": 100},
        parameters={},
    )
    response = requests.Response()
    response._content = json.dumps({"next": "a token"}).encode("utf-8")

    assert strategy.next_page_token(response, 100, None, None) == "a token"
    assert strategy.next_page_token(response, 50, None, None, page_size_override=50) == "a token"
    assert strategy.next_page_token(response, 49, None, None, page_size_override=50) is None
