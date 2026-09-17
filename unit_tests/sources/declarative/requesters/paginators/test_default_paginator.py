#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
from unittest.mock import MagicMock, Mock

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders import JsonDecoder, XmlDecoder
from airbyte_cdk.sources.declarative.extractors import DpathExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.requesters.paginators.default_paginator import (
    DefaultPaginator,
    PaginatorTestReadDecorator,
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.cursor_pagination_strategy import (
    CursorPaginationStrategy,
)
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.offset_increment import (
    OffsetIncrement,
)
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.page_increment import (
    PageIncrement,
)
from airbyte_cdk.sources.declarative.requesters.request_path import RequestPath
from airbyte_cdk.sources.declarative.types import Record, StreamSlice, StreamState
from airbyte_cdk.sources.streams.http.http_client import HttpClient


@pytest.mark.parametrize(
    "page_token_request_option, stop_condition, expected_updated_path, expected_request_params, expected_headers, expected_body_data, expected_body_json, last_record, expected_next_page_token, limit, decoder, response_body",
    [
        (
            RequestPath(parameters={}),
            None,
            "https://airbyte.io/next_url",
            {"limit": 2},
            {},
            {},
            {},
            {"id": 1},
            {"next_page_token": "https://airbyte.io/next_url"},
            2,
            JsonDecoder,
            {"next": "https://airbyte.io/next_url"},
        ),
        (
            RequestOption(
                inject_into=RequestOptionType.request_parameter, field_name="from", parameters={}
            ),
            None,
            None,
            {"limit": 2, "from": "https://airbyte.io/next_url"},
            {},
            {},
            {},
            {"id": 1},
            {"next_page_token": "https://airbyte.io/next_url"},
            2,
            JsonDecoder,
            {"next": "https://airbyte.io/next_url"},
        ),
        (
            RequestOption(
                inject_into=RequestOptionType.request_parameter, field_name="from", parameters={}
            ),
            InterpolatedBoolean(condition="{{True}}", parameters={}),
            None,
            {"limit": 2},
            {},
            {},
            {},
            {"id": 1},
            None,
            2,
            JsonDecoder,
            {"next": "https://airbyte.io/next_url"},
        ),
        (
            RequestOption(inject_into=RequestOptionType.header, field_name="from", parameters={}),
            None,
            None,
            {"limit": 2},
            {"from": "https://airbyte.io/next_url"},
            {},
            {},
            {"id": 1},
            {"next_page_token": "https://airbyte.io/next_url"},
            2,
            JsonDecoder,
            {"next": "https://airbyte.io/next_url"},
        ),
        (
            RequestOption(
                inject_into=RequestOptionType.body_data, field_name="from", parameters={}
            ),
            None,
            None,
            {"limit": 2},
            {},
            {"from": "https://airbyte.io/next_url"},
            {},
            {"id": 1},
            {"next_page_token": "https://airbyte.io/next_url"},
            2,
            JsonDecoder,
            {"next": "https://airbyte.io/next_url"},
        ),
        (
            RequestOption(
                inject_into=RequestOptionType.body_json, field_name="from", parameters={}
            ),
            None,
            None,
            {"limit": 2},
            {},
            {},
            {"from": "https://airbyte.io/next_url"},
            {"id": 1},
            {"next_page_token": "https://airbyte.io/next_url"},
            2,
            JsonDecoder,
            {"next": "https://airbyte.io/next_url"},
        ),
        (
            RequestPath(parameters={}),
            None,
            "https://airbyte.io/next_url",
            {"limit": 2},
            {},
            {},
            {},
            {"id": 1},
            {"next_page_token": "https://airbyte.io/next_url"},
            2,
            XmlDecoder,
            b"<next>https://airbyte.io/next_url</next>",
        ),
        (
            RequestOption(
                inject_into=RequestOptionType.request_parameter, field_name="from", parameters={}
            ),
            None,
            None,
            {"limit": 2, "from": "https://airbyte.io/next_url"},
            {},
            {},
            {},
            {"id": 1},
            {"next_page_token": "https://airbyte.io/next_url"},
            2,
            XmlDecoder,
            b"<next>https://airbyte.io/next_url</next>",
        ),
    ],
    ids=[
        "test_default_paginator_path",
        "test_default_paginator_request_param",
        "test_default_paginator_no_token",
        "test_default_paginator_cursor_header",
        "test_default_paginator_cursor_body_data",
        "test_default_paginator_cursor_body_json",
        "test_default_paginator_path_with_xml_decoder",
        "test_default_paginator_request_param_xml_decoder",
    ],
)
def test_default_paginator_with_cursor(
    page_token_request_option,
    stop_condition,
    expected_updated_path,
    expected_request_params,
    expected_headers,
    expected_body_data,
    expected_body_json,
    last_record,
    expected_next_page_token,
    limit,
    decoder,
    response_body,
):
    page_size_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter,
        field_name="{{parameters['page_limit']}}",
        parameters={"page_limit": "limit"},
    )
    cursor_value = "{{ response.next }}"
    url_base = "https://airbyte.io"
    config = {}
    parameters = {}
    strategy = CursorPaginationStrategy(
        page_size=limit,
        cursor_value=cursor_value,
        stop_condition=stop_condition,
        decoder=decoder(parameters={}),
        config=config,
        parameters=parameters,
    )
    paginator = DefaultPaginator(
        page_size_option=page_size_request_option,
        page_token_option=page_token_request_option,
        pagination_strategy=strategy,
        config=config,
        url_base=url_base,
        parameters={},
    )

    response = requests.Response()
    response.headers = {"A_HEADER": "HEADER_VALUE"}
    response._content = (
        json.dumps(response_body).encode("utf-8") if decoder == JsonDecoder else response_body
    )

    actual_next_page_token = paginator.next_page_token(response, 2, last_record, None)
    actual_next_path = paginator.path(actual_next_page_token)
    actual_request_params = paginator.get_request_params(next_page_token=actual_next_page_token)
    actual_headers = paginator.get_request_headers(next_page_token=actual_next_page_token)
    actual_body_data = paginator.get_request_body_data(next_page_token=actual_next_page_token)
    actual_body_json = paginator.get_request_body_json(next_page_token=actual_next_page_token)
    assert actual_next_page_token == expected_next_page_token
    assert actual_next_path == expected_updated_path
    assert actual_request_params == expected_request_params
    assert actual_headers == expected_headers
    assert actual_body_data == expected_body_data
    assert actual_body_json == expected_body_json


@pytest.mark.parametrize(
    "field_name_page_size_interpolation, field_name_page_token_interpolation, expected_request_params",
    [
        (
            "{{parameters['page_limit']}}",
            "{{parameters['page_token']}}",
            {"parameters_limit": 50, "parameters_token": "https://airbyte.io/next_url"},
        ),
        (
            "{{config['page_limit']}}",
            "{{config['page_token']}}",
            {"config_limit": 50, "config_token": "https://airbyte.io/next_url"},
        ),
    ],
    ids=[
        "parameters_interpolation",
        "config_interpolation",
    ],
)
def test_paginator_request_param_interpolation(
    field_name_page_size_interpolation: str,
    field_name_page_token_interpolation: str,
    expected_request_params: dict,
):
    config = {"page_limit": "config_limit", "page_token": "config_token"}
    parameters = {"page_limit": "parameters_limit", "page_token": "parameters_token"}
    page_size_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter,
        field_name=field_name_page_size_interpolation,
        parameters=parameters,
    )
    cursor_value = "{{ response.next }}"
    url_base = "https://airbyte.io"
    limit = 50
    strategy = CursorPaginationStrategy(
        page_size=limit,
        cursor_value=cursor_value,
        stop_condition=None,
        decoder=JsonDecoder(parameters={}),
        config=config,
        parameters=parameters,
    )
    paginator = DefaultPaginator(
        page_size_option=page_size_request_option,
        page_token_option=RequestOption(
            inject_into=RequestOptionType.request_parameter,
            field_name=field_name_page_token_interpolation,
            parameters=parameters,
        ),
        pagination_strategy=strategy,
        config=config,
        url_base=url_base,
        parameters=parameters,
    )
    response = requests.Response()
    response.headers = {"A_HEADER": "HEADER_VALUE"}
    response_body = {"next": "https://airbyte.io/next_url"}
    response._content = json.dumps(response_body).encode("utf-8")
    last_record = {"id": 1}
    next_page_token = paginator.next_page_token(response, 2, last_record, None)
    actual_request_params = paginator.get_request_params(next_page_token=next_page_token)
    assert actual_request_params == expected_request_params


def test_page_size_option_cannot_be_set_if_strategy_has_no_limit():
    page_size_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter, field_name="page_size", parameters={}
    )
    page_token_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter, field_name="offset", parameters={}
    )
    cursor_value = "{{ response.next }}"
    url_base = "https://airbyte.io"
    config = {}
    parameters = {}
    strategy = CursorPaginationStrategy(
        page_size=None, cursor_value=cursor_value, config=config, parameters=parameters
    )
    try:
        DefaultPaginator(
            page_size_option=page_size_request_option,
            page_token_option=page_token_request_option,
            pagination_strategy=strategy,
            config=config,
            url_base=url_base,
            parameters={},
        )
        assert False
    except ValueError:
        pass


def test_initial_token_with_offset_pagination():
    page_size_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter, field_name="limit", parameters={}
    )
    page_token_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter, field_name="offset", parameters={}
    )
    url_base = "https://airbyte.io"
    config = {}
    strategy = OffsetIncrement(
        config={},
        page_size=2,
        extractor=DpathExtractor(field_path=[], parameters={}, config={}),
        parameters={},
        inject_on_first_request=True,
    )
    paginator = DefaultPaginator(
        strategy,
        config,
        url_base,
        parameters={},
        page_size_option=page_size_request_option,
        page_token_option=page_token_request_option,
    )
    initial_token = paginator.get_initial_token()
    next_page_token = {"next_page_token": initial_token}

    initial_request_parameters = paginator.get_request_params(next_page_token=next_page_token)

    assert initial_request_parameters == {"limit": 2, "offset": 0}


@pytest.mark.parametrize(
    "pagination_strategy,last_page_size,expected_next_page_token,expected_second_next_page_token",
    [
        pytest.param(
            OffsetIncrement(
                config={},
                page_size=10,
                extractor=DpathExtractor(field_path=["results"], parameters={}, config={}),
                parameters={},
                inject_on_first_request=True,
            ),
            10,
            {"next_page_token": 10},
            {"next_page_token": 20},
        ),
        pytest.param(
            PageIncrement(
                config={},
                page_size=5,
                start_from_page=0,
                parameters={},
                inject_on_first_request=True,
            ),
            5,
            {"next_page_token": 1},
            {"next_page_token": 2},
        ),
    ],
)
def test_no_inject_on_first_request_offset_pagination(
    pagination_strategy, last_page_size, expected_next_page_token, expected_second_next_page_token
):
    """
    Validate that the stateless next_page_token() works when the first page does not inject the value
    """
    response_body = {
        "results": [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
            {"id": 5},
            {"id": 6},
            {"id": 7},
            {"id": 8},
            {"id": 9},
            {"id": 10},
        ]
    }
    response = requests.Response()
    response.headers = {"A_HEADER": "HEADER_VALUE"}
    response._content = json.dumps(response_body).encode("utf-8")

    last_record = Record(data={}, stream_name="test")

    page_size_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter, field_name="limit", parameters={}
    )
    page_token_request_option = RequestOption(
        inject_into=RequestOptionType.request_parameter, field_name="offset", parameters={}
    )
    url_base = "https://airbyte.io"
    config = {}
    paginator = DefaultPaginator(
        pagination_strategy,
        config,
        url_base,
        parameters={},
        page_size_option=page_size_request_option,
        page_token_option=page_token_request_option,
    )

    actual_next_page_token = paginator.next_page_token(response, last_page_size, last_record, None)
    assert actual_next_page_token == expected_next_page_token

    last_page_token_value = actual_next_page_token["next_page_token"]
    actual_next_page_token = paginator.next_page_token(
        response, last_page_size, last_record, last_page_token_value
    )
    assert actual_next_page_token == expected_second_next_page_token


def test_limit_page_fetched():
    maximum_number_of_pages = 5
    number_of_next_performed = maximum_number_of_pages - 1
    paginator = PaginatorTestReadDecorator(
        DefaultPaginator(
            page_size_option=MagicMock(),
            page_token_option=MagicMock(),
            pagination_strategy=MagicMock(),
            config=MagicMock(),
            url_base=MagicMock(),
            parameters={},
        ),
        maximum_number_of_pages,
    )

    for _ in range(number_of_next_performed):
        last_token = paginator.next_page_token(MagicMock(), 1, MagicMock())
        assert last_token

    assert not paginator.next_page_token(MagicMock(), 1, MagicMock())


def test_paginator_with_page_option_no_page_size():
    pagination_strategy = OffsetIncrement(
        config={},
        page_size=None,
        extractor=DpathExtractor(field_path=[], parameters={}, config={}),
        parameters={},
    )

    with pytest.raises(ValueError):
        (
            DefaultPaginator(
                page_size_option=MagicMock(),
                page_token_option=RequestOption(
                    field_name="limit",
                    inject_into=RequestOptionType.request_parameter,
                    parameters={},
                ),
                pagination_strategy=pagination_strategy,
                config=MagicMock(),
                url_base=MagicMock(),
                parameters={},
            ),
        )


def test_request_option_mapping_validator():
    pagination_strategy = PageIncrement(
        config={}, page_size=1, start_from_page=0, parameters={}, inject_on_first_request=True
    )

    with pytest.raises(ValueError):
        (
            DefaultPaginator(
                page_size_option=RequestOption(
                    field_path=["variables", "limit"],
                    inject_into=RequestOptionType.body_json,
                    parameters={},
                ),
                page_token_option=RequestOption(
                    field_path=["variables", "limit"],
                    inject_into=RequestOptionType.body_json,
                    parameters={},
                ),
                pagination_strategy=pagination_strategy,
                config=MagicMock(),
                url_base=MagicMock(),
                parameters={},
            ),
        )


def test_path_returns_none_when_no_token() -> None:
    page_token_option = RequestPath(parameters={})
    paginator = DefaultPaginator(
        pagination_strategy=Mock(),
        config={},
        url_base="https://domain.com",
        parameters={},
        page_token_option=page_token_option,
    )
    result = paginator.path(None)

    assert result is None


def test_path_returns_none_when_option_not_request_path() -> None:
    token_value = "https://domain.com/next_url"
    next_page_token = {"next_page_token": token_value}

    # Use a RequestOption instead of RequestPath.
    page_token_option = RequestOption(
        inject_into=RequestOptionType.request_parameter,
        field_name="some_field",
        parameters={},
    )
    paginator = DefaultPaginator(
        pagination_strategy=Mock(),
        config={},
        url_base="https://domain.com",
        parameters={},
        page_token_option=page_token_option,
    )
    result = paginator.path(next_page_token)
    assert result is None


def _request_path_paginator(
    page_size=100,
    inject_into=RequestOptionType.request_parameter,
    page_size_field="per_page",
    config=None,
):
    return DefaultPaginator(
        page_size_option=RequestOption(
            inject_into=inject_into, field_name=page_size_field, parameters={}
        ),
        page_token_option=RequestPath(parameters={}),
        pagination_strategy=CursorPaginationStrategy(
            page_size=page_size, cursor_value="{{ response.next }}", config={}, parameters={}
        ),
        config=config if config is not None else {},
        url_base="https://airbyte.io",
        parameters={},
    )


def test_given_request_path_and_no_page_size_override_then_token_url_is_untouched():
    paginator = _request_path_paginator()
    token = "https://airbyte.io/incremental/ticket_events.json?per_page=100&start_time=1234"

    assert paginator.path({"next_page_token": token}) == token


def test_given_request_path_and_page_size_override_then_rewrite_it_in_the_token_url():
    """
    The API echoes the page size back into the URL it returns for the next page. Sending the reduced page size
    next to it would leave the API to pick between the two, and `_dedupe_query_params` only drops the injected
    one when both values agree - which is exactly when there is nothing to reduce.
    """
    paginator = _request_path_paginator()
    token = "https://airbyte.io/incremental/ticket_events.json?per_page=100&start_time=1234"

    assert (
        paginator.path({"next_page_token": token}, page_size_override=50)
        == "https://airbyte.io/incremental/ticket_events.json?per_page=50&start_time=1234"
    )


def test_given_token_url_without_the_page_size_then_it_is_left_alone():
    # `get_request_params` adds the reduced page size the usual way, so there is nothing to rewrite here and
    # appending it to the path would send it twice.
    paginator = _request_path_paginator()
    token = "https://airbyte.io/incremental/ticket_events.json?start_time=1234"

    assert paginator.path({"next_page_token": token}, page_size_override=50) == token
    assert paginator.get_request_params(page_size_override=50) == {"per_page": 50}


def test_given_token_url_without_a_query_then_it_is_left_alone():
    paginator = _request_path_paginator()
    token = "https://airbyte.io/incremental/ticket_events.json"

    assert paginator.path({"next_page_token": token}, page_size_override=50) == token


def test_given_page_size_not_injected_as_a_request_parameter_then_token_url_is_untouched():
    # A header or a body carries no page size into the URL, so there is nothing there to rewrite.
    paginator = _request_path_paginator(inject_into=RequestOptionType.header)
    token = "https://airbyte.io/incremental/ticket_events.json?per_page=100"

    assert paginator.path({"next_page_token": token}, page_size_override=50) == token


def test_given_other_query_params_then_they_are_returned_byte_for_byte():
    """
    The token URL is built by the API and handed back to it. Parsing and re-encoding its parameters would
    rewrite percent-encoding the API chose, so only the page size pair is edited.
    """
    paginator = _request_path_paginator()
    token = (
        "https://airbyte.io/incremental/ticket_events.json"
        "?cursor=MTIzNDU2%3D%3D&filter=a%2Cb&per_page=100&empty=&flag"
    )

    assert paginator.path({"next_page_token": token}, page_size_override=50) == (
        "https://airbyte.io/incremental/ticket_events.json"
        "?cursor=MTIzNDU2%3D%3D&filter=a%2Cb&per_page=50&empty=&flag"
    )


def test_given_the_page_size_repeated_in_the_token_url_then_only_one_is_kept():
    paginator = _request_path_paginator()
    token = "https://airbyte.io/events?per_page=100&start_time=1234&per_page=200"

    assert (
        paginator.path({"next_page_token": token}, page_size_override=50)
        == "https://airbyte.io/events?per_page=50&start_time=1234"
    )


def test_given_a_param_whose_name_starts_with_the_page_size_field_then_it_is_not_rewritten():
    paginator = _request_path_paginator()
    token = "https://airbyte.io/events?per_page_size=100&per_page=100"

    assert (
        paginator.path({"next_page_token": token}, page_size_override=50)
        == "https://airbyte.io/events?per_page_size=100&per_page=50"
    )


def test_given_an_interpolated_page_size_field_name_then_it_is_evaluated():
    paginator = _request_path_paginator(
        page_size_field="{{ config['size_field'] }}", config={"size_field": "per_page"}
    )
    token = "https://airbyte.io/events?per_page=100"

    assert (
        paginator.path({"next_page_token": token}, page_size_override=50)
        == "https://airbyte.io/events?per_page=50"
    )


def test_given_the_token_url_has_a_fragment_then_it_is_preserved():
    paginator = _request_path_paginator()
    token = "https://airbyte.io/events?per_page=100#anchor"

    assert (
        paginator.path({"next_page_token": token}, page_size_override=50)
        == "https://airbyte.io/events?per_page=50#anchor"
    )


def test_given_rewritten_token_url_then_the_request_carries_one_page_size():
    """
    The whole point of the rewrite: what reaches the API. `HttpClient._dedupe_query_params` drops the injected
    page size only because the rewritten URL now agrees with it - without the rewrite the two disagree and
    both are sent.
    """
    paginator = _request_path_paginator()
    token = "https://airbyte.io/events?per_page=100&start_time=1234"

    url = paginator.path({"next_page_token": token}, page_size_override=50)
    params = paginator.get_request_params(page_size_override=50)
    prepared_request = HttpClient(
        name="test", logger=logging.getLogger("test")
    )._create_prepared_request(http_method="GET", url=url, params=params, dedupe_query_params=True)

    assert prepared_request.url == "https://airbyte.io/events?per_page=50&start_time=1234"

    without_rewrite = HttpClient(
        name="test", logger=logging.getLogger("test")
    )._create_prepared_request(
        http_method="GET", url=token, params=params, dedupe_query_params=True
    )

    assert (
        without_rewrite.url == "https://airbyte.io/events?per_page=100&start_time=1234&per_page=50"
    )


def test_test_read_decorator_forwards_page_size_override_to_path():
    decorated = Mock()
    decorated.path.return_value = "a path"
    paginator = PaginatorTestReadDecorator(decorated, 5)

    assert paginator.path({"next_page_token": "a token"}, page_size_override=50) == "a path"
    assert decorated.path.call_args.kwargs["page_size_override"] == 50


def test_test_read_decorator_omits_page_size_override_on_path_when_there_is_none():
    # A paginator defined outside of the CDK does not have to accept the argument.
    decorated = Mock()
    paginator = PaginatorTestReadDecorator(decorated, 5)

    paginator.path({"next_page_token": "a token"})

    assert "page_size_override" not in decorated.path.call_args.kwargs


def _paginator_with_page_size(page_size=100, inject_into=RequestOptionType.request_parameter):
    return DefaultPaginator(
        page_size_option=RequestOption(
            inject_into=inject_into, field_name="page_size", parameters={}
        ),
        page_token_option=RequestOption(
            inject_into=RequestOptionType.request_parameter, field_name="after", parameters={}
        ),
        pagination_strategy=CursorPaginationStrategy(
            page_size=page_size, cursor_value="{{ response.next }}", config={}, parameters={}
        ),
        config={},
        url_base="https://airbyte.io",
        parameters={},
    )


def test_get_page_size_returns_the_strategy_page_size():
    assert _paginator_with_page_size().get_page_size() == 100


def test_given_page_size_override_then_injected_instead_of_the_configured_page_size():
    paginator = _paginator_with_page_size()

    assert paginator.get_request_params(page_size_override=25) == {"page_size": 25}
    assert paginator.get_request_params() == {"page_size": 100}


def test_given_page_size_override_when_injected_into_body_json_then_use_override():
    paginator = _paginator_with_page_size(inject_into=RequestOptionType.body_json)

    assert paginator.get_request_body_json(page_size_override=25) == {"page_size": 25}
    assert paginator.get_request_headers(page_size_override=25) == {}


def test_given_page_size_override_when_next_page_token_then_forward_to_strategy():
    strategy = Mock()
    strategy.next_page_token.return_value = "a token"
    paginator = DefaultPaginator(
        pagination_strategy=strategy, config={}, url_base="https://airbyte.io", parameters={}
    )
    response = requests.Response()

    paginator.next_page_token(response, 25, None, None, page_size_override=25)

    assert strategy.next_page_token.call_args.kwargs["page_size_override"] == 25


def test_given_no_page_size_override_when_next_page_token_then_strategy_called_without_the_argument():
    strategy = Mock()
    strategy.next_page_token.return_value = "a token"
    paginator = DefaultPaginator(
        pagination_strategy=strategy, config={}, url_base="https://airbyte.io", parameters={}
    )
    response = requests.Response()

    paginator.next_page_token(response, 25, None, None)

    assert "page_size_override" not in strategy.next_page_token.call_args.kwargs


def test_test_read_decorator_delegates_page_size_override():
    decorated = _paginator_with_page_size()
    paginator = PaginatorTestReadDecorator(decorated, 5)

    assert paginator.get_page_size() == 100
    assert paginator.get_request_params(page_size_override=25) == {"page_size": 25}
    assert paginator.get_request_params() == {"page_size": 100}


_TEST_READ_DECORATOR_REQUEST_OPTION_METHODS = [
    "get_request_params",
    "get_request_headers",
    "get_request_body_data",
    "get_request_body_json",
]


@pytest.mark.parametrize("method_name", _TEST_READ_DECORATOR_REQUEST_OPTION_METHODS)
def test_given_page_size_override_when_test_read_decorator_request_options_then_forward_it(
    method_name,
):
    """
    This is the Connector Builder path: a method that dropped the override would make a Builder test read of a
    reduced stream request the configured page size again, or stop after the first reduced page.
    """
    decorated = Mock()
    paginator = PaginatorTestReadDecorator(decorated, 5)

    getattr(paginator, method_name)(page_size_override=25)

    assert getattr(decorated, method_name).call_args.kwargs["page_size_override"] == 25


@pytest.mark.parametrize("method_name", _TEST_READ_DECORATOR_REQUEST_OPTION_METHODS)
def test_given_no_page_size_override_when_test_read_decorator_request_options_then_omit_it(
    method_name,
):
    decorated = Mock()
    paginator = PaginatorTestReadDecorator(decorated, 5)

    getattr(paginator, method_name)()

    assert "page_size_override" not in getattr(decorated, method_name).call_args.kwargs


def test_given_page_size_override_when_test_read_decorator_next_page_token_then_forward_it():
    decorated = Mock()
    decorated.next_page_token.return_value = "a token"
    paginator = PaginatorTestReadDecorator(decorated, 5)
    response = requests.Response()

    assert paginator.next_page_token(response, 25, None, None, page_size_override=25) == "a token"
    assert decorated.next_page_token.call_args.kwargs["page_size_override"] == 25


def test_given_no_page_size_override_when_test_read_decorator_next_page_token_then_omit_it():
    decorated = Mock()
    decorated.next_page_token.return_value = "a token"
    paginator = PaginatorTestReadDecorator(decorated, 5)
    response = requests.Response()

    paginator.next_page_token(response, 25, None, None)

    assert "page_size_override" not in decorated.next_page_token.call_args.kwargs


def test_test_read_decorator_delegates_get_page_size():
    decorated = Mock()
    decorated.get_page_size.return_value = 100
    paginator = PaginatorTestReadDecorator(decorated, 5)

    assert paginator.get_page_size() == 100
