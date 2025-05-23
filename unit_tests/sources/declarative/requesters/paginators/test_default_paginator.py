#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
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


@pytest.mark.parametrize(
    "page_token_request_option, stop_condition, expected_updated_path, expected_request_params, expected_headers, expected_body_data, expected_body_json, last_record, expected_next_page_token, limit, decoder, response_body",
    [
        (
            RequestPath(parameters={}),
            None,
            "/next_url",
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
            "/next_url",
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


def test_path_with_additional_interpolation_context() -> None:
    page_token_option = RequestPath(parameters={})
    paginator = DefaultPaginator(
        pagination_strategy=Mock(),
        config={},
        url_base="https://api.domain.com/{{ stream_slice['campaign_id'] }}",
        parameters={},
        page_token_option=page_token_option,
    )
    # define stream_state here
    stream_state = {"state": "state_value"}
    # define stream_slice here
    stream_slice = StreamSlice(
        partition={
            "campaign_id": "123_abcd",
        },
        cursor_slice={
            "start": "A",
            "end": "B",
        },
        extra_fields={
            "extra_field_A": "value_A",
            "extra_field_B": "value_B",
        },
    )
    # define next_page_token here
    next_page_token = {
        "next_page_token": "https://api.domain.com/123_abcd/some_next_page_token_here"
    }

    expected_after_interpolation = "/some_next_page_token_here"

    assert expected_after_interpolation == paginator.path(
        next_page_token=next_page_token,
        stream_state=stream_state,
        stream_slice=stream_slice,
    )
