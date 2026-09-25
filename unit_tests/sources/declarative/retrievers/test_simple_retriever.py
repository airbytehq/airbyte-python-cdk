#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import partial
from typing import Any, Callable, Iterable, List, Mapping, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from airbyte_cdk.models import (
    AirbyteLogMessage,
    AirbyteMessage,
    FailureType,
    Level,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import NoAuth
from airbyte_cdk.sources.declarative.decoders import JsonDecoder
from airbyte_cdk.sources.declarative.extractors import DpathExtractor, HttpSelector, RecordSelector
from airbyte_cdk.sources.declarative.extractors.record_filter import (
    ClientSideIncrementalRecordFilterDecorator,
)
from airbyte_cdk.sources.declarative.partition_routers import SinglePartitionRouter
from airbyte_cdk.sources.declarative.requesters.paginators import DefaultPaginator, Paginator
from airbyte_cdk.sources.declarative.requesters.paginators.strategies import (
    CursorPaginationStrategy,
    PageIncrement,
)
from airbyte_cdk.sources.declarative.requesters.query_properties import (
    PropertyChunking,
    QueryProperties,
)
from airbyte_cdk.sources.declarative.requesters.query_properties.property_chunking import (
    GroupByKey,
    PropertyLimitType,
)
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.sources.declarative.requesters.requester import HttpMethod, Requester
from airbyte_cdk.sources.declarative.retrievers.page_size_reducer import (
    PageSizeReducer,
    PageSizeReduction,
    PageSizeResetPolicy,
)
from airbyte_cdk.sources.declarative.retrievers.pagination_tracker import PaginationTracker
from airbyte_cdk.sources.declarative.retrievers.request_window_splitting import (
    RequestWindowSplitting,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import (
    _MAX_REQUEST_WINDOW_SPLIT_DEPTH,
    SimpleRetriever,
)
from airbyte_cdk.sources.streams.http.page_size_reduction_exception import (
    PageSizeReductionRequiredException,
)
from airbyte_cdk.sources.streams.http.pagination_reset_exception import (
    PaginationResetRequiredException,
)
from airbyte_cdk.sources.streams.http.request_window_split_exception import (
    RequestWindowSplitNotSupportedException,
    RequestWindowSplitRequiredException,
)
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

A_RECORD_SCHEMA = {}
A_SLICE_STATE = {"slice_state": "slice state value"}
A_STREAM_NAME = "stream_name"
A_STREAM_SLICE = StreamSlice(cursor_slice={"stream slice": "slice value"}, partition={})
A_STREAM_STATE = {"stream state": "state value"}

primary_key = "pk"
records = [{"id": 1}, {"id": 2}]
request_response_logs = [
    AirbyteLogMessage(level=Level.INFO, message="request:{}"),
    AirbyteLogMessage(level=Level.INFO, message="response{}"),
]
config = {}


@patch.object(SimpleRetriever, "_read_pages", return_value=iter([]))
def test_simple_retriever_full(mock_http_stream):
    requester = MagicMock()
    request_params = {"param": "value"}
    requester.get_request_params.return_value = request_params

    requester.get_request_params.__name__ = "get_request_params"
    requester.get_request_headers.__name__ = "get_request_headers"
    requester.get_request_body_data.__name__ = "get_request_body_data"
    requester.get_request_body_json.__name__ = "get_request_body_json"

    paginator = MagicMock()
    paginator.get_initial_token.return_value = None
    next_page_token = {"cursor": "cursor_value"}
    paginator.path.return_value = None
    paginator.next_page_token.return_value = next_page_token
    paginator.get_request_headers.return_value = {}

    paginator.get_request_params.__name__ = "get_request_params"
    paginator.get_request_headers.__name__ = "get_request_headers"
    paginator.get_request_body_data.__name__ = "get_request_body_data"
    paginator.get_request_body_json.__name__ = "get_request_body_json"

    record_selector = MagicMock()
    record_selector.select_records.return_value = records

    response = requests.Response()
    response.status_code = 200

    last_page_size = 2
    last_record = Record(data={"id": "1a"}, stream_name="stream_name")
    last_page_token_value = 0

    underlying_state = {"date": "2021-01-01"}

    requester.get_authenticator.return_value = NoAuth({})
    url_base = "https://airbyte.io"
    requester.get_url_base.return_value = url_base
    path = "/v1"
    requester.get_path.return_value = path
    http_method = HttpMethod.GET
    requester.get_method.return_value = http_method
    should_retry = True
    requester.interpret_response_status.return_value = should_retry
    request_body_json = {"body": "json"}
    requester.request_body_json.return_value = request_body_json

    request_body_data = {"body": "data"}
    requester.get_request_body_data.return_value = request_body_data
    request_body_json = {"body": "json"}
    requester.get_request_body_json.return_value = request_body_json
    request_kwargs = {"kwarg": "value"}
    requester.request_kwargs.return_value = request_kwargs

    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=requester,
        paginator=paginator,
        record_selector=record_selector,
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )

    assert retriever.primary_key == primary_key
    assert (
        retriever._next_page_token(response, last_page_size, last_record, last_page_token_value)
        == next_page_token
    )
    assert retriever._request_params(None, None) == {}


@patch.object(SimpleRetriever, "_read_pages", return_value=iter([*request_response_logs, *records]))
def test_simple_retriever_with_request_response_logs(mock_http_stream):
    requester = MagicMock()
    paginator = MagicMock()
    record_selector = MagicMock()

    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=requester,
        paginator=paginator,
        record_selector=record_selector,
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )

    actual_messages = [r for r in retriever.read_records(SyncMode.full_refresh)]

    assert isinstance(actual_messages[0], AirbyteLogMessage)
    assert isinstance(actual_messages[1], AirbyteLogMessage)
    assert actual_messages[2] == records[0]
    assert actual_messages[3] == records[1]


@pytest.mark.parametrize(
    "test_name, paginator_mapping, request_options_provider_mapping, expected_mapping",
    [
        ("test_empty_headers", {}, {}, {}),
        (
            "test_header_from_pagination_and_slicer",
            {"offset": 1000},
            {"key": "value"},
            {"key": "value", "offset": 1000},
        ),
        ("test_header_from_stream_slicer", {}, {"slice": "slice_value"}, {"slice": "slice_value"}),
        ("test_duplicate_header_slicer_paginator", {"k": "v"}, {"k": "slice_value"}, None),
    ],
)
def test_get_request_options_from_pagination(
    test_name, paginator_mapping, request_options_provider_mapping, expected_mapping
):
    # This test does not test request headers because they must be strings
    paginator = MagicMock()
    paginator.get_request_params.return_value = paginator_mapping
    paginator.get_request_body_data.return_value = paginator_mapping
    paginator.get_request_body_json.return_value = paginator_mapping

    paginator.get_request_params.__name__ = "get_request_params"
    paginator.get_request_body_data.__name__ = "get_request_body_data"
    paginator.get_request_body_json.__name__ = "get_request_body_json"

    request_options_provider = MagicMock()
    request_options_provider.get_request_params.return_value = request_options_provider_mapping
    request_options_provider.get_request_body_data.return_value = request_options_provider_mapping
    request_options_provider.get_request_body_json.return_value = request_options_provider_mapping

    request_options_provider.get_request_params.__name__ = "get_request_params"
    request_options_provider.get_request_body_data.__name__ = "get_request_body_data"
    request_options_provider.get_request_body_json.__name__ = "get_request_body_json"

    record_selector = MagicMock()
    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=MagicMock(),
        record_selector=record_selector,
        paginator=paginator,
        request_option_provider=request_options_provider,
        parameters={},
        config={},
    )

    request_option_type_to_method = {
        RequestOptionType.request_parameter: retriever._request_params,
        RequestOptionType.body_data: retriever._request_body_data,
        RequestOptionType.body_json: retriever._request_body_json,
    }

    for _, method in request_option_type_to_method.items():
        if expected_mapping is not None:
            actual_mapping = method(None, None)
            assert actual_mapping == expected_mapping
        else:
            try:
                method(None, None)
                assert False
            except ValueError:
                pass


@pytest.mark.parametrize(
    "test_name, paginator_mapping, expected_mapping",
    [
        ("test_only_base_headers", {}, {"key": "value"}),
        ("test_header_from_pagination", {"offset": 1000}, {"key": "value", "offset": "1000"}),
        ("test_duplicate_header", {"key": 1000}, None),
    ],
)
def test_get_request_headers(test_name, paginator_mapping, expected_mapping):
    # This test is separate from the other request options because request headers must be strings
    paginator = MagicMock()
    paginator.get_request_headers.return_value = paginator_mapping
    paginator.get_request_headers.__name__ = "get_request_headers"
    requester = MagicMock(use_cache=False)

    request_option_provider = MagicMock()
    request_option_provider.get_request_headers.return_value = {"key": "value"}
    request_option_provider.get_request_headers.__name__ = "get_request_headers"

    record_selector = MagicMock()
    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        request_option_provider=request_option_provider,
        paginator=paginator,
        parameters={},
        config={},
    )

    request_option_type_to_method = {
        RequestOptionType.header: retriever._request_headers,
    }

    for _, method in request_option_type_to_method.items():
        if expected_mapping:
            actual_mapping = method(None, None)
            assert actual_mapping == expected_mapping
        else:
            try:
                method(None, None)
                assert False
            except ValueError:
                pass


@pytest.mark.parametrize(
    "test_name, paginator_mapping, ignore_stream_slicer_parameters_on_paginated_requests, next_page_token, expected_mapping",
    [
        (
            "test_do_not_ignore_stream_slicer_params_if_ignore_is_true_but_no_next_page_token",
            {"key_from_pagination": "1000"},
            True,
            None,
            {"key_from_pagination": "1000"},
        ),
        (
            "test_do_not_ignore_stream_slicer_params_if_ignore_is_false_and_no_next_page_token",
            {"key_from_pagination": "1000"},
            False,
            None,
            {"key_from_pagination": "1000", "key_from_slicer": "value"},
        ),
        (
            "test_ignore_stream_slicer_params_on_paginated_request",
            {"key_from_pagination": "1000"},
            True,
            {"page": 2},
            {"key_from_pagination": "1000"},
        ),
        (
            "test_do_not_ignore_stream_slicer_params_on_paginated_request",
            {"key_from_pagination": "1000"},
            False,
            {"page": 2},
            {"key_from_pagination": "1000", "key_from_slicer": "value"},
        ),
    ],
)
def test_ignore_request_option_provider_parameters_on_paginated_requests(
    test_name,
    paginator_mapping,
    ignore_stream_slicer_parameters_on_paginated_requests,
    next_page_token,
    expected_mapping,
):
    # This test is separate from the other request options because request headers must be strings
    paginator = MagicMock()
    paginator.get_request_headers.return_value = paginator_mapping
    paginator.get_request_headers.__name__ = "get_request_headers"
    requester = MagicMock(use_cache=False)

    request_option_provider = MagicMock()
    request_option_provider.get_request_headers.return_value = {"key_from_slicer": "value"}
    request_option_provider.get_request_headers.__name__ = "get_request_headers"

    record_selector = MagicMock()
    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        request_option_provider=request_option_provider,
        paginator=paginator,
        ignore_stream_slicer_parameters_on_paginated_requests=ignore_stream_slicer_parameters_on_paginated_requests,
        parameters={},
        config={},
    )

    request_option_type_to_method = {
        RequestOptionType.header: retriever._request_headers,
    }

    for _, method in request_option_type_to_method.items():
        actual_mapping = method(None, next_page_token={"next_page_token": "1000"})
        assert actual_mapping == expected_mapping


@pytest.mark.parametrize(
    "test_name, request_options_provider_body_data, paginator_body_data, expected_body_data",
    [
        ("test_only_slicer_mapping", {"key": "value"}, {}, {"key": "value"}),
        ("test_only_slicer_string", "key=value", {}, "key=value"),
        (
            "test_slicer_mapping_and_paginator_no_duplicate",
            {"key": "value"},
            {"offset": 1000},
            {"key": "value", "offset": 1000},
        ),
        ("test_slicer_mapping_and_paginator_with_duplicate", {"key": "value"}, {"key": 1000}, None),
        ("test_slicer_string_and_paginator", "key=value", {"offset": 1000}, None),
    ],
)
def test_request_body_data(
    test_name, request_options_provider_body_data, paginator_body_data, expected_body_data
):
    paginator = MagicMock()
    paginator.get_request_body_data.return_value = paginator_body_data
    paginator.get_request_body_data.__name__ = "get_request_body_data"
    requester = MagicMock(use_cache=False)

    request_option_provider = MagicMock()
    request_option_provider.get_request_body_data.return_value = request_options_provider_body_data

    record_selector = MagicMock()
    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        request_option_provider=request_option_provider,
        parameters={},
        config={},
    )

    if expected_body_data:
        actual_body_data = retriever._request_body_data(None, None)
        assert actual_body_data == expected_body_data
    else:
        try:
            retriever._request_body_data(None, None)
            assert False
        except ValueError:
            pass


@pytest.mark.parametrize(
    "test_name, requester_path, paginator_path, expected_path",
    [
        ("test_path_from_requester", "/v1/path", None, None),
        ("test_path_from_paginator", "/v1/path/", "/v2/paginator", "/v2/paginator"),
    ],
)
def test_path(test_name, requester_path, paginator_path, expected_path):
    paginator = MagicMock()
    paginator.path.return_value = paginator_path
    requester = MagicMock(use_cache=False)

    requester.get_path.return_value = requester_path

    record_selector = MagicMock()
    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        parameters={},
        config={},
    )

    actual_path = retriever._paginator_path(next_page_token=None)
    assert actual_path == expected_path


def test_given_stream_data_is_not_record_when_read_records_then_update_slice_with_optional_record():
    stream_data = [
        AirbyteMessage(
            type=Type.LOG, log=AirbyteLogMessage(level=Level.INFO, message="a log message")
        )
    ]
    record_selector = MagicMock()
    record_selector.select_records.return_value = []

    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=MagicMock(),
        paginator=Mock(),
        record_selector=record_selector,
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )
    stream_slice = StreamSlice(cursor_slice={}, partition={"repository": "airbyte"})

    def retriever_read_pages(_, __):
        return retriever._parse_records(
            response=MagicMock(), stream_slice=stream_slice, records_schema={}
        )

    with patch.object(
        SimpleRetriever,
        "_read_pages",
        return_value=iter(stream_data),
        side_effect=retriever_read_pages,
    ):
        list(retriever.read_records(stream_slice=stream_slice, records_schema={}))


def test_given_initial_token_is_zero_when_read_records_then_pass_initial_token():
    record_selector = MagicMock()
    record_selector.select_records.return_value = []
    paginator = MagicMock()
    paginator.get_initial_token.return_value = 0
    paginator.next_page_token.return_value = None

    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=MagicMock(),
        paginator=paginator,
        record_selector=record_selector,
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )
    stream_slice = StreamSlice(cursor_slice={}, partition={})

    response = requests.Response()
    response.status_code = 200
    response._content = "{}".encode()

    with patch.object(
        SimpleRetriever,
        "_fetch_next_page",
        return_value=response,
    ) as fetch_next_page_mock:
        list(retriever.read_records(stream_slice=stream_slice, records_schema={}))
        fetch_next_page_mock.assert_called_once_with(stream_slice, {"next_page_token": 0})


def _generate_slices(number_of_slices):
    return [{"date": f"2022-01-0{day + 1}"} for day in range(number_of_slices)]


@patch.object(SimpleRetriever, "_read_pages", return_value=iter([]))
def test_given_state_selector_when_read_records_use_stream_state(http_stream_read_pages, mocker):
    requester = MagicMock()
    paginator = MagicMock()
    record_selector = MagicMock()

    retriever = SimpleRetriever(
        name="stream_name",
        primary_key=primary_key,
        requester=requester,
        paginator=paginator,
        record_selector=record_selector,
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )

    list(retriever.read_records(stream_slice=A_STREAM_SLICE, records_schema={}))

    http_stream_read_pages.assert_called_once_with(mocker.ANY, A_STREAM_SLICE)


def test_retriever_last_page_size_for_page_increment():
    requester = MagicMock()
    requester.send_request.return_value = MagicMock()

    paginator = DefaultPaginator(
        config={},
        pagination_strategy=PageIncrement(config={}, page_size=5, parameters={}),
        url_base="https://airbyte.io",
        parameters={},
    )

    retriever = SimpleRetriever(
        name="employees",
        primary_key=primary_key,
        requester=requester,
        paginator=paginator,
        record_selector=MagicMock(),
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )

    expected_records = [
        Record(data={"id": "1a", "name": "Cross Product Sales"}, stream_name="departments"),
        Record(data={"id": "2b", "name": "Foreign Exchange"}, stream_name="departments"),
        Record(data={"id": "3c", "name": "Wealth Management"}, stream_name="departments"),
        Record(data={"id": "4d", "name": "Investment Banking Division"}, stream_name="departments"),
    ]

    def mock_parse_records(response: Optional[requests.Response]) -> Iterable[Record]:
        yield from expected_records

    actual_records = list(
        retriever._read_pages(
            records_generator_fn=mock_parse_records,
            stream_slice=StreamSlice(cursor_slice={}, partition={}),
        )
    )
    assert actual_records == expected_records


def test_retriever_last_record_for_page_increment():
    requester = MagicMock()
    requester.send_request.return_value = MagicMock()

    paginator = DefaultPaginator(
        config={},
        pagination_strategy=CursorPaginationStrategy(
            cursor_value="{{ last_record['id'] }}",
            stop_condition="{{ last_record['last_record'] }}",
            config={},
            parameters={},
        ),
        url_base="https://airbyte.io",
        parameters={},
    )

    retriever = SimpleRetriever(
        name="employees",
        primary_key=primary_key,
        requester=requester,
        paginator=paginator,
        record_selector=MagicMock(),
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )

    expected_records = [
        Record(data={"id": "a", "name": "Cross Product Sales"}, stream_name="departments"),
        Record(data={"id": "b", "name": "Foreign Exchange"}, stream_name="departments"),
        Record(data={"id": "c", "name": "Wealth Management"}, stream_name="departments"),
        Record(
            data={"id": "d", "name": "Investment Banking Division", "last_record": True},
            stream_name="departments",
        ),
    ]

    def mock_parse_records(response: Optional[requests.Response]) -> Iterable[Record]:
        yield from expected_records

    actual_records = list(
        retriever._read_pages(
            records_generator_fn=mock_parse_records,
            stream_slice=StreamSlice(cursor_slice={}, partition={}),
        )
    )
    assert actual_records == expected_records


def test_retriever_is_stateless():
    """
    Special test case to verify that retrieving the pages for a given slice does not affect an internal
    state of the component. Specifically, because this test don't call any type of reset so invoking the
    _read_pages() method twice will fail if there is an internal state (and is therefore not stateless)
    because the page count will not be reset.
    """

    page_response_1 = requests.Response()
    page_response_1.status_code = 200
    page_response_1._content = json.dumps(
        {
            "employees": [
                {"id": "0", "first_name": "eric", "last_name": "tao"},
                {"id": "1", "first_name": "rishi", "last_name": "ramdani"},
                {"id": "2", "first_name": "harper", "last_name": "stern"},
                {"id": "3", "first_name": "robert", "last_name": "spearing"},
                {"id": "4", "first_name": "yasmin", "last_name": "kara-hanani"},
            ]
        }
    ).encode("utf-8")

    page_response_2 = requests.Response()
    page_response_2.status_code = 200
    page_response_2._content = json.dumps(
        {
            "employees": [
                {"id": "5", "first_name": "daria", "last_name": "greenock"},
                {"id": "6", "first_name": "venetia", "last_name": "berens"},
                {"id": "7", "first_name": "kenny", "last_name": "killbane"},
            ]
        }
    ).encode("utf-8")

    def mock_send_request(
        next_page_token: Optional[Mapping[str, Any]] = None, **kwargs
    ) -> Optional[requests.Response]:
        page_number = next_page_token.get("next_page_token") if next_page_token else None
        if page_number is None:
            return page_response_1
        elif page_number == 1:
            return page_response_2
        else:
            raise ValueError(f"Requested an invalid page number {page_number}")

    requester = MagicMock()
    requester.send_request.side_effect = mock_send_request

    decoder = JsonDecoder(parameters={})
    extractor = DpathExtractor(
        field_path=["employees"], decoder=decoder, config=config, parameters={}
    )
    record_selector = RecordSelector(
        name="employees",
        extractor=extractor,
        record_filter=None,
        transformations=[],
        config=config,
        parameters={},
        schema_normalization=TypeTransformer(TransformConfig.DefaultSchemaNormalization),
    )

    paginator = DefaultPaginator(
        config={},
        pagination_strategy=PageIncrement(config={}, page_size=5, parameters={}),
        url_base="https://airbyte.io",
        parameters={},
    )

    retriever = SimpleRetriever(
        name="employees",
        primary_key=primary_key,
        requester=requester,
        paginator=paginator,
        record_selector=record_selector,
        stream_slicer=SinglePartitionRouter(parameters={}),
        parameters={},
        config={},
    )

    _slice = StreamSlice(cursor_slice={}, partition={})

    record_generator = partial(
        retriever._parse_records,
        stream_slice=_slice,
        records_schema={},
    )

    # We call _read_pages() because the existing read_records() used to modify and reset state whereas
    # _read_pages() did not invoke any methods to reset state
    actual_records = list(
        retriever._read_pages(records_generator_fn=record_generator, stream_slice=_slice)
    )
    assert len(actual_records) == 8
    assert actual_records[0] == Record(
        data={"id": "0", "first_name": "eric", "last_name": "tao"}, stream_name="employees"
    )
    assert actual_records[7] == Record(
        data={"id": "7", "first_name": "kenny", "last_name": "killbane"}, stream_name="employees"
    )

    actual_records = list(
        retriever._read_pages(records_generator_fn=record_generator, stream_slice=_slice)
    )
    assert len(actual_records) == 8
    assert actual_records[2] == Record(
        data={"id": "2", "first_name": "harper", "last_name": "stern"}, stream_name="employees"
    )
    assert actual_records[5] == Record(
        data={"id": "5", "first_name": "daria", "last_name": "greenock"}, stream_name="employees"
    )


def test_simple_retriever_with_additional_query_properties():
    stream_name = "stream_name"
    expected_records = [
        Record(
            {
                "id": "a",
                "first_name": "gentarou",
                "last_name": "hongou",
                "nonary": "second",
                "bracelet": "1",
                "dict_field": {
                    "key1": "value1",
                    "key2": "value2",
                    "affiliation": {
                        "company": "cradle",
                        "industry": "pharmaceutical",
                    },
                },
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "b",
                "first_name": "clover",
                "last_name": "field",
                "nonary": "ambidex",
                "bracelet": "green",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "c",
                "first_name": "akane",
                "last_name": "kurashiki",
                "nonary": "second",
                "bracelet": "6",
                "allies": ["aoi_kurashiki"],
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "d",
                "first_name": "sigma",
                "last_name": "klim",
                "nonary": "ambidex",
                "bracelet": "red",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "e",
                "first_name": "light",
                "last_name": "field",
                "nonary": "second",
                "bracelet": "2",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
    ]

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({"data": [record.data for record in expected_records]}).encode(
        "utf-8"
    )

    requester = MagicMock()
    requester.send_request.side_effect = [
        response,
        response,
    ]

    record_selector = MagicMock()
    record_selector.select_records.side_effect = [
        [
            Record(
                data={
                    "id": "a",
                    "first_name": "gentarou",
                    "last_name": "hongou",
                    "dict_field": {"key1": "value1", "affiliation": {"company": "cradle"}},
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "b", "first_name": "clover", "last_name": "field"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={
                    "id": "c",
                    "first_name": "akane",
                    "last_name": "kurashiki",
                    "allies": ["aoi_kurashiki"],
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "d", "first_name": "sigma", "last_name": "klim"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "e", "first_name": "light", "last_name": "field"},
                associated_slice=None,
                stream_name=stream_name,
            ),
        ],
        [
            Record(
                data={"id": "e", "nonary": "second", "bracelet": "2"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "d", "nonary": "ambidex", "bracelet": "red"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "c", "nonary": "second", "bracelet": "6"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "b", "nonary": "ambidex", "bracelet": "green"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={
                    "id": "a",
                    "nonary": "second",
                    "bracelet": "1",
                    "dict_field": {"key2": "value2", "affiliation": {"industry": "pharmaceutical"}},
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
        ],
    ]

    query_properties = QueryProperties(
        property_list=["first_name", "last_name", "nonary", "bracelet"],
        always_include_properties=[],
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=2,
            record_merge_strategy=GroupByKey(key="id", config=config, parameters={}),
            config=config,
            parameters={},
        ),
        property_selector=None,
        config=config,
        parameters={},
    )

    retriever = SimpleRetriever(
        name=stream_name,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        additional_query_properties=query_properties,
        parameters={},
        config={},
    )

    actual_records = [
        r for r in retriever.read_records(records_schema={}, stream_slice=stream_slice)
    ]

    assert len(actual_records) == 5
    assert actual_records == expected_records


def test_simple_retriever_with_additional_query_properties_but_without_property_chunking():
    stream_name = "stream_name"
    expected_records = [
        Record(
            data={"id": "a", "field": "value_first_page"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "b", "field": "value_second_page"},
            associated_slice=None,
            stream_name=stream_name,
        ),
    ]

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({"data": [{"whatever": 1}]}).encode("utf-8")

    requester = MagicMock()
    requester.send_request.side_effect = [
        response,
        response,
    ]

    record_selector = MagicMock()
    record_selector.select_records.side_effect = [
        [
            Record(
                data={"id": "a", "field": "value_first_page"},
                associated_slice=None,
                stream_name=stream_name,
            ),
        ],
        [
            Record(
                data={"id": "b", "field": "value_second_page"},
                associated_slice=None,
                stream_name=stream_name,
            ),
        ],
    ]

    query_properties = QueryProperties(
        property_list=["first_name", "last_name", "nonary", "bracelet"],
        always_include_properties=[],
        property_chunking=None,
        property_selector=None,
        config=config,
        parameters={},
    )

    paginator = _mock_paginator()
    paginator.next_page_token.side_effect = [{"next_page_token": 1}, None]

    retriever = SimpleRetriever(
        name=stream_name,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        additional_query_properties=query_properties,
        paginator=paginator,
        parameters={},
        config={},
    )

    actual_records = [
        r for r in retriever.read_records(records_schema={}, stream_slice=stream_slice)
    ]

    assert len(actual_records) == 2
    assert actual_records == expected_records
    assert requester.send_request.call_args_list[0].kwargs["stream_slice"].extra_fields


def test_simple_retriever_with_additional_query_properties_single_chunk():
    stream_name = "stream_name"
    expected_records = [
        Record(
            {
                "id": "a",
                "first_name": "gentarou",
                "last_name": "hongou",
                "nonary": "second",
                "bracelet": "1",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "b",
                "first_name": "clover",
                "last_name": "field",
                "nonary": "ambidex",
                "bracelet": "green",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "c",
                "first_name": "akane",
                "last_name": "kurashiki",
                "nonary": "second",
                "bracelet": "6",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "d",
                "first_name": "sigma",
                "last_name": "klim",
                "nonary": "ambidex",
                "bracelet": "red",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {
                "id": "e",
                "first_name": "light",
                "last_name": "field",
                "nonary": "second",
                "bracelet": "2",
            },
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            {"id": "f", "first_name": "carlos", "nonary": "decision", "bracelet": "c"},
            associated_slice=None,
            stream_name=stream_name,
        ),
    ]

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({"data": [record.data for record in expected_records]}).encode(
        "utf-8"
    )

    requester = MagicMock()
    requester.send_request.side_effect = [
        response,
        response,
    ]

    record_selector = MagicMock()
    record_selector.select_records.side_effect = [
        [
            Record(
                data={
                    "id": "a",
                    "first_name": "gentarou",
                    "last_name": "hongou",
                    "nonary": "second",
                    "bracelet": "1",
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={
                    "id": "b",
                    "first_name": "clover",
                    "last_name": "field",
                    "nonary": "ambidex",
                    "bracelet": "green",
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={
                    "id": "c",
                    "first_name": "akane",
                    "last_name": "kurashiki",
                    "nonary": "second",
                    "bracelet": "6",
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={
                    "id": "d",
                    "first_name": "sigma",
                    "last_name": "klim",
                    "nonary": "ambidex",
                    "bracelet": "red",
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={
                    "id": "e",
                    "first_name": "light",
                    "last_name": "field",
                    "nonary": "second",
                    "bracelet": "2",
                },
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "f", "first_name": "carlos", "nonary": "decision", "bracelet": "c"},
                associated_slice=None,
                stream_name=stream_name,
            ),
        ]
    ]

    query_properties = QueryProperties(
        property_list=["first_name", "last_name", "nonary", "bracelet"],
        always_include_properties=[],
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=10,
            record_merge_strategy=GroupByKey(key="id", config=config, parameters={}),
            config=config,
            parameters={},
        ),
        property_selector=None,
        config=config,
        parameters={},
    )

    retriever = SimpleRetriever(
        name=stream_name,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        additional_query_properties=query_properties,
        parameters={},
        config={},
    )

    actual_records = [
        r for r in retriever.read_records(records_schema={}, stream_slice=stream_slice)
    ]

    assert len(actual_records) == 6
    assert actual_records == expected_records


def test_simple_retriever_still_emit_records_if_no_merge_key():
    stream_name = "stream_name"
    expected_records = [
        Record(
            data={"id": "a", "first_name": "gentarou", "last_name": "hongou"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "b", "first_name": "clover", "last_name": "field"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "c", "first_name": "akane", "last_name": "kurashiki"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "d", "first_name": "sigma", "last_name": "klim"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "e", "first_name": "light", "last_name": "field"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "e", "nonary": "second", "bracelet": "2"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "d", "nonary": "ambidex", "bracelet": "red"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "c", "nonary": "second", "bracelet": "6"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "b", "nonary": "ambidex", "bracelet": "green"},
            associated_slice=None,
            stream_name=stream_name,
        ),
        Record(
            data={"id": "a", "nonary": "second", "bracelet": "1"},
            associated_slice=None,
            stream_name=stream_name,
        ),
    ]

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({"data": [record.data for record in expected_records]}).encode(
        "utf-8"
    )

    requester = MagicMock()
    requester.send_request.side_effect = [
        response,
        response,
    ]

    record_selector = MagicMock()
    record_selector.select_records.side_effect = [
        [
            Record(
                data={"id": "a", "first_name": "gentarou", "last_name": "hongou"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "b", "first_name": "clover", "last_name": "field"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "c", "first_name": "akane", "last_name": "kurashiki"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "d", "first_name": "sigma", "last_name": "klim"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "e", "first_name": "light", "last_name": "field"},
                associated_slice=None,
                stream_name=stream_name,
            ),
        ],
        [
            Record(
                data={"id": "e", "nonary": "second", "bracelet": "2"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "d", "nonary": "ambidex", "bracelet": "red"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "c", "nonary": "second", "bracelet": "6"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "b", "nonary": "ambidex", "bracelet": "green"},
                associated_slice=None,
                stream_name=stream_name,
            ),
            Record(
                data={"id": "a", "nonary": "second", "bracelet": "1"},
                associated_slice=None,
                stream_name=stream_name,
            ),
        ],
    ]

    query_properties = QueryProperties(
        property_list=["first_name", "last_name", "nonary", "bracelet"],
        always_include_properties=[],
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=2,
            record_merge_strategy=GroupByKey(key="not_real", config=config, parameters={}),
            config=config,
            parameters={},
        ),
        property_selector=None,
        config=config,
        parameters={},
    )

    retriever = SimpleRetriever(
        name=stream_name,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        additional_query_properties=query_properties,
        parameters={},
        config={},
    )

    actual_records = [
        r for r in retriever.read_records(records_schema={}, stream_slice=stream_slice)
    ]

    assert len(actual_records) == 10
    assert actual_records == expected_records


def test_given_requester_raise_pagination_reset_exception_when_read_records_than_reduce_slice_range_and_retry_with_new_slice():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        [{"id": 1}],
        PaginationResetRequiredException(),
        [{"id": 2}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [
        [{"id": 1}],
        [{"id": 2}],
    ]
    pagination_tracker = Mock(spec=PaginationTracker)
    pagination_tracker.has_reached_limit.return_value = False
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = 1
    paginator.next_page_token.side_effect = [
        {"next_page_token": 2},
        None,
    ]
    retriever = SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        pagination_tracker_factory=lambda: pagination_tracker,
        parameters={},
        config={},
    )

    x = list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert len(x) == 2
    assert pagination_tracker.reduce_slice_range_if_possible.call_count == 1
    assert requester.send_request.call_count == 3
    assert requester.send_request.call_args_list[1].kwargs["stream_slice"] == A_STREAM_SLICE
    assert requester.send_request.call_args_list[1].kwargs["next_page_token"] == {
        "next_page_token": 2
    }
    assert (
        requester.send_request.call_args_list[2].kwargs["stream_slice"]
        == pagination_tracker.reduce_slice_range_if_possible.return_value
    )
    assert requester.send_request.call_args_list[2].kwargs["next_page_token"] == {
        "next_page_token": 1
    }


def test_given_reach_pagination_limit_after_two_pages_when_read_records_than_reduce_slice_range_and_retry_with_new_slice():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        [{"id": 1}],
        [{"id": 2}],
        [{"id": 3}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [
        [{"id": 1}],
        [{"id": 2}],
        [{"id": 3}],
    ]
    pagination_tracker = Mock(spec=PaginationTracker)
    pagination_tracker.has_reached_limit.side_effect = [
        False,
        True,
        False,
    ]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = 1
    paginator.next_page_token.side_effect = [
        {"next_page_token": 2},
        None,
    ]
    retriever = SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        pagination_tracker_factory=lambda: pagination_tracker,
        parameters={},
        config={},
    )

    x = list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert len(x) == 3
    assert pagination_tracker.reduce_slice_range_if_possible.call_count == 1
    assert requester.send_request.call_count == 3
    assert requester.send_request.call_args_list[1].kwargs["stream_slice"] == A_STREAM_SLICE
    assert requester.send_request.call_args_list[1].kwargs["next_page_token"] == {
        "next_page_token": 2
    }
    assert (
        requester.send_request.call_args_list[2].kwargs["stream_slice"]
        == pagination_tracker.reduce_slice_range_if_possible.return_value
    )
    assert requester.send_request.call_args_list[2].kwargs["next_page_token"] == {
        "next_page_token": 1
    }


@pytest.fixture(autouse=True)
def _no_page_size_reduction_backoff(monkeypatch):
    """The reducer waits between reduction retries; taking those waits for real adds seconds to every CI run."""
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.retrievers.page_size_reducer.time.sleep", lambda _: None
    )


def _page_size_reduction_retriever(
    requester: Requester,
    paginator: Paginator,
    record_selector: HttpSelector,
    page_size_reduction: PageSizeReduction,
) -> SimpleRetriever:
    return SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        page_size_reduction=page_size_reduction,
        parameters={},
        config={},
    )


def test_given_page_size_reduction_when_read_records_then_retry_same_page_with_reduced_page_size():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        PageSizeReductionRequiredException(),
        [{"id": 1}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.return_value = [{"id": 1}]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = "a token"
    paginator.next_page_token.return_value = None

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction()
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert records == [{"id": 1}]
    assert requester.send_request.call_count == 2
    # the same page is requested again: only the page size changes
    assert [call.kwargs["next_page_token"] for call in requester.send_request.call_args_list] == [
        {"next_page_token": "a token"},
        {"next_page_token": "a token"},
    ]
    assert [call.kwargs["stream_slice"] for call in requester.send_request.call_args_list] == [
        A_STREAM_SLICE,
        A_STREAM_SLICE,
    ]
    assert [
        call.kwargs.get("page_size_override")
        for call in paginator.get_request_params.call_args_list
    ] == [None, 50]


def test_given_retries_at_minimum_page_size_when_at_the_floor_then_re_issue_the_same_page():
    """The page size stays at the floor: what the retry buys is the wait, not a smaller request."""
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        PageSizeReductionRequiredException(),
        [{"id": 1}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.return_value = [{"id": 1}]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 1
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    retriever = _page_size_reduction_retriever(
        requester,
        paginator,
        record_selector,
        PageSizeReduction(retries_at_minimum_page_size=1),
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert records == [{"id": 1}]
    assert requester.send_request.call_count == 2, (
        "the page that could not be reduced was issued twice"
    )
    assert [
        call.kwargs.get("page_size_override")
        for call in paginator.get_request_params.call_args_list
    ] == [None, None]


def test_given_retries_at_minimum_page_size_are_spent_then_raise_transient_error():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = PageSizeReductionRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 1
    paginator.get_initial_token.return_value = None

    retriever = _page_size_reduction_retriever(
        requester,
        paginator,
        record_selector,
        PageSizeReduction(retries_at_minimum_page_size=1),
    )

    with pytest.raises(AirbyteTracedException) as exception:
        list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert exception.value.failure_type == FailureType.transient_error
    assert requester.send_request.call_count == 2


def test_given_page_size_reduction_when_read_records_then_next_page_token_not_computed_for_failed_page():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        PageSizeReductionRequiredException(),
        [{"id": 1}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.return_value = [{"id": 1}]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction()
    )

    list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert paginator.next_page_token.call_count == 1
    assert paginator.next_page_token.call_args.kwargs["page_size_override"] == 50


def test_given_reset_policy_never_when_page_succeeds_then_following_pages_stay_reduced():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        PageSizeReductionRequiredException(),
        [{"id": 1}],
        [{"id": 2}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [[{"id": 1}], [{"id": 2}]]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.side_effect = [{"next_page_token": 2}, None]

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction()
    )

    list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert [
        call.kwargs.get("page_size_override")
        for call in paginator.get_request_params.call_args_list
    ] == [None, 50, 50]


def test_given_reset_policy_after_successful_page_when_page_succeeds_then_page_size_restored():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        PageSizeReductionRequiredException(),
        [{"id": 1}],
        [{"id": 2}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [[{"id": 1}], [{"id": 2}]]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.side_effect = [{"next_page_token": 2}, None]

    retriever = _page_size_reduction_retriever(
        requester,
        paginator,
        record_selector,
        PageSizeReduction(reset_policy=PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE),
    )

    list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    # the reduced page size applies to the retry only, the following page is back to the configured one
    assert [
        call.kwargs.get("page_size_override")
        for call in paginator.get_request_params.call_args_list
    ] == [None, 50, None]


def test_given_reductions_exhausted_when_read_records_then_raise_transient_error():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = PageSizeReductionRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = None

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction(max_attempts=2)
    )

    with pytest.raises(AirbyteTracedException) as exception:
        list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert exception.value.failure_type == FailureType.transient_error
    assert requester.send_request.call_count == 3


def test_given_no_page_size_reduction_when_reduce_page_size_required_then_raise_config_error():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = PageSizeReductionRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    retriever = SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        parameters={},
        config={},
    )

    with pytest.raises(AirbyteTracedException) as exception:
        list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert exception.value.failure_type == FailureType.config_error
    # the neutral "the API asked for a smaller page" message is replaced by the one describing the
    # misconfiguration, which is what this branch actually means
    assert "not set up to send a smaller page" in exception.value.message


def test_given_records_already_emitted_when_reduce_page_size_required_then_raise_instead_of_retrying():
    """
    Re-issuing a page is only safe while none of its records have been emitted. Every in-CDK path raises from
    the fetch, but a custom extractor, filter or transformation can issue its own request from inside the
    record generator, and retrying then would emit those records twice.
    """
    requester = Mock(spec=Requester)
    requester.send_request.return_value = [{"id": 1}]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = None

    def select_records(**kwargs):
        yield Record(data={"id": 1}, stream_name=A_STREAM_NAME)
        raise PageSizeReductionRequiredException()

    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = select_records

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction()
    )

    with pytest.raises(AirbyteTracedException) as exception:
        list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert exception.value.failure_type == FailureType.config_error
    assert "middle of a page" in exception.value.message
    assert requester.send_request.call_count == 1


def test_given_page_size_reduction_and_pagination_limit_reached_when_read_records_then_reduce_before_resetting():
    """
    The reduction retry runs before the pagination limit check, so a page that failed defers the reset by one
    iteration. That is correct - the failed page observed no record, so the limit it reports is stale - but the
    two features never met in a test.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        PageSizeReductionRequiredException(),
        [{"id": 1}],
        [{"id": 2}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [[{"id": 1}], [{"id": 2}]]
    pagination_tracker = Mock(spec=PaginationTracker)
    pagination_tracker.has_reached_limit.side_effect = [True, False]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = 1
    paginator.next_page_token.return_value = None

    retriever = SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        pagination_tracker_factory=lambda: pagination_tracker,
        page_size_reduction=PageSizeReduction(),
        parameters={},
        config={},
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert len(records) == 2
    # the failed page is retried on the same slice with a smaller page, and only the page after it resets
    assert pagination_tracker.has_reached_limit.call_count == 2
    assert pagination_tracker.reduce_slice_range_if_possible.call_count == 1
    assert [
        call.kwargs.get("page_size_override")
        for call in paginator.get_request_params.call_args_list
    ] == [None, 50, 50]
    assert requester.send_request.call_args_list[1].kwargs["stream_slice"] == A_STREAM_SLICE
    assert (
        requester.send_request.call_args_list[2].kwargs["stream_slice"]
        == pagination_tracker.reduce_slice_range_if_possible.return_value
    )


def test_given_partitions_read_concurrently_when_one_reduces_then_others_keep_configured_page_size():
    """
    One retriever instance is shared by every partition of a stream, so the page size in effect must not leak
    from one partition to another.

    The handshake has to make partition b read the page size in effect *after* partition a has reduced, which
    means blocking b's first response until a is reduced and giving b a second page: the page size is read at
    the top of the page loop, before the request is built, so a barrier inside the request building would come
    too late and the assertion would hold even with a single shared reducer.
    """
    slice_a = StreamSlice(cursor_slice={}, partition={"id": "a"})
    slice_b = StreamSlice(cursor_slice={}, partition={"id": "b"})
    partition_a_reduced = threading.Event()
    requested_page_sizes = defaultdict(list)

    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = None

    def get_request_params(*, stream_slice, next_page_token, page_size_override=None):
        requested_page_sizes[stream_slice.partition["id"]].append(page_size_override)
        return {}

    paginator.get_request_params.side_effect = get_request_params

    def next_page_token(*, response, last_page_token_value, **kwargs):
        # only partition b has a second page, which it requests once partition a is known to be reduced
        if response[0]["id"] == "b" and last_page_token_value is None:
            return {"next_page_token": "b page 2"}
        return None

    paginator.next_page_token.side_effect = next_page_token

    def send_request(*args, **kwargs):
        partition = kwargs["stream_slice"].partition["id"]
        if partition == "a":
            if len(requested_page_sizes["a"]) == 1:
                raise PageSizeReductionRequiredException()
            # the retry is in flight with the reduced page size
            partition_a_reduced.set()
        elif len(requested_page_sizes["b"]) == 1:
            # hold partition b's first page open until partition a has reduced, so that b reads the page
            # size in effect strictly after the reduction happened
            assert partition_a_reduced.wait(timeout=10)
        return [{"id": partition}]

    requester = Mock(spec=Requester)
    requester.send_request.side_effect = send_request
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = lambda **kwargs: [{"id": 1}]

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction()
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(lambda s=s: list(retriever.read_records(A_RECORD_SCHEMA, s)))
            for s in (slice_a, slice_b)
        ]
        for future in futures:
            future.result()

    assert requested_page_sizes["a"] == [None, 50]
    assert requested_page_sizes["b"] == [None, None]


def test_given_partitions_read_concurrently_then_each_read_owns_its_page_size_reducer():
    """
    Cheap and fully deterministic counterpart to the test above: two concurrent reads of the same retriever
    must not share the object that holds the page size in effect.
    """
    reducers = []
    original_init = PageSizeReducer.__init__

    def record_reducer(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        reducers.append(self)

    requester = Mock(spec=Requester)
    requester.send_request.return_value = [{"id": 1}]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.return_value = [{"id": 1}]
    paginator = _mock_paginator()
    paginator.get_page_size.return_value = 100
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction()
    )

    with patch.object(PageSizeReducer, "__init__", record_reducer):
        for partition in ("a", "b"):
            list(
                retriever.read_records(
                    A_RECORD_SCHEMA, StreamSlice(cursor_slice={}, partition={"id": partition})
                )
            )

    assert len(reducers) == 2
    assert reducers[0] is not reducers[1]


def test_given_page_size_reduction_when_read_records_then_outgoing_request_carries_reduced_page_size():
    """
    The tests above assert that the retriever hands the reduced page size to the paginator. This one uses a
    real DefaultPaginator so that a break anywhere between the retriever and `inject_into_request` is caught.
    """
    response = requests.Response()
    response.status_code = 200
    response._content = b"{}"
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        PageSizeReductionRequiredException(),
        response,
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.return_value = [{"id": 1}]
    paginator = DefaultPaginator(
        page_size_option=RequestOption(
            field_name="limit", inject_into=RequestOptionType.request_parameter, parameters={}
        ),
        page_token_option=RequestOption(
            field_name="cursor", inject_into=RequestOptionType.request_parameter, parameters={}
        ),
        pagination_strategy=CursorPaginationStrategy(
            page_size=100, cursor_value="{{ None }}", config={}, parameters={}
        ),
        config={},
        url_base="https://airbyte.io",
        parameters={},
    )

    retriever = _page_size_reduction_retriever(
        requester, paginator, record_selector, PageSizeReduction()
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_STREAM_SLICE))

    assert records == [{"id": 1}]
    assert [call.kwargs["request_params"] for call in requester.send_request.call_args_list] == [
        {"limit": 100},
        {"limit": 50},
    ]


def _mock_paginator():
    paginator = Mock(spec=Paginator)
    paginator.get_request_params.__name__ = "get_request_params"
    paginator.get_request_headers.__name__ = "get_request_headers"
    paginator.get_request_body_data.__name__ = "get_request_body_data"
    paginator.get_request_body_json.__name__ = "get_request_body_json"
    return paginator


def _data_feed_retriever(cursor: Optional[Mock], paginator: Paginator) -> SimpleRetriever:
    requester = MagicMock()
    requester.send_request.return_value = MagicMock()
    record_selector = MagicMock()
    return SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        paginator=paginator,
        record_selector=record_selector,
        stream_slicer=SinglePartitionRouter(parameters={}),
        post_pagination_filter=ClientSideIncrementalRecordFilterDecorator(
            config={}, parameters={}, condition=None, cursor=cursor
        )
        if cursor
        else None,
        parameters={},
        config={},
    )


def test_given_data_feed_when_read_records_then_filter_out_already_synced_records():
    page = [
        Record(data={"id": "1"}, stream_name=A_STREAM_NAME),
        Record(data={"id": "2"}, stream_name=A_STREAM_NAME),
        Record(data={"id": "3"}, stream_name=A_STREAM_NAME),
    ]
    cursor = Mock()
    cursor.should_be_synced.side_effect = lambda record: record.data["id"] != "3"
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None
    retriever = _data_feed_retriever(cursor, paginator)

    with patch.object(SimpleRetriever, "_parse_records", return_value=iter(page)):
        actual_records = list(
            retriever.read_records(records_schema={}, stream_slice=A_STREAM_SLICE)
        )

    assert actual_records == page[:2]


def test_given_data_feed_when_read_records_then_paginator_still_sees_the_whole_page():
    """
    The record that stops the pagination is the very one being filtered out, so the paginator must
    be given the page as returned by the API rather than the filtered one.
    """
    page = [
        Record(data={"id": "1"}, stream_name=A_STREAM_NAME),
        Record(data={"id": "2"}, stream_name=A_STREAM_NAME),
        Record(data={"id": "3"}, stream_name=A_STREAM_NAME),
    ]
    cursor = Mock()
    cursor.should_be_synced.side_effect = lambda record: record.data["id"] != "3"
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None
    retriever = _data_feed_retriever(cursor, paginator)

    with patch.object(SimpleRetriever, "_parse_records", return_value=iter(page)):
        list(retriever.read_records(records_schema={}, stream_slice=A_STREAM_SLICE))

    assert paginator.next_page_token.call_args.kwargs["last_page_size"] == 3
    assert paginator.next_page_token.call_args.kwargs["last_record"] == page[-1]


def test_given_no_data_feed_when_read_records_then_emit_every_record():
    page = [
        Record(data={"id": "1"}, stream_name=A_STREAM_NAME),
        Record(data={"id": "2"}, stream_name=A_STREAM_NAME),
    ]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None
    retriever = _data_feed_retriever(cursor=None, paginator=paginator)

    with patch.object(SimpleRetriever, "_parse_records", return_value=iter(page)):
        actual_records = list(
            retriever.read_records(records_schema={}, stream_slice=A_STREAM_SLICE)
        )

    assert actual_records == page


# --- request_window_splitting ---

A_WINDOW_SLICE = StreamSlice(
    cursor_slice={"start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-01T23:59:59Z"},
    partition={},
)
A_FIRST_HALF_SLICE = StreamSlice(
    cursor_slice={"start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-01T11:59:59Z"},
    partition={},
)
A_SECOND_HALF_SLICE = StreamSlice(
    cursor_slice={"start_time": "2024-01-01T12:00:00Z", "end_time": "2024-01-01T23:59:59Z"},
    partition={},
)


def _request_window_splitting_retriever(
    requester: Requester,
    paginator: Paginator,
    record_selector: HttpSelector,
    request_window_splitter: Callable[
        [StreamSlice, Optional[timedelta]], Optional[List[StreamSlice]]
    ],
    request_window_splitting: Optional[RequestWindowSplitting] = None,
) -> SimpleRetriever:
    return SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        request_window_splitting=request_window_splitting or RequestWindowSplitting(),
        request_window_splitter=request_window_splitter,
        parameters={},
        config={},
    )


def test_given_request_window_splitting_when_read_records_then_split_and_read_both_children():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        RequestWindowSplitRequiredException(),
        [{"id": 1}],
        [{"id": 2}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [[{"id": 1}], [{"id": 2}]]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = [
        A_FIRST_HALF_SLICE,
        A_SECOND_HALF_SLICE,
    ]

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert records == [{"id": 1}, {"id": 2}]
    request_window_splitter.assert_called_once_with(A_WINDOW_SLICE, None)
    assert requester.send_request.call_count == 3
    assert [call.kwargs["stream_slice"] for call in requester.send_request.call_args_list] == [
        A_WINDOW_SLICE,
        A_FIRST_HALF_SLICE,
        A_SECOND_HALF_SLICE,
    ]


def test_given_min_split_window_configured_when_read_records_then_pass_it_to_the_splitter():
    """
    `min_split_window` lives on `request_window_splitting`, not on the splitter itself, so the retriever must
    thread it through on every call rather than the splitter reading it from somewhere else.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = RequestWindowSplitRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = None

    retriever = _request_window_splitting_retriever(
        requester,
        paginator,
        record_selector,
        request_window_splitter,
        request_window_splitting=RequestWindowSplitting(min_split_window=timedelta(days=1)),
    )

    with pytest.raises(AirbyteTracedException):
        list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    request_window_splitter.assert_called_once_with(A_WINDOW_SLICE, timedelta(days=1))


def test_given_only_one_child_needs_further_reduction_when_read_records_then_recurse_asymmetrically():
    """
    Nested/asymmetric reduction: the first child is itself rejected and split again, while the second
    child (read after the first child's full recursive read completes) succeeds on the first try.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        RequestWindowSplitRequiredException(),  # top-level window
        RequestWindowSplitRequiredException(),  # first half, rejected again
        [{"id": 1}],  # first quarter of the first half
        [{"id": 2}],  # second quarter of the first half
        [{"id": 3}],  # second half, succeeds directly
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    a_quarter_slice = StreamSlice(
        cursor_slice={"start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-01T05:59:59Z"},
        partition={},
    )
    another_quarter_slice = StreamSlice(
        cursor_slice={"start_time": "2024-01-01T06:00:00Z", "end_time": "2024-01-01T11:59:59Z"},
        partition={},
    )
    request_window_splitter = Mock()
    request_window_splitter.side_effect = [
        [A_FIRST_HALF_SLICE, A_SECOND_HALF_SLICE],
        [a_quarter_slice, another_quarter_slice],
    ]

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert request_window_splitter.call_args_list == [
        ((A_WINDOW_SLICE, None),),
        ((A_FIRST_HALF_SLICE, None),),
    ]
    assert [call.kwargs["stream_slice"] for call in requester.send_request.call_args_list] == [
        A_WINDOW_SLICE,
        A_FIRST_HALF_SLICE,
        a_quarter_slice,
        another_quarter_slice,
        A_SECOND_HALF_SLICE,
    ]


def test_given_no_request_window_splitting_configured_when_reduction_required_then_raise_not_supported():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = RequestWindowSplitRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    retriever = SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        parameters={},
        config={},
    )

    with pytest.raises(RequestWindowSplitNotSupportedException):
        list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))


def test_given_no_request_window_splitter_when_reduction_required_then_raise_not_supported():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = RequestWindowSplitRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    retriever = SimpleRetriever(
        name=A_STREAM_NAME,
        primary_key=primary_key,
        requester=requester,
        record_selector=record_selector,
        paginator=paginator,
        request_window_splitting=RequestWindowSplitting(),
        request_window_splitter=None,
        parameters={},
        config={},
    )

    with pytest.raises(RequestWindowSplitNotSupportedException):
        list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))


def test_given_records_already_emitted_when_reduction_requested_then_split_and_replay_anyway(
    caplog,
):
    """
    A later page of the same window fails after an earlier page already emitted a record: since a failed
    partition is never checkpointed, refusing to split here wouldn't avoid the duplicate anyway - the next
    attempt would just re-read the same window and re-emit it. So this always splits and re-reads, logging a
    warning with how many records were already emitted.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        Mock(),
        RequestWindowSplitRequiredException(),
        [{"id": 2}],
        [{"id": 3}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.side_effect = [{"next_page_token": "page2"}, None, None]

    request_window_splitter = Mock()
    request_window_splitter.return_value = [
        A_FIRST_HALF_SLICE,
        A_SECOND_HALF_SLICE,
    ]

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        records = list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    # the record from the page emitted before the split signal is included once more: splitting and
    # re-reading accepts this at-least-once duplication rather than silently dropping it or failing the sync.
    assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert "already emitted 1 record" in caplog.text
    request_window_splitter.assert_called_once_with(A_WINDOW_SLICE, None)


def test_given_request_window_splitter_returns_none_when_reduction_requested_then_raise_transient_error():
    """
    `request_window_splitter` returning `None` means the window cannot be split any further - either the
    granularity floor or `min_split_window` was reached, or splitting would not make progress. This is never
    the user's fault - the API kept rejecting every window size tried - so it is always `transient_error`,
    matching `PageSizeReducer`'s equivalent exhaustion branch, regardless of how the triggering response was
    classified (here, deliberately the opposite - `config_error` - to prove it is overridden).
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = RequestWindowSplitRequiredException(
        failure_type=FailureType.config_error
    )
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = None

    retriever = _request_window_splitting_retriever(
        requester,
        paginator,
        record_selector,
        request_window_splitter,
        request_window_splitting=RequestWindowSplitting(
            failure_message="Lower time_window so that each request covers less data."
        ),
    )

    with pytest.raises(AirbyteTracedException) as exception:
        list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert exception.value.failure_type == FailureType.transient_error
    assert "Lower time_window so that each request covers less data." in exception.value.message
    request_window_splitter.assert_called_once_with(A_WINDOW_SLICE, None)


def test_given_request_window_splitter_returns_none_and_min_split_window_configured_then_name_it_in_the_message():
    """
    When `min_split_window` is configured, it may be the actual reason splitting stopped rather than the
    cursor's own granularity floor - the terminal message should name it so the user knows which knob to check.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = RequestWindowSplitRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = None

    retriever = _request_window_splitting_retriever(
        requester,
        paginator,
        record_selector,
        request_window_splitter,
        request_window_splitting=RequestWindowSplitting(min_split_window=timedelta(days=1)),
    )

    with pytest.raises(AirbyteTracedException) as exception:
        list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert "min_split_window" in exception.value.message
    assert str(timedelta(days=1)) in exception.value.message


def test_given_empty_children_list_when_reduction_requested_then_emit_no_records_and_do_not_loop():
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = RequestWindowSplitRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = []

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert records == []
    request_window_splitter.assert_called_once_with(A_WINDOW_SLICE, None)


def test_given_a_later_child_fails_when_read_records_then_the_failure_propagates():
    """
    The first child reads fully and its records are emitted before the second child fails; the failure of a
    later child must still surface as a real error, not be silently swallowed after the first child's success.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        RequestWindowSplitRequiredException(),
        [{"id": 1}],
        RequestWindowSplitRequiredException(),
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.return_value = [{"id": 1}]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.side_effect = [
        [A_FIRST_HALF_SLICE, A_SECOND_HALF_SLICE],
        None,
    ]

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    emitted = []
    with pytest.raises(AirbyteTracedException):
        for record in retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE):
            emitted.append(record)

    # the first child's records were already emitted to the caller before the second child failed
    assert emitted == [{"id": 1}]
    assert request_window_splitter.call_args_list == [
        ((A_WINDOW_SLICE, None),),
        ((A_SECOND_HALF_SLICE, None),),
    ]


def test_given_request_window_splitting_when_read_records_then_each_child_gets_fresh_pagination_state():
    """
    Each recursive call re-enters read_records/_read_pages from scratch for its child slice, so the
    paginator's initial token is requested again for every child rather than resuming from wherever the
    failed parent window left off.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        RequestWindowSplitRequiredException(),
        [{"id": 1}],
        [{"id": 2}],
    ]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [[{"id": 1}], [{"id": 2}]]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = "initial token"
    paginator.next_page_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = [
        A_FIRST_HALF_SLICE,
        A_SECOND_HALF_SLICE,
    ]

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert [call.kwargs["next_page_token"] for call in requester.send_request.call_args_list] == [
        {"next_page_token": "initial token"},
        {"next_page_token": "initial token"},
        {"next_page_token": "initial token"},
    ]


def test_reassociate_with_original_slice_rewraps_a_record_stamped_with_a_different_slice():
    child_slice = StreamSlice(cursor_slice={"start_time": "child"}, partition={})
    record = Record(
        data={"id": 1},
        stream_name=A_STREAM_NAME,
        associated_slice=child_slice,
    )

    result = SimpleRetriever._reassociate_with_original_slice(record, A_WINDOW_SLICE)

    assert result.associated_slice is A_WINDOW_SLICE
    assert result.data == {"id": 1}
    assert result.stream_name == A_STREAM_NAME


def test_reassociate_with_original_slice_is_a_no_op_when_already_associated():
    record = Record(
        data={"id": 1},
        stream_name=A_STREAM_NAME,
        associated_slice=A_WINDOW_SLICE,
    )

    result = SimpleRetriever._reassociate_with_original_slice(record, A_WINDOW_SLICE)

    assert result is record


def test_reassociate_with_original_slice_passes_through_non_record_stream_data():
    message = {"raw": "not a Record instance"}

    result = SimpleRetriever._reassociate_with_original_slice(message, A_WINDOW_SLICE)

    assert result is message


def test_given_request_window_splitting_when_split_then_records_are_associated_with_original_slice():
    """
    Regression test: `ConcurrentCursor.observe()` keys its per-partition bookkeeping by `Record.associated_slice`,
    but `ConcurrentCursor.close_partition()` always looks that bookkeeping up by the partition's own (original)
    slice. A record read from a reduced child window - via a real `RecordSelector`, not the mocks the other
    tests in this module use - would otherwise carry the child as its `associated_slice`, a key
    `close_partition()` can never find, silently degrading the most-recently-observed cursor value tracked for a
    split partition.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [
        RequestWindowSplitRequiredException(),
        Mock(),
        Mock(),
    ]

    def select_records(*, response, stream_slice, **kwargs):
        return [
            Record(
                data={"start": stream_slice.cursor_slice["start_time"]},
                stream_name=A_STREAM_NAME,
                associated_slice=stream_slice,
            )
        ]

    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = select_records
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = [
        A_FIRST_HALF_SLICE,
        A_SECOND_HALF_SLICE,
    ]

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert len(records) == 2
    assert all(record.associated_slice is A_WINDOW_SLICE for record in records)
    # the underlying data still reflects which child the record actually came from
    assert {record.data["start"] for record in records} == {
        A_FIRST_HALF_SLICE.cursor_slice["start_time"],
        A_SECOND_HALF_SLICE.cursor_slice["start_time"],
    }


def test_given_exception_raised_during_record_extraction_when_read_then_still_split_request_window():
    """
    Google Ads/Iterable-style exception-driven cases fail during response streaming/decoding, not at the point
    the request is sent - unlike PayPal, where the API rejects the request outright before any body is read.
    The catch boundary must cover the entire read, not only the requester call, so custom code raising
    `RequestWindowSplitRequiredException` from anywhere in the pipeline is still honored.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = [Mock(), Mock(), Mock()]
    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = [
        RequestWindowSplitRequiredException(),
        [{"id": 1}],
        [{"id": 2}],
    ]
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    request_window_splitter = Mock()
    request_window_splitter.return_value = [
        A_FIRST_HALF_SLICE,
        A_SECOND_HALF_SLICE,
    ]

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    records = list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    assert records == [{"id": 1}, {"id": 2}]


def test_given_max_split_depth_exceeded_when_read_records_then_raise_terminal_error(caplog):
    """
    Defense-in-depth: even if a (misbehaving, e.g. custom) `request_window_splitter` never itself signals "cannot split
    any further", the retriever's own depth counter must still terminate the read deterministically rather than
    recursing without bound.
    """
    requester = Mock(spec=Requester)
    requester.send_request.side_effect = RequestWindowSplitRequiredException()
    record_selector = Mock(spec=HttpSelector)
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None

    request_window_splitter = Mock()
    # never signals "no further progress possible" - always splits into two children identical to the parent
    request_window_splitter.side_effect = lambda stream_slice, min_split_window=None: [
        stream_slice,
        stream_slice,
    ]

    retriever = _request_window_splitting_retriever(
        requester,
        paginator,
        record_selector,
        request_window_splitter,
        request_window_splitting=RequestWindowSplitting(
            failure_message="Lower time_window so that each request covers less data.",
        ),
    )

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        with pytest.raises(AirbyteTracedException) as exception:
            list(retriever.read_records(A_RECORD_SCHEMA, A_WINDOW_SLICE))

    # exhausting the split depth is not the user's fault - the API kept rejecting every window size tried -
    # so it is transient_error, matching PageSizeReducer's equivalent exhaustion branch, not config_error
    assert exception.value.failure_type == FailureType.transient_error
    assert "maximum request window split depth" in exception.value.internal_message
    assert f"within {_MAX_REQUEST_WINDOW_SPLIT_DEPTH} splits" in exception.value.message
    assert "Lower time_window so that each request covers less data." in exception.value.message
    # the safety net is expected to be rare, so it's logged separately from the exception (per review feedback
    # to make it visible for later review even if the trace message's internal_message isn't surfaced)
    assert (
        f"hit the maximum request window split depth ({_MAX_REQUEST_WINDOW_SPLIT_DEPTH})"
        in caplog.text
    )
    # the single leftmost recursion path is explored to the full depth (one split_request_window call per
    # depth, from 0 up to the cap) before the cap stops the next call - the exception then propagates
    # immediately, so sibling branches at shallower depths are never explored
    assert request_window_splitter.call_count == _MAX_REQUEST_WINDOW_SPLIT_DEPTH


def test_given_partitions_read_concurrently_then_window_reduction_isolates_between_partitions():
    """
    One retriever instance - and one shared `request_window_splitter` (the stream's cursor) - is used for every partition
    of a stream, so a reduction triggered while reading one partition must not affect a concurrently-running
    read of another partition at all: the other partition must complete normally, and the reducer must only ever
    be asked to split the slice that actually needed it.
    """
    slice_a = StreamSlice(
        cursor_slice={"start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-01T23:59:59Z"},
        partition={"id": "a"},
    )
    slice_b = StreamSlice(
        cursor_slice={"start_time": "2024-02-01T00:00:00Z", "end_time": "2024-02-01T23:59:59Z"},
        partition={"id": "b"},
    )
    b_requested = threading.Event()

    def send_request(*, stream_slice, **kwargs):
        # only the original top-level slice_a is rejected (its children must succeed, or the recursion would
        # never terminate); identity, not partition id, distinguishes it from the children split_request_window builds
        if stream_slice is slice_a:
            # only raise once b's own, unrelated request is known to be in flight, so the two reads
            # genuinely overlap rather than running one after the other
            assert b_requested.wait(timeout=10)
            raise RequestWindowSplitRequiredException()
        if stream_slice is slice_b:
            b_requested.set()
        return Mock()

    requester = Mock(spec=Requester)
    requester.send_request.side_effect = send_request

    def select_records(*, response, stream_slice, **kwargs):
        return [
            {
                "partition": stream_slice.partition["id"],
                "start": stream_slice.cursor_slice["start_time"],
            }
        ]

    record_selector = Mock(spec=HttpSelector)
    record_selector.select_records.side_effect = select_records
    paginator = _mock_paginator()
    paginator.get_initial_token.return_value = None
    paginator.next_page_token.return_value = None

    def split_request_window(stream_slice, min_split_window=None):
        # preserve the parent's own partition, matching what a real request_window_splitter would do
        return [
            StreamSlice(
                cursor_slice={
                    "start_time": stream_slice.cursor_slice["start_time"],
                    "end_time": "midpoint",
                },
                partition=stream_slice.partition,
            ),
            StreamSlice(
                cursor_slice={
                    "start_time": "midpoint",
                    "end_time": stream_slice.cursor_slice["end_time"],
                },
                partition=stream_slice.partition,
            ),
        ]

    request_window_splitter = Mock()
    request_window_splitter.side_effect = split_request_window

    retriever = _request_window_splitting_retriever(
        requester, paginator, record_selector, request_window_splitter
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(lambda: list(retriever.read_records(A_RECORD_SCHEMA, slice_a)))
        future_b = executor.submit(lambda: list(retriever.read_records(A_RECORD_SCHEMA, slice_b)))
        records_a = future_a.result(timeout=10)
        records_b = future_b.result(timeout=10)

    assert records_b == [{"partition": "b", "start": "2024-02-01T00:00:00Z"}]
    assert len(records_a) == 2
    request_window_splitter.assert_called_once_with(slice_a, None)
