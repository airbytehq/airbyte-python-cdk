#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from urllib.parse import urljoin

import requests
from requests.auth import AuthBase
from typing_extensions import deprecated

from airbyte_cdk.models import AirbyteMessage, FailureType, SyncMode
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.message.repository import InMemoryMessageRepository
from airbyte_cdk.sources.streams.call_rate import APIBudget
from airbyte_cdk.sources.streams.checkpoint.cursor import Cursor
from airbyte_cdk.sources.streams.checkpoint.resumable_full_refresh_cursor import (
    ResumableFullRefreshCursor,
)
from airbyte_cdk.sources.streams.checkpoint.substream_resumable_full_refresh_cursor import (
    SubstreamResumableFullRefreshCursor,
)
from airbyte_cdk.sources.streams.core import CheckpointMixin, Stream, StreamData
from airbyte_cdk.sources.streams.http.error_handlers import (
    BackoffStrategy,
    ErrorHandler,
    HttpStatusErrorHandler,
)
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    ErrorResolution,
    ResponseAction,
)
from airbyte_cdk.sources.streams.http.http_client import HttpClient
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.sources.utils.types import JsonType

# list of all possible HTTP methods which can be used for sending of request bodies
BODY_REQUEST_METHODS = ("GET", "POST", "PUT", "PATCH")


class HttpStream(Stream, CheckpointMixin, ABC):
    """Base class for implementing HTTP-based Airbyte streams.

    Provides core functionality for building source connectors that interact with
    HTTP APIs. Handles authentication, pagination, error handling, and other
    common HTTP integration patterns.

    To implement a new HTTP stream:
    1. Implement abstract methods (url_base, path)
    2. Override optional methods as needed (auth, params, headers, etc.)
    3. Set appropriate configuration properties (use_cache, page_size, etc.)
    """

    source_defined_cursor = True  # Most HTTP streams use a source defined cursor (i.e: the user can't configure it like on a SQL table)
    page_size: Optional[int] = (
        None  # Use this variable to define page size for API http requests with pagination support
    )

    def __init__(
        self, authenticator: Optional[AuthBase] = None, api_budget: Optional[APIBudget] = None
    ):
        self._exit_on_rate_limit: bool = False
        self._http_client = HttpClient(
            name=self.name,
            logger=self.logger,
            error_handler=self.get_error_handler(),
            api_budget=api_budget or APIBudget(policies=[]),
            authenticator=authenticator,
            use_cache=self.use_cache,
            backoff_strategy=self.get_backoff_strategy(),
            message_repository=InMemoryMessageRepository(),
        )

        # There are three conditions that dictate if RFR should automatically be applied to a stream
        # 1. Streams that explicitly initialize their own cursor should defer to it and not automatically apply RFR
        # 2. Streams with at least one cursor_field are incremental and thus a superior sync to RFR.
        # 3. Streams overriding read_records() do not guarantee that they will call the parent implementation which can perform
        #    per-page checkpointing so RFR is only supported if a stream use the default `HttpStream.read_records()` method
        if (
            not self.cursor
            and len(self.cursor_field) == 0
            and type(self).read_records is HttpStream.read_records
        ):
            self.cursor = ResumableFullRefreshCursor()

    @property
    def exit_on_rate_limit(self) -> bool:
        """Control behavior when rate limited by the API.

        Returns:
            True to exit when rate limited, False to retry endlessly.
        """
        return self._exit_on_rate_limit

    @exit_on_rate_limit.setter
    def exit_on_rate_limit(self, value: bool) -> None:
        self._exit_on_rate_limit = value

    @property
    def cache_filename(self) -> str:
        """Get the filename for caching HTTP responses.

        Override to customize cache filename. If REQUEST_CACHE_PATH environment
        variable is not set, cache will be in-memory only.

        Returns:
            Name of the cache file.
        """
        return f"{self.name}.sqlite"

    @property
    def use_cache(self) -> bool:
        """Control HTTP response caching.

        Override to enable caching. If True, all HTTP responses will be cached.
        If REQUEST_CACHE_PATH environment variable is not set, cache will be
        in-memory only.

        Returns:
            True to enable caching, False to disable.
        """
        return False

    @property
    @abstractmethod
    def url_base(self) -> str:
        """Get the base URL for the API endpoint.

        For example, if the URL is https://myapi.com/v1/some_entity,
        this should return "https://myapi.com/v1/"

        Returns:
            The base URL for the API endpoint.
        """

    @property
    def http_method(self) -> str:
        """Get the HTTP method for requests.

        Override if needed. See request_body_data/request_body_json
        for configuring POST/PUT/PATCH request bodies.

        Returns:
            The HTTP method to use (default: "GET").
        """
        return "GET"

    @property
    @deprecated(
        "Deprecated as of CDK version 3.0.0. "
        "You should set error_handler explicitly in HttpStream.get_error_handler() instead."
    )
    def raise_on_http_errors(self) -> bool:
        """
        Override if needed. If set to False, allows opting-out of raising HTTP code exception.
        """
        return True

    @property
    @deprecated(
        "Deprecated as of CDK version 3.0.0. "
        "You should set backoff_strategies explicitly in HttpStream.get_backoff_strategy() instead."
    )
    def max_retries(self) -> Union[int, None]:
        """
        Override if needed. Specifies maximum amount of retries for backoff policy. Return None for no limit.
        """
        return 5

    @property
    @deprecated(
        "Deprecated as of CDK version 3.0.0. "
        "You should set backoff_strategies explicitly in HttpStream.get_backoff_strategy() instead."
    )
    def max_time(self) -> Union[int, None]:
        """
        Override if needed. Specifies maximum total waiting time (in seconds) for backoff policy. Return None for no limit.
        """
        return 60 * 10

    @property
    @deprecated(
        "Deprecated as of CDK version 3.0.0. "
        "You should set backoff_strategies explicitly in HttpStream.get_backoff_strategy() instead."
    )
    def retry_factor(self) -> float:
        """
        Override if needed. Specifies factor for backoff policy.
        """
        return 5

    @abstractmethod
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Define the pagination strategy for the stream.

        Args:
            response: The HTTP response from the previous request.

        Returns:
            A token for the next page, or None if there are no more pages.
        """

    @abstractmethod
    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        """Get the URL path for the API endpoint.

        For example, if the URL is https://myapi.com/v1/some_entity,
        this should return "some_entity"

        Args:
            stream_state: The current state of the stream.
            stream_slice: The current stream slice being read.
            next_page_token: Token for retrieving the next page.

        Returns:
            The URL path for the API endpoint.
        """

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """Define query parameters for outgoing HTTP requests.

        Override to set query parameters based on stream state, slice, or pagination token.
        Common use case is setting pagination parameters when next_page_token is present.

        Args:
            stream_state: The current state of the stream.
            stream_slice: The current stream slice being read.
            next_page_token: Token for retrieving the next page.

        Returns:
            A mapping of query parameter names to values.
        """
        return {}

    def request_headers(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """Define headers for outgoing HTTP requests.

        Override to set custom headers. Note that authentication headers will
        overwrite any overlapping headers returned by this method.

        Args:
            stream_state: The current state of the stream.
            stream_slice: The current stream slice being read.
            next_page_token: Token for retrieving the next page.

        Returns:
            A mapping of header names to values.
        """
        return {}

    def request_body_data(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Union[Mapping[str, Any], str]]:
        """Define the non-JSON data body for HTTP requests.

        Override for POST/PUT/PATCH requests requiring non-JSON data payloads.
        Returns string for raw data or dict for urlencoded form data.
        Example: {"key1": "value1", "key2": "value2"} becomes "key1=value1&key2=value2"

        Note: Only one of request_body_data or request_body_json should be overridden.

        Args:
            stream_state: The current state of the stream.
            stream_slice: The current stream slice being read.
            next_page_token: Token for retrieving the next page.

        Returns:
            Data to be sent in the request body, or None if no body is required.
        """
        return None

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        """Define the JSON body for HTTP requests.

        Override for POST/PUT/PATCH requests requiring JSON payloads.
        Note: Only one of request_body_data or request_body_json should be overridden.

        Args:
            stream_state: The current state of the stream.
            stream_slice: The current stream slice being read.
            next_page_token: Token for retrieving the next page.

        Returns:
            A mapping to be sent as a JSON body, or None if no body is required.
        """
        return None

    def request_kwargs(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """Define additional keyword arguments for HTTP requests.

        Override to add custom keyword arguments when creating HTTP requests.
        See https://docs.python-requests.org/en/latest/api/#requests.adapters.BaseAdapter.send
        for available options. These options are separate from headers, params, etc.

        Args:
            stream_state: The current state of the stream.
            stream_slice: The current stream slice being read.
            next_page_token: Token for retrieving the next page.

        Returns:
            A mapping of keyword arguments for the HTTP request.
        """
        return {}

    @abstractmethod
    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Parse the response from the API into a list of records.

        By default, returns an iterable containing the response.
        Override to implement a custom parsing logic.

        Args:
            response: The HTTP response from the API.
            stream_state: The current state of the stream.
            stream_slice: The current stream slice being read.
            next_page_token: Token for retrieving the next page.

        Returns:
            An iterable containing the parsed response records.
        """

    def get_backoff_strategy(self) -> Optional[Union[BackoffStrategy, List[BackoffStrategy]]]:
        """Get the backoff strategy for handling rate limits and retries.

        Used to initialize the HTTP client adapter. If the stream has a `backoff_time`
        method, it uses the legacy backoff handler through an adapter.

        Override to provide a custom BackoffStrategy implementation.

        Returns:
            A BackoffStrategy instance, list of strategies, or None if not needed.
        """
        if hasattr(self, "backoff_time"):
            return HttpStreamAdapterBackoffStrategy(self)
        else:
            return None

    def get_error_handler(self) -> Optional[ErrorHandler]:
        """Get the error handler for HTTP request failures.

        Used to initialize the HTTP client adapter. If the stream has a `should_retry`
        method, it uses the legacy error handler through an adapter.

        Override to provide a custom ErrorHandler implementation.

        Returns:
            An ErrorHandler instance or None if not needed.
        """
        if hasattr(self, "should_retry"):
            error_handler = HttpStreamAdapterHttpStatusErrorHandler(
                stream=self,
                logger=logging.getLogger(),
                max_retries=self.max_retries,
                max_time=timedelta(seconds=self.max_time or 0),
            )
            return error_handler
        else:
            return None

    @classmethod
    def _join_url(cls, url_base: str, path: str) -> str:
        return urljoin(url_base, path)

    @classmethod
    def parse_response_error_message(cls, response: requests.Response) -> Optional[str]:
        """Parse a failed response into a user-friendly error message.

        By default, attempts to extract error messages from JSON responses following common API patterns.
        Override to implement custom error parsing.

        Args:
            response: The failed HTTP response to parse.

        Returns:
            A user-friendly message describing the error, or None if no message could be extracted.
        """

        # default logic to grab error from common fields
        def _try_get_error(value: Optional[JsonType]) -> Optional[str]:
            if isinstance(value, str):
                return value
            elif isinstance(value, list):
                errors_in_value = [_try_get_error(v) for v in value]
                return ", ".join(v for v in errors_in_value if v is not None)
            elif isinstance(value, dict):
                new_value = (
                    value.get("message")
                    or value.get("messages")
                    or value.get("error")
                    or value.get("errors")
                    or value.get("failures")
                    or value.get("failure")
                    or value.get("detail")
                )
                return _try_get_error(new_value)
            return None

        try:
            body = response.json()
            return _try_get_error(body)
        except requests.exceptions.JSONDecodeError:
            return None

    def get_error_display_message(self, exception: BaseException) -> Optional[str]:
        """Get a user-friendly message for an exception encountered during record reading.

        Used to build the AirbyteTraceMessage when errors occur. By default, only handles HTTPErrors
        by parsing the response. Override to handle additional exception types.

        Args:
            exception: The exception that was raised.

        Returns:
            A user-friendly message describing the error, or None if the error type is not handled.
        """
        if isinstance(exception, requests.HTTPError) and exception.response is not None:
            return self.parse_response_error_message(exception.response)
        return None

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        # A cursor_field indicates this is an incremental stream which offers better checkpointing than RFR enabled via the cursor
        if self.cursor_field or not isinstance(self.get_cursor(), ResumableFullRefreshCursor):
            yield from self._read_pages(
                lambda req, res, state, _slice: self.parse_response(
                    res, stream_slice=_slice, stream_state=state
                ),
                stream_slice,
                stream_state,
            )
        else:
            yield from self._read_single_page(
                lambda req, res, state, _slice: self.parse_response(
                    res, stream_slice=_slice, stream_state=state
                ),
                stream_slice,
                stream_state,
            )

    @property
    def state(self) -> MutableMapping[str, Any]:
        cursor = self.get_cursor()
        if cursor:
            return cursor.get_stream_state()  # type: ignore
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        cursor = self.get_cursor()
        if cursor:
            cursor.set_initial_state(value)
        self._state = value

    def get_cursor(self) -> Optional[Cursor]:
        # I don't love that this is semi-stateful but not sure what else to do. We don't know exactly what type of cursor to
        # instantiate when creating the class. We can make a few assumptions like if there is a cursor_field which implies
        # incremental, but we don't know until runtime if this is a substream. Ideally, a stream should explicitly define
        # its cursor, but because we're trying to automatically apply RFR we're stuck with this logic where we replace the
        # cursor at runtime once we detect this is a substream based on self.has_multiple_slices being reassigned
        if self.has_multiple_slices and isinstance(self.cursor, ResumableFullRefreshCursor):
            self.cursor = SubstreamResumableFullRefreshCursor()
            return self.cursor
        else:
            return self.cursor

    def _read_pages(
        self,
        records_generator_fn: Callable[
            [
                requests.PreparedRequest,
                requests.Response,
                Mapping[str, Any],
                Optional[Mapping[str, Any]],
            ],
            Iterable[StreamData],
        ],
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        partition, _, _ = self._extract_slice_fields(stream_slice=stream_slice)

        stream_state = stream_state or {}
        pagination_complete = False
        next_page_token = None
        while not pagination_complete:
            request, response = self._fetch_next_page(stream_slice, stream_state, next_page_token)
            yield from records_generator_fn(request, response, stream_state, stream_slice)

            next_page_token = self.next_page_token(response)
            if not next_page_token:
                pagination_complete = True

        cursor = self.get_cursor()
        if cursor and isinstance(cursor, SubstreamResumableFullRefreshCursor):
            # Substreams checkpoint state by marking an entire parent partition as completed so that on the subsequent attempt
            # after a failure, completed parents are skipped and the sync can make progress
            cursor.close_slice(StreamSlice(cursor_slice={}, partition=partition))

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def _read_single_page(
        self,
        records_generator_fn: Callable[
            [
                requests.PreparedRequest,
                requests.Response,
                Mapping[str, Any],
                Optional[Mapping[str, Any]],
            ],
            Iterable[StreamData],
        ],
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        partition, cursor_slice, remaining_slice = self._extract_slice_fields(
            stream_slice=stream_slice
        )
        stream_state = stream_state or {}
        next_page_token = cursor_slice or None

        request, response = self._fetch_next_page(remaining_slice, stream_state, next_page_token)
        yield from records_generator_fn(request, response, stream_state, remaining_slice)

        next_page_token = self.next_page_token(response) or {
            "__ab_full_refresh_sync_complete": True
        }

        cursor = self.get_cursor()
        if cursor:
            cursor.close_slice(StreamSlice(cursor_slice=next_page_token, partition=partition))

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    @staticmethod
    def _extract_slice_fields(
        stream_slice: Optional[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]:
        """Extract partition, cursor slice, and remaining fields from a stream slice.

        Handles both StreamSlice objects and legacy mapping formats.

        Args:
            stream_slice: The stream slice to extract fields from.

        Returns:
            A tuple of (partition, cursor_slice, remaining) mappings.
            partition: Contains partition information
            cursor_slice: Contains cursor-specific slice data
            remaining: Contains any remaining fields
        """
        if not stream_slice:
            return {}, {}, {}

        if isinstance(stream_slice, StreamSlice):
            partition = stream_slice.partition
            cursor_slice = stream_slice.cursor_slice
            remaining = {k: v for k, v in stream_slice.items()}
        else:
            # RFR streams that implement stream_slices() to generate stream slices in the legacy mapping format are converted into a
            # structured stream slice mapping by the LegacyCursorBasedCheckpointReader. The structured mapping object has separate
            # fields for the partition and cursor_slice value
            partition = stream_slice.get("partition", {})
            cursor_slice = stream_slice.get("cursor_slice", {})
            remaining = {
                key: val
                for key, val in stream_slice.items()
                if key != "partition" and key != "cursor_slice"
            }
        return partition, cursor_slice, remaining

    def _fetch_next_page(
        self,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[requests.PreparedRequest, requests.Response]:
        request, response = self._http_client.send_request(
            http_method=self.http_method,
            url=self._join_url(
                self.url_base,
                self.path(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                ),
            ),
            request_kwargs=self.request_kwargs(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            headers=self.request_headers(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            params=self.request_params(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            json=self.request_body_json(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            data=self.request_body_data(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            dedupe_query_params=True,
            log_formatter=self.get_log_formatter(),
            exit_on_rate_limit=self.exit_on_rate_limit,
        )

        return request, response

    def get_log_formatter(self) -> Optional[Callable[[requests.Response], Any]]:
        """Get a custom formatter for logging HTTP responses.

        Override to provide custom formatting for HTTP response logging.

        Returns:
            A callable that formats requests.Response objects for logging,
            or None to use default formatting.
        """
        return None


class HttpSubStream(HttpStream, ABC):
    def __init__(self, parent: HttpStream, **kwargs: Any):
        """Initialize a substream with its parent stream.

        Args:
            parent: The parent HttpStream instance this substream depends on.
            **kwargs: Additional arguments passed to HttpStream.__init__
        """
        super().__init__(**kwargs)
        self.parent = parent
        self.has_multiple_slices = (
            True  # Substreams are based on parent records which implies there are multiple slices
        )

        # There are three conditions that dictate if RFR should automatically be applied to a stream
        # 1. Streams that explicitly initialize their own cursor should defer to it and not automatically apply RFR
        # 2. Streams with at least one cursor_field are incremental and thus a superior sync to RFR.
        # 3. Streams overriding read_records() do not guarantee that they will call the parent implementation which can perform
        #    per-page checkpointing so RFR is only supported if a stream use the default `HttpStream.read_records()` method
        if (
            not self.cursor
            and len(self.cursor_field) == 0
            and type(self).read_records is HttpStream.read_records
        ):
            self.cursor = SubstreamResumableFullRefreshCursor()

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        # read_stateless() assumes the parent is not concurrent. This is currently okay since the concurrent CDK does
        # not support either substreams or RFR, but something that needs to be considered once we do
        for parent_record in self.parent.read_only_records(stream_state):
            # Skip non-records (eg AirbyteLogMessage)
            if isinstance(parent_record, AirbyteMessage):
                if parent_record.type == MessageType.RECORD:
                    parent_record = parent_record.record.data  # type: ignore [assignment, union-attr]  # Incorrect type for assignment
                else:
                    continue
            elif isinstance(parent_record, Record):
                parent_record = parent_record.data
            yield {"parent": parent_record}


@deprecated(
    "Deprecated as of CDK version 3.0.0."
    "You should set backoff_strategies explicitly in HttpStream.get_backoff_strategy() instead."
)
class HttpStreamAdapterBackoffStrategy(BackoffStrategy):
    def __init__(self, stream: HttpStream):
        self.stream = stream

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        """Get backoff time from the underlying stream.

        Args:
            response_or_exception: The response or exception from the failed request.
            attempt_count: Number of retry attempts made so far.

        Returns:
            Time to wait before next retry, or None if no retry needed.
        """
        return self.stream.backoff_time(response_or_exception)  # type: ignore # noqa  # HttpStream.backoff_time has been deprecated


@deprecated(
    "Deprecated as of CDK version 3.0.0. "
    "You should set error_handler explicitly in HttpStream.get_error_handler() instead."
)
class HttpStreamAdapterHttpStatusErrorHandler(HttpStatusErrorHandler):
    def __init__(self, stream: HttpStream, **kwargs):  # type: ignore # noqa
        self.stream = stream
        super().__init__(**kwargs)

    def interpret_response(
        self, response_or_exception: Optional[Union[requests.Response, Exception]] = None
    ) -> ErrorResolution:
        """Interpret HTTP response or exception to determine error handling action.

        Args:
            response_or_exception: The HTTP response or exception to interpret.

        Returns:
            ErrorResolution indicating how to handle the error.
        """
        if isinstance(response_or_exception, Exception):
            return super().interpret_response(response_or_exception)
        elif isinstance(response_or_exception, requests.Response):
            should_retry = self.stream.should_retry(response_or_exception)  # type: ignore # noqa
            if should_retry:
                if response_or_exception.status_code == 429:
                    return ErrorResolution(
                        response_action=ResponseAction.RATE_LIMITED,
                        failure_type=FailureType.transient_error,
                        error_message=f"Response status code: {response_or_exception.status_code}. Retrying...",
                    )
                return ErrorResolution(
                    response_action=ResponseAction.RETRY,
                    failure_type=FailureType.transient_error,
                    error_message=f"Response status code: {response_or_exception.status_code}. Retrying...",
                )
            else:
                if response_or_exception.ok:
                    return ErrorResolution(
                        response_action=ResponseAction.SUCCESS,
                        failure_type=None,
                        error_message=None,
                    )
                if self.stream.raise_on_http_errors:
                    return ErrorResolution(
                        response_action=ResponseAction.FAIL,
                        failure_type=FailureType.transient_error,
                        error_message=f"Response status code: {response_or_exception.status_code}. Unexpected error. Failed.",
                    )
                else:
                    return ErrorResolution(
                        response_action=ResponseAction.IGNORE,
                        failure_type=FailureType.transient_error,
                        error_message=f"Response status code: {response_or_exception.status_code}. Ignoring...",
                    )
        else:
            self._logger.error(f"Received unexpected response type: {type(response_or_exception)}")
            return ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.system_error,
                error_message=f"Received unexpected response type: {type(response_or_exception)}",
            )
