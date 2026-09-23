#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Dict, Mapping, Optional

import requests

from airbyte_cdk.sources.declarative.requesters.request_options.request_options_provider import (
    RequestOptionsProvider,
)
from airbyte_cdk.sources.types import Record, StreamSlice


def page_size_override_kwargs(page_size_override: Optional[int]) -> Dict[str, Any]:
    """
    Build the `page_size_override` keyword argument only when there is an override to pass.

    Paginators and pagination strategies defined outside of the CDK may not accept the argument, and they only
    need to when the stream actually reduces its page size (see `ResponseAction.REDUCE_PAGE_SIZE`).
    """
    return {"page_size_override": page_size_override} if page_size_override is not None else {}


def stream_slice_kwargs(
    next_page_token: Callable[..., Any], stream_slice: Optional[StreamSlice]
) -> Dict[str, Any]:
    """
    Build the `stream_slice` keyword argument only for a `next_page_token` that accepts it.

    Unlike `page_size_override`, the slice is almost always present, so passing it only when it is set would still
    break a paginator or pagination strategy defined outside of the CDK whose signature predates the argument. The
    signature is checked instead, and a callee that does not declare `stream_slice` (or `**kwargs`) is called as
    before.
    """
    if stream_slice is None:
        return {}
    function = getattr(next_page_token, "__func__", next_page_token)
    try:
        accepts_stream_slice = _accepts_stream_slice(function)
    except TypeError:
        # Unhashable callables cannot be cached; they are rare enough to inspect on every call.
        accepts_stream_slice = _accepts_stream_slice.__wrapped__(function)
    return {"stream_slice": stream_slice} if accepts_stream_slice else {}


@lru_cache(maxsize=None)
def _accepts_stream_slice(function: Callable[..., Any]) -> bool:
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        return False
    return "stream_slice" in parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    )


@dataclass
class Paginator(ABC, RequestOptionsProvider):
    """
    Defines the token to use to fetch the next page of records from the API.

    If needed, the Paginator will set request options to be set on the HTTP request to fetch the next page of records.
    If the next_page_token is the path to the next page of records, then it should be accessed through the `path` method
    """

    @abstractmethod
    def get_initial_token(self) -> Optional[Any]:
        """
        Get the page token that should be included in the request to get the first page of records
        """

    @abstractmethod
    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Optional[Record],
        last_page_token_value: Optional[Any],
        page_size_override: Optional[int] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> Optional[Mapping[str, Any]]:
        """
        Returns the next_page_token to use to fetch the next page of records.

        :param response: the response to process
        :param last_page_size: the number of records read from the response
        :param last_record: the last record extracted from the response
        :param last_page_token_value: The current value of the page token made on the last request
        :param page_size_override: the page size that was actually requested, when it differs from the configured
            one because of a `REDUCE_PAGE_SIZE` response action
        :param stream_slice: the slice the page was read for, so that a stop condition can compare the page against
            the slice's own window or partition
        :return: A mapping {"next_page_token": <token>} for the next page from the input response object. Returning None means there are no more pages to read in this response.
        """
        pass

    def get_page_size(self) -> Optional[int]:
        """
        Evaluated against the config alone. A pagination strategy whose `page_size` template references the
        response evaluates it with the response in context inside `next_page_token`, so the two can disagree -
        pre-existing, and only observable for a `page_size` that is not a constant.

        :return: the number of records this paginator asks for per page, or None if it does not define one
        """
        return None

    @abstractmethod
    def path(
        self,
        next_page_token: Optional[Mapping[str, Any]],
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> Optional[str]:
        """
        Returns the URL path to hit to fetch the next page of records

        e.g: if you wanted to hit https://myapi.com/v1/some_entity then this will return "some_entity"

        :return: path to hit to fetch the next request. Returning None means the path is not defined by the next_page_token
        """
        pass
