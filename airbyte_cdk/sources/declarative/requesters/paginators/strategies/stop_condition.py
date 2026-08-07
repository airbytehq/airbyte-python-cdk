#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional

import requests

from airbyte_cdk.sources.declarative.requesters.paginators.strategies.pagination_strategy import (
    PaginationStrategy,
)
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor
from airbyte_cdk.sources.types import Record

if TYPE_CHECKING:
    from airbyte_cdk.sources.declarative.extractors.record_filter import (
        ClientSideIncrementalRecordFilterDecorator,
    )


class PaginationStopCondition(ABC):
    @abstractmethod
    def is_met(self, record: Optional[Record]) -> bool:
        """
        Given a condition is met, the pagination will stop

        :param record: the last record yielded for the current page, if any. Records dropped by
            record filters are not visible here — a condition that needs to observe them has to get
            that signal from the filter itself (see `FilterAwareStopCondition`).
        """
        raise NotImplementedError()


class CursorStopCondition(PaginationStopCondition):
    def __init__(
        self,
        cursor: Cursor,
    ):
        self._cursor = cursor

    def is_met(self, record: Optional[Record]) -> bool:
        return record is not None and not self._cursor.should_be_synced(record)


class FilterAwareStopCondition(PaginationStopCondition):
    """
    Stop condition for streams combining `is_data_feed` with `is_client_side_incremental`.

    The client-side incremental filter drops records older than the cursor before the paginator can
    observe them, so `CursorStopCondition` — which only sees the last record that survived
    filtering — would never fire. The filter, however, evaluates `should_be_synced` on every raw
    record; this condition stops pagination as soon as the filter reports that the current page
    contained a record that was filtered out as already synced.
    """

    def __init__(self, record_filter: "ClientSideIncrementalRecordFilterDecorator"):
        self._record_filter = record_filter

    def is_met(self, record: Optional[Record]) -> bool:
        return self._record_filter.stale_record_seen_on_current_page


class StopConditionPaginationStrategyDecorator(PaginationStrategy):
    def __init__(self, _delegate: PaginationStrategy, stop_condition: PaginationStopCondition):
        self._delegate = _delegate
        self._stop_condition = stop_condition

    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Optional[Record],
        last_page_token_value: Optional[Any] = None,
    ) -> Optional[Any]:
        # We evaluate in reverse order because the assumption is that most of the APIs using data feed structure
        # will return records in descending order. In terms of performance/memory, we return the records lazily
        # Note: `last_record` may be None even mid-feed when every record of the page was dropped by
        # a record filter, so the stop condition is consulted regardless
        if self._stop_condition.is_met(last_record):
            return None
        return self._delegate.next_page_token(
            response, last_page_size, last_record, last_page_token_value
        )

    def get_page_size(self) -> Optional[int]:
        return self._delegate.get_page_size()

    @property
    def initial_token(self) -> Optional[Any]:
        return self._delegate.initial_token
