#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any

import requests

from airbyte_cdk.sources.types import Record, StreamSlice, StreamState


class HttpSelector:
    """
    Responsible for translating an HTTP response into a list of records by extracting records from the response and optionally filtering
    records based on a heuristic.
    """

    @abstractmethod
    def select_records(
        self,
        response: requests.Response,
        stream_state: StreamState,
        records_schema: Mapping[str, Any],
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Iterable[Record]:
        """
        Selects records from the response
        :param response: The response to select the records from
        :param stream_state: The stream state
        :param records_schema: json schema of records to return
        :param stream_slice: The stream slice
        :param next_page_token: The paginator token
        :return: List of Records selected from the response
        """
        pass
