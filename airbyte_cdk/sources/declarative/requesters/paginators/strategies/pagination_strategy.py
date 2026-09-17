#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import requests

from airbyte_cdk.sources.types import Record


@dataclass
class PaginationStrategy:
    """
    Defines how to get the next page token
    """

    @property
    @abstractmethod
    def initial_token(self) -> Optional[Any]:
        """
        Return the initial value of the token
        """

    @abstractmethod
    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Optional[Record],
        last_page_token_value: Optional[Any],
    ) -> Optional[Any]:
        """
        :param response: response to process
        :param last_page_size: the number of records read from the response
        :param last_record: the last record extracted from the response
        :param last_page_token_value: The current value of the page token made on the last request
        :return: next page token. Returns None if there are no more pages to fetch
        """
        pass

    @abstractmethod
    def get_page_size(self) -> Optional[int]:
        """
        :return: page size: The number of records to fetch in a page. Returns None if unspecified
        """

    def reduce_page_size(self) -> None:
        """Halve the current effective page size (floored at 1).

        Called by `SimpleRetriever` when a `REDUCE_PAGE_SIZE` response action is received.
        Subclasses that support dynamic page-size reduction should override this method.
        """

    def reset_page_size(self) -> None:
        """Restore the page size to the originally configured default.

        Called by `SimpleRetriever` after a successful page fetch following a reduction.
        Subclasses that support dynamic page-size reduction should override this method.
        """
