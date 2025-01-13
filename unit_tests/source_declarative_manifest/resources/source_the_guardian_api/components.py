#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import requests

from airbyte_cdk.sources.declarative.requesters.paginators.strategies.page_increment import (
    PageIncrement,
)


@dataclass
class CustomPageIncrement(PageIncrement):
    """
    Starts page from 1 instead of the default value that is 0. Stops Pagination when currentPage is equal to totalPages.
    """

    def next_page_token(self, response: requests.Response, *args) -> Optional[Any]:
        """
        Retrieve the next page token for pagination based on the current page and total pages.
        
        Extracts the current page and total pages from the API response. If more pages are available,
        increments the page counter and returns the next page number. Otherwise, returns None to
        indicate the end of pagination.
        
        Parameters:
            response (requests.Response): The HTTP response from the API containing pagination details.
            *args: Variable length argument list (unused in this implementation).
        
        Returns:
            Optional[Any]: The next page number if more pages are available, or None if pagination is complete.
        
        Raises:
            KeyError: If the expected keys are missing in the response JSON.
        """
        res = response.json().get("response")
        currPage = res.get("currentPage")
        totalPages = res.get("pages")
        if currPage < totalPages:
            self._page += 1
            return self._page
        else:
            return None

    def __post_init__(self, parameters: Mapping[str, Any]):
        """
        Initialize the page increment with a starting page number of 1.
        
        This method is called after the class initialization and sets the initial page 
        to 1 by invoking the parent class's __post_init__ method and then explicitly 
        setting the _page attribute.
        
        Parameters:
            parameters (Mapping[str, Any]): Configuration parameters passed during initialization.
        """
        super().__post_init__(parameters)
        self._page = 1

    def reset(self):
        """
        Reset the page counter to the initial state.
        
        This method resets the internal page counter to 1, allowing pagination to start over from the beginning. It is useful when you want to restart the pagination process for a new request or after completing a previous pagination cycle.
        """
        self._page = 1
