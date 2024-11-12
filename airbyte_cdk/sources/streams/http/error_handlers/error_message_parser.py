#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import requests


class ErrorMessageParser(ABC):
    @abstractmethod
    def parse_response_error_message(self, response: requests.Response) -> str | None:
        """Parse error message from response.
        :param response: response received for the request
        :return: error message
        """
        pass
