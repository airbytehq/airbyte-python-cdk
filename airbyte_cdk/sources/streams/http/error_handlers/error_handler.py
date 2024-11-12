# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import requests

    from .response_models import ErrorResolution


class ErrorHandler(ABC):
    """Abstract base class to determine how to handle a failed HTTP request."""

    @property
    @abstractmethod
    def max_retries(self) -> int | None:
        """The maximum number of retries to attempt before giving up."""
        pass

    @property
    @abstractmethod
    def max_time(self) -> int | None:
        """The maximum amount of time in seconds to retry before giving up."""
        pass

    @abstractmethod
    def interpret_response(self, response: requests.Response | Exception | None) -> ErrorResolution:
        """Interpret the response or exception and return the corresponding response action, failure type, and error message.

        :param response: The HTTP response object or exception raised during the request.
        :return: A tuple containing the response action, failure type, and error message.
        """
        pass
