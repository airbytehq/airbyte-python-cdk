#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import Optional, Union

import requests


class BackoffStrategy(ABC):
    @abstractmethod
    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        """
        Override this method to dynamically determine backoff time e.g: by reading the X-Retry-After header.

        Not called for every retryable response. `HttpClient` skips the strategies entirely when a
        rate-limited response can be retried on another credential -- the authenticator says so via
        `TokenRotatingAuthenticator.has_alternative_token` -- because the wait computed here is
        derived from the credential that was rejected, and the retry will not use it. Implementations
        must therefore not rely on being called for side effects such as counting attempts or
        emitting metrics.

        :param response_or_exception: The response or exception that caused the backoff.
        :param attempt_count: The number of attempts already performed for this request.
        :return how long to backoff in seconds. The return value may be a floating point number for subsecond precision. Returning None defers backoff
        to the default backoff behavior (e.g using an exponential algorithm).
        """
        pass
