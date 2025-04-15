# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import requests

from .backoff_strategy import BackoffStrategy


class DefaultBackoffStrategy(BackoffStrategy):
    def backoff_time(
        self,
        response_or_exception: requests.Response | requests.RequestException | None,
        attempt_count: int,
    ) -> float | None:
        return None
