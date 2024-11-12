# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from typing import TYPE_CHECKING

from .backoff_strategy import BackoffStrategy


if TYPE_CHECKING:
    import requests


class DefaultBackoffStrategy(BackoffStrategy):
    def backoff_time(
        self,
        response_or_exception: requests.Response | requests.RequestException | None,  # noqa: ARG002  (unused)
        attempt_count: int,  # noqa: ARG002  (unused)
    ) -> float | None:
        return None
