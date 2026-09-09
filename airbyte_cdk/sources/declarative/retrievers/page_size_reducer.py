# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

LOGGER = logging.getLogger("airbyte")


class PageSizeResetPolicy(Enum):
    NEVER = "NEVER"
    AFTER_SUCCESSFUL_PAGE = "AFTER_SUCCESSFUL_PAGE"


@dataclass(frozen=True)
class PageSizeReduction:
    """
    How much to shrink the page size when an error handler resolves to `ResponseAction.REDUCE_PAGE_SIZE`.

    This is immutable configuration: it is created once per stream and shared, while the page size in effect
    lives in a `PageSizeReducer` created per partition read.
    """

    reduction_factor: float = 2.0
    minimum_page_size: int = 1
    max_attempts: int = 5
    reset_policy: PageSizeResetPolicy = PageSizeResetPolicy.NEVER

    def __post_init__(self) -> None:
        if self.reduction_factor <= 1:
            raise ValueError(
                f"The page size reduction factor needs to be greater than 1. Got {self.reduction_factor}"
            )
        if self.minimum_page_size < 1:
            raise ValueError(
                f"The minimum page size needs to be strictly positive. Got {self.minimum_page_size}"
            )
        if self.max_attempts < 1:
            raise ValueError(
                f"The maximum number of page size reductions needs to be strictly positive. Got {self.max_attempts}"
            )


class PageSizeReducer:
    """
    Tracks the page size to use while reading one partition when the API asks for smaller pages.

    One instance is created per `SimpleRetriever._read_pages` call. This is deliberate: a retriever and its
    paginator are shared by every partition of a stream and partitions are read concurrently, so the reduced
    page size must not be stored on the paginator or on the retriever.
    """

    def __init__(
        self,
        config: PageSizeReduction,
        configured_page_size: Optional[int],
        stream_name: str = "",
    ) -> None:
        self._config = config
        self._configured_page_size = configured_page_size
        self._stream_name = stream_name
        self._current_page_size: Optional[int] = None
        self._attempts = 0

    @property
    def page_size_override(self) -> Optional[int]:
        """
        :return: the reduced page size to request, or None while the configured page size is in effect
        """
        return self._current_page_size

    def reduce(self) -> None:
        """
        Shrink the page size used for the next request. Raises once the page size cannot be shrunk any further
        so that an endpoint that keeps failing does not loop forever.
        """
        if self._configured_page_size is None:
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} received a REDUCE_PAGE_SIZE response action but its paginator does not inject a page size",
                message="The connector is set up to reduce its page size on error but does not define one. Set `page_size` on the pagination strategy and `page_size_option` on the paginator.",
                failure_type=FailureType.config_error,
            )

        current_page_size = (
            self._current_page_size
            if self._current_page_size is not None
            else self._configured_page_size
        )

        self._attempts += 1
        if self._attempts > self._config.max_attempts:
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} reduced its page size {self._attempts - 1} times while reading a single partition, which is the maximum allowed",
                message=f"The source kept failing while the connector requested smaller and smaller pages (down to {current_page_size} records per page). The API is likely unable to serve these requests. Try syncing fewer streams at once, or contact the API provider.",
                failure_type=FailureType.transient_error,
            )

        reduced_page_size = max(
            self._config.minimum_page_size,
            int(current_page_size // self._config.reduction_factor),
        )
        if reduced_page_size >= current_page_size:
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} still fails with a page size of {current_page_size} which cannot be reduced below the minimum of {self._config.minimum_page_size}",
                message=f"The source is still failing with the smallest page the connector is allowed to request ({current_page_size} records per page). The API is likely unable to serve this request. Try syncing fewer streams at once, or contact the API provider.",
                failure_type=FailureType.transient_error,
            )

        LOGGER.info(
            f"Reducing the page size of stream {self._stream_name} from {current_page_size} to {reduced_page_size} and retrying the same page."
        )
        self._current_page_size = reduced_page_size

    def on_successful_page(self) -> None:
        """
        Called after each page that did not require a reduction. Note that the number of reductions is not reset
        here: it bounds the number of extra requests for the whole partition.
        """
        if (
            self._config.reset_policy == PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE
            and self._current_page_size is not None
        ):
            LOGGER.info(
                f"Restoring the page size of stream {self._stream_name} from {self._current_page_size} to {self._configured_page_size}."
            )
            self._current_page_size = None
