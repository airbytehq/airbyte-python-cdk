# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional

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

    # `PageSizeReductionRequiredException` is deliberately neither a `BaseBackoffException` nor a transient
    # exception, so the reduced page is re-issued outside of the HTTP retry budget and nothing else spaces
    # those requests out. The wait is kept non-zero and grows with the number of reductions so an endpoint
    # that fails whatever page size we ask for degrades to a slow retry instead of a burst of requests.
    BACKOFF_SECONDS: float = 0.5

    # Backstop that bounds the reductions for the whole partition regardless of the reset policy. Under
    # `AFTER_SUCCESSFUL_PAGE` the `max_attempts` budget restarts on every successful page, which is what lets a
    # long partition complete when the API needs one reduction per page - so something else has to guarantee
    # that the partition cannot spend reductions forever. It is deliberately far above any sane `max_attempts`
    # because reaching it is a pathology, not a tuning problem, and it is therefore not exposed in the schema.
    MAX_TOTAL_REDUCTIONS: int = 1000

    def __init__(
        self,
        config: PageSizeReduction,
        configured_page_size: Optional[int],
        stream_name: str = "",
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._configured_page_size = configured_page_size
        self._stream_name = stream_name
        self._sleep = sleep
        self._current_page_size: Optional[int] = None
        self._attempts = 0
        self._total_reductions = 0

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
        if not isinstance(current_page_size, int) or isinstance(current_page_size, bool):
            # A custom pagination strategy can return anything from `get_page_size`. Reducing
            # is arithmetic, so a non-integer would otherwise fail with a bare TypeError in
            # the middle of a sync.
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} has a page size of type {type(current_page_size).__name__}: {current_page_size!r}",
                message="The connector is set up to reduce its page size on error but its page size is not a whole number. "
                "Make sure the pagination strategy's `get_page_size` returns an integer.",
                failure_type=FailureType.config_error,
            )

        self._attempts += 1
        self._total_reductions += 1
        if (
            self._attempts > self._config.max_attempts
            or self._total_reductions > self.MAX_TOTAL_REDUCTIONS
        ):
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} reduced its page size {self._total_reductions - 1} times while reading a single partition ({self._attempts - 1} of them since the last successful page), which is the maximum allowed",
                message=f"The source kept failing while the connector requested smaller and smaller pages (down to {current_page_size} records per page). The API is likely unable to serve these requests. Try syncing fewer streams at once, or contact the API provider.",
                failure_type=FailureType.transient_error,
            )

        reduced_page_size = max(
            self._config.minimum_page_size,
            int(current_page_size // self._config.reduction_factor),
        )
        if reduced_page_size >= current_page_size:
            if self._current_page_size is None:
                # No reduction was ever applied, so the configured page size is already at or below the
                # minimum. Nothing about the response can fix that, which makes it a configuration error
                # rather than something the platform should retry the whole job for.
                raise AirbyteTracedException(
                    internal_message=f"Stream {self._stream_name} has a configured page size of {current_page_size} which is not greater than the configured minimum page size of {self._config.minimum_page_size}, so it can never be reduced",
                    message=f"The connector is set up to reduce its page size on error but its page size ({current_page_size}) is already at or below the configured minimum of {self._config.minimum_page_size}. Lower `minimum_page_size` or raise the pagination strategy's `page_size`.",
                    failure_type=FailureType.config_error,
                )
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} still fails with a page size of {current_page_size}, which is the smallest page size allowed by the configured minimum of {self._config.minimum_page_size}",
                message=f"The source is still failing with the smallest page the connector is allowed to request ({current_page_size} records per page). The API is likely unable to serve this request. Try syncing fewer streams at once, or contact the API provider.",
                failure_type=FailureType.transient_error,
            )

        backoff = self.BACKOFF_SECONDS * self._attempts
        LOGGER.info(
            f"Reducing the page size of stream {self._stream_name} from {current_page_size} to {reduced_page_size} "
            f"and retrying the same page in {backoff}s."
        )
        self._current_page_size = reduced_page_size
        self._sleep(backoff)

    def on_successful_page(self) -> None:
        """
        Called after each page that did not require a reduction.

        Under `NEVER` nothing happens: the reduced page size stays in effect and `max_attempts` keeps bounding
        the reductions for the whole partition, which is the right budget when reductions are one-off.

        Under `AFTER_SUCCESSFUL_PAGE` the page size is restored and the `max_attempts` budget restarts. The
        reduction count has to restart with it: this policy exists for an API that rejects the configured page
        size on every page, so every page legitimately costs one reduction, and a budget spanning the whole
        partition would fail the sync at page `max_attempts + 1` no matter how healthy the reads are.
        `MAX_TOTAL_REDUCTIONS` still bounds the partition, so the sync cannot run forever.
        """
        if self._config.reset_policy != PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE:
            return

        self._attempts = 0
        if self._current_page_size is not None:
            LOGGER.info(
                f"Restoring the page size of stream {self._stream_name} from {self._current_page_size} to {self._configured_page_size}."
            )
            self._current_page_size = None
