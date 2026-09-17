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
    # Base wait before a page is re-issued, multiplied by the number of attempts made in a row. A
    # `REDUCE_PAGE_SIZE` response never reaches the HTTP retry budget - the exception raised for it is
    # deliberately not a backoff exception - so this is the only thing spacing these requests out, and an
    # API whose 502 means "we are briefly unwell" rather than "your page is too big" needs it to be more
    # than a token pause.
    backoff_seconds: float = 0.5
    # How many times the same page is re-issued unchanged once the page size cannot be shrunk any further,
    # before the read gives up. Zero keeps the strict behaviour: the first response that cannot be answered
    # with a smaller page fails the stream. It is what gives an API whose error is transient a budget at the
    # floor, where reducing is no longer an option but waiting still is.
    retries_at_minimum_page_size: int = 0
    # Appended to the two messages raised once the page size cannot be reduced any further. Those are
    # `transient_error`s the CDK has no remediation for - it only knows that the API rejected every page size
    # asked for - while the connector knows what narrows a query down on this particular API.
    failure_message: Optional[str] = None

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
        if self.backoff_seconds < 0:
            raise ValueError(
                f"The wait between page size reductions cannot be negative. Got {self.backoff_seconds}"
            )
        if self.retries_at_minimum_page_size < 0:
            raise ValueError(
                f"The number of retries at the minimum page size cannot be negative. Got {self.retries_at_minimum_page_size}"
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
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self._config = config
        self._configured_page_size = configured_page_size
        self._stream_name = stream_name
        # Resolved on each call rather than bound here: a default of `time.sleep` would capture
        # the function object, and a test that patches `time.sleep` to keep a run of reductions
        # from taking its two minutes for real would have no effect on an already-bound default.
        self._sleep_override = sleep
        self._current_page_size: Optional[int] = None
        self._attempts = 0
        self._total_reductions = 0
        self._retries_at_minimum_page_size = 0

    def _sleep(self, seconds: float) -> None:
        if self._sleep_override is not None:
            self._sleep_override(seconds)
            return
        time.sleep(seconds)

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
                message=f"Stream {self._stream_name} is set up to reduce its page size on error but does not define "
                f"one. Set `page_size` on the pagination strategy and `page_size_option` on the paginator.",
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
                message=f"The page size of stream {self._stream_name} is not a whole number, so the connector cannot "
                f"reduce it. Make sure the pagination strategy's `page_size` is a number.",
                failure_type=FailureType.config_error,
            )

        reduced_page_size = max(
            self._config.minimum_page_size,
            int(current_page_size // self._config.reduction_factor),
        )
        if reduced_page_size >= current_page_size:
            # Nothing left to give up on the page size. Whether that is the end of the read is
            # `retries_at_minimum_page_size`'s call, not this branch's: the response may still be transient.
            self._retry_at_minimum_page_size(current_page_size)
            return

        self._attempts += 1
        self._total_reductions += 1
        if self._attempts > self._config.max_attempts:
            # The budget counts the reductions that did *not* get a page through, which is what separates a
            # partition that is stuck from one that is merely expensive. A partition where pages keep
            # succeeding restarts this counter on each of them, under either reset policy, and reads to the
            # end however many pages it has; a partition where nothing gets through burns the budget here.
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} reduced its page size {self._attempts - 1} times in a row without a single page succeeding, which is the configured maximum of {self._config.max_attempts} ({self._total_reductions - 1} reductions so far while reading this partition)",
                # `transient_error`, so the only remediation is the connector's own, if it defined one.
                message=self._with_failure_message(
                    f"The source keeps rejecting pages of stream {self._stream_name} at every page size the "
                    f"connector requested, down to {current_page_size} records per page."
                ),
                failure_type=FailureType.transient_error,
            )

        backoff = self._config.backoff_seconds * self._attempts
        LOGGER.info(
            f"Reducing the page size of stream {self._stream_name} from {current_page_size} to {reduced_page_size} "
            f"and retrying the same page in {backoff}s."
        )
        self._current_page_size = reduced_page_size
        self._sleep(backoff)

    def _retry_at_minimum_page_size(self, current_page_size: int) -> None:
        """
        Handle a `REDUCE_PAGE_SIZE` response that arrives when the page size is already as small as the
        connector is allowed to request.

        Reducing is out of options here, but re-issuing the page is not: an API that answers 502 to a page it
        considers too heavy answers the same 502 when it is briefly unwell, and the error handler cannot tell
        the two apart. `REDUCE_PAGE_SIZE` bypasses the HTTP retry budget, so without this budget the second
        kind of 502 ends the stream on the first response once the floor is reached - fewer attempts than the
        same connector got before it adopted the reduction.
        """
        if self._current_page_size is None and self._config.minimum_page_size > 1:
            # No reduction was ever applied and the connector's own floor is what blocks it, so the page size
            # can never be reduced on this stream however the API behaves. That is a configuration error, and
            # it is actionable: both numbers in the message are the connector's to change.
            raise AirbyteTracedException(
                internal_message=f"Stream {self._stream_name} has a configured page size of {current_page_size} which is not greater than the configured minimum page size of {self._config.minimum_page_size}, so it can never be reduced",
                message=f"The page size of stream {self._stream_name} ({current_page_size}) is already at or below "
                f"the configured minimum of {self._config.minimum_page_size}, so the connector cannot reduce it. "
                f"Raise the page size of the stream, or lower `minimum_page_size`.",
                failure_type=FailureType.config_error,
            )

        if self._retries_at_minimum_page_size < self._config.retries_at_minimum_page_size:
            self._retries_at_minimum_page_size += 1
            backoff = self._config.backoff_seconds * self._retries_at_minimum_page_size
            LOGGER.info(
                f"Stream {self._stream_name} cannot request a page smaller than {current_page_size} records, "
                f"so the same page is retried unchanged in {backoff}s "
                f"({self._retries_at_minimum_page_size} of {self._config.retries_at_minimum_page_size})."
            )
            self._sleep(backoff)
            return

        raise AirbyteTracedException(
            internal_message=f"Stream {self._stream_name} still fails with a page size of {current_page_size}, which is the smallest page size allowed by the configured minimum of {self._config.minimum_page_size}"
            + (
                f", after {self._retries_at_minimum_page_size} retries at that size"
                if self._retries_at_minimum_page_size
                else ""
            ),
            # `transient_error`, so the only remediation is the connector's own, if it defined one.
            message=self._with_failure_message(
                f"The source keeps rejecting pages of stream {self._stream_name} at the smallest page size "
                f"the connector is allowed to request ({current_page_size} records per page)."
            ),
            failure_type=FailureType.transient_error,
        )

    def _with_failure_message(self, message: str) -> str:
        """
        :return: the message followed by the connector's `failure_message`, when it defined one
        """
        failure_message = (self._config.failure_message or "").strip()
        if not failure_message:
            return message
        return f"{message} {failure_message}"

    def on_successful_page(self) -> None:
        """
        Called after each page that did not require a reduction.

        The `max_attempts` budget restarts under both policies. It counts the reductions made *in a row*
        without a single page succeeding, which is what separates a partition that is stuck from one that is
        merely expensive: on a stream whose per-page cost varies - the GraphQL case this feature exists for -
        a handful of heavy pages spread over a long partition is a healthy read, and a budget spanning the
        whole partition would fail it at the `max_attempts + 1`-th heavy page while every reduction so far had
        been followed by a successful page. The terminal message says the source rejected every page size the
        connector asked for, so the budget has to mean exactly that.

        The budget still terminates the read, because only a page that succeeded restarts it and only
        `_read_pages` calls this, once per page it consumed. So between any two restarts the partition made one
        page of progress, and the reductions that make no progress are bounded by `max_attempts`. Under `NEVER`
        the reduced page size is never restored either, so it strictly decreases and `minimum_page_size` bounds
        the reductions of the whole partition on its own.

        Only `AFTER_SUCCESSFUL_PAGE` restores the page size. That policy is for an API that rejects the
        configured page size on every page, so every page legitimately costs one reduction; `NEVER` keeps the
        reduced size for the rest of the partition, which is the right behaviour when reductions are one-off.
        """
        self._attempts = 0
        self._retries_at_minimum_page_size = 0

        if self._config.reset_policy != PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE:
            return

        if self._current_page_size is not None:
            LOGGER.info(
                f"Restoring the page size of stream {self._stream_name} from {self._current_page_size} to {self._configured_page_size}."
            )
            self._current_page_size = None
