#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from typing import Optional

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


class RequestWindowSplitRequiredException(AirbyteTracedException):
    """
    Signals that the request window backing the current read must be split and retried as smaller children.

    This is control flow rather than a failure: `SimpleRetriever.read_records` catches it and replaces the
    failing window with ordered, non-overlapping child windows produced by the stream's cursor. Unlike
    `PageSizeReductionRequiredException`, which is only ever raised by `HttpClient`, this exception is public
    and is meant to be raised directly by custom code - a `CustomRetriever`, a `CustomExtractor`, or code
    handling a transport or streamed-decoding failure - so a connector can opt into window splitting for a
    failure the built-in `HttpResponseFilter`/`ResponseAction.SPLIT_REQUEST_WINDOW` path cannot see, without
    the CDK needing to know about that connector's specific exception types.

    The classifying error's `failure_type` and `error_message` are preserved on this exception so the eventual
    terminal failure - if the window cannot be split any further - reports the original cause rather than a
    generic "please split" message. Callers with an underlying exception should chain it the normal Python way,
    `raise RequestWindowSplitRequiredException(...) from original_exception`, so the original traceback is
    not lost.
    """

    def __init__(
        self,
        stream_name: Optional[str] = None,
        error_message: Optional[str] = None,
        failure_type: Optional[FailureType] = None,
    ) -> None:
        stream = f" of stream {stream_name}" if stream_name else ""
        detail = f": {error_message}" if error_message else ""
        self.classified_failure_type = failure_type
        super().__init__(
            internal_message=f"A request window split was requested{stream}{detail}",
            message=f"The API rejected the current request window{stream} and requires a smaller one. If this message ends a sync, the stream is not set up to split its window: add `request_window_splitting` to its retriever, or remove the `SPLIT_REQUEST_WINDOW` action from its error handler.",
            failure_type=failure_type or FailureType.config_error,
        )


class RequestWindowSplitNotSupportedException(AirbyteTracedException):
    """
    Raised when a window split is requested but the retriever cannot honor it.

    The factory rejects this at config time wherever it can see the error handler, so reaching this means the
    signal came from somewhere it cannot inspect - a custom error handler, a custom requester/retriever raising
    `RequestWindowSplitRequiredException` directly, or a stream whose cursor has no `split_request_window`
    method (for example, a non-datetime or non-incremental slicer).
    """

    def __init__(self, stream_name: Optional[str] = None) -> None:
        stream = f"Stream {stream_name}" if stream_name else "The stream"
        super().__init__(
            internal_message=f"A request window split was requested for stream {stream_name} but its retriever defines no `request_window_splitting`, or its cursor does not support window splitting. The action is only supported on a SimpleRetriever whose cursor has a `split_request_window` method and that defines `request_window_splitting`.",
            message=f"{stream} resolves to a request window split but is not set up to split its window. Add `request_window_splitting` to the stream's retriever (this requires a datetime-based incremental cursor with `cursor_granularity` set), or remove the `SPLIT_REQUEST_WINDOW` action from its error handler.",
            failure_type=FailureType.config_error,
        )
