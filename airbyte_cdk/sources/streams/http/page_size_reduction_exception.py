#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from typing import Optional

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


class PageSizeReductionRequiredException(AirbyteTracedException):
    """
    Raised when an error handler resolves to `ResponseAction.REDUCE_PAGE_SIZE`.

    This is control flow rather than a failure: `SimpleRetriever._read_pages` catches it and re-issues the same
    page with a smaller page size. It is raised on every reduction, including the ones a correctly configured
    connector is expected to make, so the message describes what happened and nothing else - it must not read
    as a bug report when it surfaces as the `__context__` of a later failure.

    A reduction the connector cannot honor raises `PageSizeReductionNotSupportedException` instead, but only on
    the `SimpleRetriever` path, which is the only one that knows a reduction was impossible. A `CustomRetriever`,
    or a plain Python-CDK `HttpStream` whose error handler returns `ResponseAction.REDUCE_PAGE_SIZE`, never
    catches this and lets it escape. That escape is kept deliberately - it is the only signal such a stream gets
    - and it is typed `config_error` because nothing retries the page there, so a job-level retry would fail the
    same way forever. On the `SimpleRetriever` path the type is inert, since the exception never leaves the loop.
    """

    def __init__(
        self, stream_name: Optional[str] = None, error_message: Optional[str] = None
    ) -> None:
        stream = f" of stream {stream_name}" if stream_name else ""
        # The error handler's own `error_message` has no other outlet: the raise precedes every site that
        # logs it, so an author who writes one on the reduction filter would otherwise never see it.
        detail = f": {error_message}" if error_message else ""
        super().__init__(
            internal_message=f"An error handler{stream} resolved to REDUCE_PAGE_SIZE{detail}",
            message=f"The API rejected a page{stream} and asked the connector for a smaller one. If this message ends a sync, the stream is not set up to request a smaller page: add `page_size_reduction` to its retriever, or remove the REDUCE_PAGE_SIZE action from its error handler.",
            failure_type=FailureType.config_error,
        )


class PageSizeReductionNotSupportedException(AirbyteTracedException):
    """
    Raised when a reduction is requested on a retriever that cannot honor it.

    The factory rejects this at config time wherever it can see the error handler, so reaching this means the
    action came from somewhere it cannot inspect: a custom error handler, or a custom requester or retriever.
    """

    def __init__(self, stream_name: Optional[str] = None) -> None:
        stream = f"Stream {stream_name}" if stream_name else "The stream"
        super().__init__(
            internal_message=f"An error handler of stream {stream_name} resolved to REDUCE_PAGE_SIZE but the retriever it is attached to defines no page_size_reduction. The action is only supported on the main requester of a SimpleRetriever that defines page_size_reduction.",
            message=f"{stream} resolves an API response to the REDUCE_PAGE_SIZE action but is not set up to send a smaller page. Add `page_size_reduction` to the stream's retriever, or remove the REDUCE_PAGE_SIZE action from its error handler.",
            failure_type=FailureType.config_error,
        )
