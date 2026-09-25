#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import io
from typing import Any, BinaryIO, Optional

import requests

_BODY_STREAMED_ATTR = "_airbyte_body_streamed"
_DOCUMENT_REMAINDER_ATTR = "_airbyte_document_remainder"
_SPOOLED_BODY_SIZE_ATTR = "_airbyte_spooled_body_size"


def mark_body_streamed(response: requests.Response) -> None:
    response.__dict__[_BODY_STREAMED_ATTR] = True


def is_body_streamed(response: requests.Response) -> bool:
    return bool(response.__dict__.get(_BODY_STREAMED_ATTR, False))


def set_document_remainder(response: requests.Response, remainder: Any) -> None:
    response.__dict__[_DOCUMENT_REMAINDER_ATTR] = remainder


def get_document_remainder(response: requests.Response) -> Optional[Any]:
    return response.__dict__.get(_DOCUMENT_REMAINDER_ATTR)


def set_spooled_body_size(response: requests.Response, size: int) -> None:
    response.__dict__[_SPOOLED_BODY_SIZE_ATTR] = size


def get_spooled_body_size(response: requests.Response) -> Optional[int]:
    return response.__dict__.get(_SPOOLED_BODY_SIZE_ATTR)


class SpooledResponseBody(io.BufferedReader):
    """File-backed replacement for `requests.Response.raw` once the body has been copied to disk."""

    def __init__(self, spool: BinaryIO) -> None:
        # spool is a tempfile.TemporaryFile() (already unlinked, anonymous)
        self._spool = spool
        super().__init__(io.FileIO(spool.fileno(), mode="rb", closefd=False))
        self.auto_close = False  # CompositeRawDecoder sets this attribute; keep it settable

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._spool.close()  # last fd -> kernel frees the unlinked file
