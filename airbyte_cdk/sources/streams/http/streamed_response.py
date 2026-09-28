#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import io
from typing import IO, Any, Optional

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


class _FileLikeRaw(io.RawIOBase):
    """Raw adapter over a SpooledTemporaryFile so BufferedReader can wrap it without fileno()."""

    def __init__(self, fileobj: IO[bytes]) -> None:
        self._fileobj = fileobj

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def readinto(self, b: Any) -> int:
        # SpooledTemporaryFile has no readinto on python 3.10
        data = self._fileobj.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._fileobj.seek(offset, whence)

    def tell(self) -> int:
        return self._fileobj.tell()

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._fileobj.close()


class SpooledResponseBody(io.BufferedReader):
    """File-backed replacement for `requests.Response.raw` once the body has been copied to disk."""

    def __init__(self, spool: IO[bytes]) -> None:
        # spool is a tempfile.SpooledTemporaryFile(); BytesIO below max_size, unlinked file above
        self._spool = spool
        super().__init__(_FileLikeRaw(spool))
        self.auto_close = False  # CompositeRawDecoder sets this attribute; keep it settable

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._spool.close()  # last fd -> kernel frees the unlinked file
