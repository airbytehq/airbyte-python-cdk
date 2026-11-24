# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

from pathlib import PurePosixPath
from typing import Optional, Sequence

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_SIGNATURES = {
    "application/gzip": (b"\x1f\x8b\x08",),
    "application/pdf": (b"%PDF-",),
    "application/zip": (b"PK\x03\x04", b"PK\x05\x06"),
    "image/gif": (b"GIF87a", b"GIF89a"),
    "image/jpeg": (b"\xff\xd8\xff",),
    "image/png": (b"\x89PNG\r\n\x1a\n",),
    "image/tiff": (b"II*\x00", b"MM\x00*"),
}
_EXTENSION_TYPES = {
    "gif": "image/gif",
    "gz": "application/gzip",
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "pdf": "application/pdf",
    "png": "image/png",
    "tif": "image/tiff",
    "tiff": "image/tiff",
    "zip": "application/zip",
}


class FileTypePolicy:
    def __init__(
        self, extensions: Optional[Sequence[str]], content_types: Optional[Sequence[str]]
    ) -> None:
        self._extensions = (
            frozenset(value.lower().removeprefix(".") for value in extensions)
            if extensions is not None
            else None
        )
        self._content_types = (
            frozenset(value.lower() for value in content_types)
            if content_types is not None
            else None
        )
        if self._extensions is not None and (
            not self._extensions or not self._extensions.issubset(_EXTENSION_TYPES)
        ):
            raise self._rejection(
                "allowed_extensions must list one or more supported extensions. "
                "Check the FileUploader format list."
            )
        if self._content_types is not None and (
            not self._content_types or not self._content_types.issubset(_SIGNATURES)
        ):
            raise self._rejection(
                "allowed_content_types must list one or more supported media types. "
                "Check the FileUploader format list."
            )

    def expected_type_from_filename(self, filename: str) -> Optional[str]:
        if self._extensions is None:
            return None
        path = PurePosixPath(filename)
        if path.is_absolute() or ".." in path.parts or "\\" in filename or "\x00" in filename:
            raise self._rejection(
                "The attachment name is not a valid relative path. "
                "Set filename_extractor to a relative filename."
            )
        extension = path.suffix.lower().removeprefix(".")
        if extension not in self._extensions:
            raise self._rejection(
                "The attachment filename has a missing or disallowed extension. "
                "Check filename_extractor and allowed_extensions."
            )
        return _EXTENSION_TYPES[extension]

    def validate_content(
        self, content: bytes, content_type: str, expected_from_filename: Optional[str]
    ) -> None:
        declared_type = content_type.partition(";")[0].strip().lower()
        if self._content_types is not None and declared_type not in self._content_types:
            raise self._rejection(
                "The attachment Content-Type is missing or disallowed. "
                "Check the source response header and allowed_content_types."
            )
        if expected_from_filename and not content.startswith(_SIGNATURES[expected_from_filename]):
            raise self._rejection(
                "The attachment bytes do not match the filename extension. "
                "Check the source file and filename_extractor."
            )
        if self._content_types is not None and not content.startswith(_SIGNATURES[declared_type]):
            raise self._rejection(
                "The attachment bytes do not match the Content-Type header. "
                "Check the source file and response header."
            )

    @staticmethod
    def _rejection(message: str) -> AirbyteTracedException:
        return AirbyteTracedException(message=message, failure_type=FailureType.config_error)
