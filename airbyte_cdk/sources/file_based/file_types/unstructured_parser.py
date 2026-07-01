#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
import os
import tempfile
from io import IOBase
from typing import Any, Iterable, Mapping

from markitdown import MarkItDown

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import SchemaType

_markitdown_instance: MarkItDown | None = None

# File extensions that MarkItDown can handle (beyond plain text)
_MARKITDOWN_EXTENSIONS = {".pdf", ".docx", ".pptx", ".xlsx", ".html", ".htm", ".xls"}

# Plain text extensions that should be returned as-is
_PLAINTEXT_EXTENSIONS = {".md", ".txt"}

# All supported extensions
_SUPPORTED_EXTENSIONS = _MARKITDOWN_EXTENSIONS | _PLAINTEXT_EXTENSIONS

# Map of MIME types to file extensions for type detection
_MIME_TO_EXTENSION: dict[str, str] = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "text/markdown": ".md",
    "text/plain": ".txt",
    "text/html": ".html",
}


def _get_markitdown() -> MarkItDown:
    """Return a lazily-initialized singleton `MarkItDown` instance."""
    global _markitdown_instance
    if _markitdown_instance is None:
        _markitdown_instance = MarkItDown()
    return _markitdown_instance


def _get_file_extension(uri: str) -> str | None:
    """Extract the file extension from a URI, or `None` if there is no extension."""
    _, ext = os.path.splitext(uri)
    return ext.lower() if ext else None


def _resolve_extension(uri: str, mime_type: str | None) -> str | None:
    """Resolve the effective file extension from the URI or MIME type."""
    extension = _get_file_extension(uri)
    if extension:
        return extension
    if mime_type and mime_type in _MIME_TO_EXTENSION:
        return _MIME_TO_EXTENSION[mime_type]
    return None


class UnstructuredParser(FileTypeParser):
    """Parses document files (PDF, DOCX, PPTX, MD, TXT, etc.) into markdown text.

    Uses Microsoft's `MarkItDown` library for document-to-markdown conversion.
    Plain text and markdown files are returned as-is without conversion.
    """

    @property
    def parser_max_n_files_for_schema_inference(self) -> int | None:
        """Just check one file as the schema is static."""
        return 1

    @property
    def parser_max_n_files_for_parsability(self) -> int | None:
        """Do not check any files for parsability because it might be an expensive operation."""
        return 0

    def get_parser_defined_primary_key(self, config: FileBasedStreamConfig) -> str | None:
        """Return the `document_key` field as the primary key."""
        return "document_key"

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        format_config = _extract_format(config)
        extension = _resolve_extension(file.uri, file.mime_type)

        if extension is not None and extension not in _SUPPORTED_EXTENSIONS:
            if not format_config.skip_unprocessable_files:
                raise _create_parse_error(file, _unsupported_file_message(extension))

        return {
            "content": {
                "type": "string",
                "description": "Content of the file as markdown. Might be null if the file could not be parsed",
            },
            "document_key": {
                "type": "string",
                "description": "Unique identifier of the document, e.g. the file path",
            },
            "_ab_source_file_parse_error": {
                "type": "string",
                "description": "Error message if the file could not be parsed even though the file is supported",
            },
        }

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Mapping[str, SchemaType] | None,
    ) -> Iterable[dict[str, Any]]:
        format_config = _extract_format(config)
        with stream_reader.open_file(file, self.file_read_mode, None, logger) as file_handle:
            try:
                markdown = self._read_file(file_handle, file, format_config, logger)
                yield {
                    "content": markdown,
                    "document_key": file.uri,
                    "_ab_source_file_parse_error": None,
                }
            except RecordParseError as e:
                if format_config.skip_unprocessable_files:
                    exception_str = str(e)
                    logger.warn(f"File {file.uri} cannot be parsed. Skipping it.")
                    yield {
                        "content": None,
                        "document_key": file.uri,
                        "_ab_source_file_parse_error": exception_str,
                    }
                else:
                    raise

    def _read_file(
        self,
        file_handle: IOBase,
        remote_file: RemoteFile,
        format_config: UnstructuredFormat,
        logger: logging.Logger,
    ) -> str:
        extension = _resolve_extension(remote_file.uri, remote_file.mime_type)

        if extension is not None and extension not in _SUPPORTED_EXTENSIONS:
            raise _create_parse_error(remote_file, _unsupported_file_message(extension))

        # Plain text and markdown files are returned as-is
        if extension in _PLAINTEXT_EXTENSIONS:
            file_content: bytes = file_handle.read()
            return _optional_decode(file_content)

        # Use MarkItDown for document conversion.
        # For files without a recognized extension (extension is None),
        # MarkItDown will attempt content-type auto-detection.
        return self._convert_with_markitdown(file_handle, remote_file, extension)

    def _convert_with_markitdown(
        self,
        file_handle: IOBase,
        remote_file: RemoteFile,
        extension: str | None,
    ) -> str:
        md = _get_markitdown()

        file_handle.seek(0)
        content = file_handle.read()

        suffix = extension or ""
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
            tmp.write(content)
            tmp.flush()
            try:
                result = md.convert(tmp.name)
            except Exception as e:
                raise _create_parse_error(remote_file, str(e))

        return result.markdown

    def check_config(self, config: FileBasedStreamConfig) -> tuple[bool, str | None]:
        """Validate that the parser config is valid.

        For MarkItDown-based parsing, we just verify the library is available and
        that API mode is not configured (since it is no longer supported).
        """
        format_config = _extract_format(config)

        if hasattr(format_config, "processing") and hasattr(format_config.processing, "mode"):
            if format_config.processing.mode == "api":
                return False, (
                    "API processing mode is no longer supported. "
                    "The parser now uses local MarkItDown-based processing. "
                    "Remove the API processing configuration."
                )

        return True, None

    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ_BINARY


def _optional_decode(contents: str | bytes) -> str:
    if isinstance(contents, bytes):
        return contents.decode("utf-8")
    return contents


def _extract_format(config: FileBasedStreamConfig) -> UnstructuredFormat:
    config_format = config.format
    if not isinstance(config_format, UnstructuredFormat):
        raise ValueError(f"Invalid format config: {config_format}")
    return config_format


def _create_parse_error(remote_file: RemoteFile, message: str) -> RecordParseError:
    return RecordParseError(
        FileBasedSourceError.ERROR_PARSING_RECORD, filename=remote_file.uri, message=message
    )


def _unsupported_file_message(extension: str | None) -> str:
    supported = ", ".join(sorted(_SUPPORTED_EXTENSIONS))
    return f"File extension '{extension or 'None'}' is not supported. Supported extensions are {supported}"
