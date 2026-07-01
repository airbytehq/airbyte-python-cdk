#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import asyncio
from datetime import datetime
from unittest.mock import MagicMock, mock_open

import pytest

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.unstructured_format import (
    APIProcessingConfigModel,
    UnstructuredFormat,
)
from airbyte_cdk.sources.file_based.exceptions import RecordParseError
from airbyte_cdk.sources.file_based.file_types import UnstructuredParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile

FILE_URI = "path/to/file.xyz"


@pytest.mark.parametrize(
    "file_uri, format_config, raises",
    [
        pytest.param(
            "file.md",
            UnstructuredFormat(skip_unprocessable_files=False),
            False,
            id="markdown_file",
        ),
        pytest.param(
            "file.csv",
            UnstructuredFormat(skip_unprocessable_files=False),
            True,
            id="wrong_file_format",
        ),
        pytest.param(
            "file.csv",
            UnstructuredFormat(skip_unprocessable_files=True),
            False,
            id="wrong_file_format_skipping",
        ),
        pytest.param(
            "file.pdf",
            UnstructuredFormat(skip_unprocessable_files=False),
            False,
            id="pdf_file",
        ),
        pytest.param(
            "file.docx",
            UnstructuredFormat(skip_unprocessable_files=False),
            False,
            id="docx_file",
        ),
        pytest.param(
            "file.pptx",
            UnstructuredFormat(skip_unprocessable_files=False),
            False,
            id="pptx_file",
        ),
        pytest.param(
            "file.txt",
            UnstructuredFormat(skip_unprocessable_files=False),
            False,
            id="txt_file",
        ),
    ],
)
def test_infer_schema(file_uri, format_config, raises):
    main_loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stream_reader = MagicMock()
    mock_open(stream_reader.open_file)
    fake_file = MagicMock()
    fake_file.uri = file_uri
    fake_file.mime_type = None
    logger = MagicMock()
    config = MagicMock()
    config.format = format_config
    if raises:
        with pytest.raises(RecordParseError):
            loop.run_until_complete(
                UnstructuredParser().infer_schema(config, fake_file, stream_reader, logger)
            )
    else:
        schema = loop.run_until_complete(
            UnstructuredParser().infer_schema(config, fake_file, stream_reader, logger)
        )
        assert schema == {
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
    loop.close()
    asyncio.set_event_loop(main_loop)


@pytest.mark.parametrize(
    "file_uri, format_config, file_content, raises, expected_content_substring",
    [
        pytest.param(
            "file.md",
            UnstructuredFormat(skip_unprocessable_files=False),
            b"# Hello markdown",
            False,
            "# Hello markdown",
            id="markdown_file",
        ),
        pytest.param(
            "file.txt",
            UnstructuredFormat(skip_unprocessable_files=False),
            b"Plain text content",
            False,
            "Plain text content",
            id="txt_file",
        ),
        pytest.param(
            "file.csv",
            UnstructuredFormat(skip_unprocessable_files=False),
            b"col1,col2",
            True,
            None,
            id="unsupported_file_type_raises",
        ),
        pytest.param(
            "file.csv",
            UnstructuredFormat(skip_unprocessable_files=True),
            b"col1,col2",
            False,
            None,
            id="unsupported_file_type_skipped",
        ),
    ],
)
def test_parse_records(file_uri, format_config, file_content, raises, expected_content_substring):
    stream_reader = MagicMock()
    mock_open(stream_reader.open_file, read_data=file_content)
    fake_file = RemoteFile(uri=file_uri, last_modified=datetime.now())
    fake_file.uri = file_uri
    fake_file.mime_type = None
    logger = MagicMock()
    config = MagicMock()
    config.format = format_config

    if raises:
        with pytest.raises(RecordParseError):
            list(
                UnstructuredParser().parse_records(
                    config, fake_file, stream_reader, logger, MagicMock()
                )
            )
    else:
        records = list(
            UnstructuredParser().parse_records(
                config, fake_file, stream_reader, logger, MagicMock()
            )
        )
        assert len(records) == 1
        record = records[0]
        assert record["document_key"] == file_uri
        if expected_content_substring:
            assert expected_content_substring in record["content"]
            assert record["_ab_source_file_parse_error"] is None
        else:
            # Skipped unsupported file
            assert record["content"] is None
            assert record["_ab_source_file_parse_error"] is not None


@pytest.mark.parametrize(
    "format_config, is_ok, expected_error",
    [
        pytest.param(
            UnstructuredFormat(skip_unprocessable_files=False),
            True,
            None,
            id="local_default",
        ),
        pytest.param(
            UnstructuredFormat(skip_unprocessable_files=False, strategy="fast"),
            True,
            None,
            id="local_strategy_ignored",
        ),
        pytest.param(
            UnstructuredFormat(
                skip_unprocessable_files=False,
                processing=APIProcessingConfigModel(mode="api", api_key="test"),
            ),
            False,
            "API processing mode is no longer supported",
            id="api_mode_rejected",
        ),
    ],
)
def test_check_config(format_config, is_ok, expected_error):
    result, error = UnstructuredParser().check_config(
        FileBasedStreamConfig(name="test", format=format_config)
    )
    assert result == is_ok
    if expected_error:
        assert expected_error in error
