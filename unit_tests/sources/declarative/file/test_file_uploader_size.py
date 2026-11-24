# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
from requests import Response

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.retrievers.file_uploader import (
    DefaultFileUploader,
    LocalFileSystemFileWriter,
    NoopFileWriter,
)
from airbyte_cdk.sources.declarative.types import Record, StreamSlice
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


@pytest.fixture
def response() -> MagicMock:
    response = MagicMock(spec=Response)
    response.__enter__.return_value = response
    response.headers = {}
    response.iter_content.return_value = iter((b"ab", b"cd"))
    return response


@pytest.fixture
def record() -> Record:
    return Record(
        data={},
        stream_name="attachments",
        associated_slice=StreamSlice(partition={}, cursor_slice={}),
    )


@pytest.fixture
def uploader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, response: MagicMock
) -> DefaultFileUploader:
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.retrievers.file_uploader.default_file_uploader.get_files_directory",
        lambda: str(tmp_path),
    )
    return DefaultFileUploader(
        requester=Mock(spec=HttpRequester, **{"send_request.return_value": response}),
        download_target_extractor=Mock(**{"extract_records.return_value": ["/attachment"]}),
        config={},
        file_writer=LocalFileSystemFileWriter(),
        parameters={},
        filename_extractor="attachment",
        max_file_size_bytes=4,
    )


def test_file_at_byte_limit_is_staged(
    tmp_path: Path, uploader: DefaultFileUploader, record: Record, response: MagicMock
) -> None:
    uploader.upload(record)

    assert (tmp_path / "attachments/attachment").read_bytes() == b"abcd"
    assert record.file_reference.file_size_bytes == 4
    assert uploader.requester.send_request.call_args.kwargs["stream_response"] is True
    assert uploader.requester.send_request.call_args.kwargs["bypass_cache"] is True
    response.__exit__.assert_called_once()


@pytest.mark.parametrize("content_length", [None, "1", "invalid"])
def test_counts_body_bytes_and_discards_partial_file(
    tmp_path: Path,
    uploader: DefaultFileUploader,
    record: Record,
    response: MagicMock,
    content_length: str | None,
) -> None:
    uploader.max_file_size_bytes = 3
    if content_length is not None:
        response.headers["Content-Length"] = content_length

    with pytest.raises(AirbyteTracedException, match="exceeds the 3-byte") as error:
        uploader.upload(record)

    assert error.value.failure_type == FailureType.config_error
    assert not record.file_reference
    assert list((tmp_path / "attachments").iterdir()) == []
    response.__exit__.assert_called_once()


def test_oversized_content_length_does_not_read_body(
    uploader: DefaultFileUploader, record: Record, response: MagicMock
) -> None:
    response.headers["Content-Length"] = "5"

    with pytest.raises(AirbyteTracedException, match="exceeds the 4-byte"):
        uploader.upload(record)

    response.iter_content.assert_not_called()
    response.__exit__.assert_called_once()


def test_builder_applies_byte_limit_without_staging(
    tmp_path: Path, uploader: DefaultFileUploader, record: Record
) -> None:
    uploader.file_writer = NoopFileWriter()
    uploader.max_file_size_bytes = 3

    with pytest.raises(AirbyteTracedException, match="exceeds the 3-byte"):
        uploader.upload(record)

    assert not record.file_reference
    assert list(tmp_path.iterdir()) == []
