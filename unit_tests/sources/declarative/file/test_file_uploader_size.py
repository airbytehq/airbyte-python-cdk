# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

from io import BytesIO
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, Mock, call

import pytest
from requests import Response
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from requests_mock import Mocker
from urllib3.exceptions import IncompleteRead, ProtocolError
from urllib3.response import HTTPResponse

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.requesters.error_handlers.default_error_handler import (
    DefaultErrorHandler,
)
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
        "airbyte_cdk.sources.utils.files_directory.AIRBYTE_STAGING_DIRECTORY",
        str(tmp_path),
    )
    return DefaultFileUploader(
        requester=Mock(
            spec=HttpRequester,
            disable_retries=False,
            error_handler=None,
            **{"send_request.return_value": response},
        ),
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


@pytest.mark.parametrize(
    ("max_retries", "status_codes", "error_message", "body_restarts"),
    [
        (1, [503, 200], "source connection does not deliver the complete file", 0),
        (2, [503, 503, 200], "source connection does not deliver the complete file", 0),
        (2, [503, 200, 503], "Exhausted available request attempts", 1),
    ],
)
def test_http_and_body_retries_share_attachment_attempt_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    uploader: DefaultFileUploader,
    record: Record,
    requests_mock: Mocker,
    retry_sleep: Mock,
    max_retries: int,
    status_codes: list[int],
    error_message: str,
    body_restarts: int,
) -> None:
    monkeypatch.setattr("time.sleep", Mock())
    url = "https://example.com/attachment"
    incomplete_response = HTTPResponse(
        body=BytesIO(b"ab"), headers={"Content-Length": "4"}, status=200, preload_content=False
    )
    endpoint = requests_mock.get(
        url,
        [
            {"raw": incomplete_response} if status == 200 else {"status_code": status}
            for status in status_codes
        ]
        + [{"content": b"abcd"}],
    )
    uploader.requester = HttpRequester(
        name="attachment",
        url=url,
        config={},
        parameters={},
        error_handler=DefaultErrorHandler(config={}, parameters={}, max_retries=max_retries),
    )

    with pytest.raises(AirbyteTracedException, match=error_message) as error:
        uploader.upload(record)

    assert error.value.failure_type == FailureType.transient_error
    assert endpoint.call_count == max_retries + 1
    assert retry_sleep.call_count == body_restarts
    assert not record.file_reference
    assert list((tmp_path / "attachments").iterdir()) == []


def test_oversized_content_length_does_not_read_body(
    uploader: DefaultFileUploader, record: Record, response: MagicMock
) -> None:
    response.headers["Content-Length"] = "5"

    with pytest.raises(AirbyteTracedException, match="exceeds the 4-byte"):
        uploader.upload(record)

    response.iter_content.assert_not_called()
    response.__exit__.assert_called_once()
    uploader.requester.send_request.assert_called_once()


def test_oversized_download_leaves_existing_attachment_unchanged(
    tmp_path: Path, uploader: DefaultFileUploader, record: Record
) -> None:
    staged_file = tmp_path / "attachments" / "attachment"
    staged_file.parent.mkdir()
    staged_file.write_bytes(b"existing")
    uploader.max_file_size_bytes = 3

    with pytest.raises(AirbyteTracedException, match="exceeds the 3-byte"):
        uploader.upload(record)

    assert staged_file.read_bytes() == b"existing"
    assert list(staged_file.parent.iterdir()) == [staged_file]
    assert not record.file_reference


def test_builder_applies_byte_limit_without_staging(
    tmp_path: Path, uploader: DefaultFileUploader, record: Record
) -> None:
    uploader.file_writer = NoopFileWriter()
    uploader.max_file_size_bytes = 3

    with pytest.raises(AirbyteTracedException, match="exceeds the 3-byte"):
        uploader.upload(record)

    assert not record.file_reference
    assert list(tmp_path.iterdir()) == []


@pytest.fixture
def retry_sleep(monkeypatch: pytest.MonkeyPatch) -> Mock:
    sleep = Mock()
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.retrievers.file_uploader.default_file_uploader.sleep",
        sleep,
    )
    return sleep


@pytest.mark.parametrize(
    "failure",
    [
        ChunkedEncodingError(),
        ConnectionError(),
        ReadTimeout(),
        ProtocolError(),
        IncompleteRead(1, 2),
    ],
)
@pytest.mark.parametrize("builder", [False, True])
def test_restarts_interrupted_download(
    tmp_path: Path,
    uploader: DefaultFileUploader,
    record: Record,
    response: MagicMock,
    retry_sleep: Mock,
    failure: Exception,
    builder: bool,
) -> None:
    if builder:
        uploader.file_writer = NoopFileWriter()

    def interrupted_chunks() -> Iterator[bytes]:
        yield b"ab"
        raise failure

    interrupted = MagicMock(spec=Response, headers={})
    interrupted.iter_content.return_value = interrupted_chunks()
    file_path = tmp_path / "attachments" / "attachment"

    def requests(*args: object, **kwargs: object) -> Response:
        assert not record.file_reference
        assert not file_path.exists()
        assert not list(tmp_path.rglob(".airbyte-upload-*"))
        return interrupted if uploader.requester.send_request.call_count == 1 else response

    uploader.requester.send_request.side_effect = requests

    uploader.upload(record)

    assert uploader.requester.send_request.call_count == 2
    assert (
        uploader.requester.send_request.call_args_list[0]
        == uploader.requester.send_request.call_args_list[1]
    )
    retry_sleep.assert_called_once_with(1)
    interrupted.__exit__.assert_called_once()
    response.__exit__.assert_called_once()
    assert not list(tmp_path.rglob(".airbyte-upload-*"))
    if builder:
        assert record.file_reference.file_size_bytes == NoopFileWriter.NOOP_FILE_SIZE
        assert list(tmp_path.iterdir()) == []
    else:
        assert record.file_reference.file_size_bytes == 4
        assert file_path.read_bytes() == b"abcd"


@pytest.mark.parametrize(
    ("disabled", "configured", "attempts"), [(False, None, 3), (False, 1, 2), (True, None, 1)]
)
def test_attachment_read_failure_exhausts_retry_budget(
    tmp_path: Path,
    uploader: DefaultFileUploader,
    record: Record,
    response: MagicMock,
    retry_sleep: Mock,
    disabled: bool,
    configured: int | None,
    attempts: int,
) -> None:
    uploader.requester.disable_retries = disabled
    if configured is not None:
        uploader.requester.error_handler = Mock(max_retries=configured)
    failure = ChunkedEncodingError()

    def interrupted_chunks(**kwargs: object) -> Iterator[bytes]:
        yield b"ab"
        raise failure

    response.iter_content.side_effect = interrupted_chunks
    with pytest.raises(
        AirbyteTracedException, match="source connection does not deliver the complete file"
    ) as error:
        uploader.upload(record)

    assert error.value.failure_type == FailureType.transient_error
    assert uploader.requester.send_request.call_count == attempts
    assert response.__exit__.call_count == attempts
    assert retry_sleep.call_args_list == [call(1), call(2)][: attempts - 1]
    assert not record.file_reference
    assert list((tmp_path / "attachments").iterdir()) == []
