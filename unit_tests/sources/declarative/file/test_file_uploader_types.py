# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

from pathlib import Path
from unittest.mock import Mock

import pytest
from requests import Response

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.retrievers.file_uploader import (
    DefaultFileUploader,
    LocalFileSystemFileWriter,
)
from airbyte_cdk.sources.declarative.types import Record, StreamSlice
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

PNG_PREFIX = b"\x89PNG\r\n\x1a\n"
PDF_PREFIX = b"%PDF-"


def test_leading_slash_is_stripped_before_checking_extension(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.retrievers.file_uploader.default_file_uploader.get_files_directory",
        lambda: str(tmp_path),
    )
    response = Mock(spec=Response, content=PDF_PREFIX, headers={"Content-Type": "application/pdf"})
    record = Record(data={}, stream_name="attachments")
    uploader = DefaultFileUploader(
        requester=Mock(**{"send_request.return_value": response}),
        download_target_extractor=Mock(**{"extract_records.return_value": ["/attachment"]}),
        config={},
        file_writer=LocalFileSystemFileWriter(),
        parameters={},
        filename_extractor="/doc.pdf",
        allowed_extensions=["pdf"],
    )

    uploader.upload(record)

    assert (tmp_path / "attachments" / "doc.pdf").read_bytes() == PDF_PREFIX
    assert record.file_reference.source_file_relative_path == "attachments/doc.pdf"


@pytest.mark.parametrize(
    ("filename", "declared_type", "content", "extensions", "media_types", "error_message"),
    [
        (
            "documents/file.PNG/",
            "IMAGE/PNG; charset=binary",
            PNG_PREFIX,
            [".PnG"],
            ["image/png"],
            None,
        ),
        ("file", "image/png", PNG_PREFIX, None, ["image/png"], None),
        ("file", "", b"an opaque attachment", None, None, None),
        ("file", "", PNG_PREFIX, ["png"], None, "missing or disallowed extension"),
        ("file.pdf", "application/pdf", PDF_PREFIX, ["png"], None, "disallowed extension"),
        ("file.png", "image/png", PDF_PREFIX, ["png"], None, "do not match the filename extension"),
        ("file.png", "", PNG_PREFIX, None, ["image/png"], "Content-Type is missing or disallowed"),
        ("file.png", "image/png", PDF_PREFIX, None, ["image/png"], "do not match the Content-Type"),
        (
            "file.png",
            "image/png",
            PNG_PREFIX,
            None,
            [],
            "must list one or more supported media types",
        ),
    ],
)
def test_file_type_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    declared_type: str,
    content: bytes,
    extensions: list[str] | None,
    media_types: list[str] | None,
    error_message: str | None,
) -> None:
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.retrievers.file_uploader.default_file_uploader.get_files_directory",
        lambda: str(tmp_path),
    )
    mock_response = Mock(spec=Response, content=content, headers={"Content-Type": declared_type})
    record = Record(
        data={},
        stream_name="attachments",
        associated_slice=StreamSlice(partition={}, cursor_slice={}),
    )

    def upload() -> None:
        DefaultFileUploader(
            requester=Mock(**{"send_request.return_value": mock_response}),
            download_target_extractor=Mock(**{"extract_records.return_value": ["/attachment"]}),
            config={},
            file_writer=LocalFileSystemFileWriter(),
            parameters={},
            filename_extractor=filename,
            allowed_extensions=extensions,
            allowed_content_types=media_types,
        ).upload(record)

    if error_message is None:
        upload()
        assert (tmp_path / "attachments" / filename).read_bytes() == content
        assert record.file_reference.file_size_bytes == len(content)
    else:
        with pytest.raises(AirbyteTracedException, match=error_message) as error:
            upload()
        assert error.value.failure_type == FailureType.config_error
        assert not record.file_reference
        assert list(tmp_path.iterdir()) == []
