#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from io import BytesIO
from pathlib import Path
from typing import Any, Mapping

import pytest
from requests import Response

from airbyte_cdk.sources.declarative.retrievers.file_uploader.default_file_uploader import (
    DefaultFileUploader,
)
from airbyte_cdk.sources.declarative.retrievers.file_uploader.local_file_system_file_writer import (
    LocalFileSystemFileWriter,
)
from airbyte_cdk.sources.declarative.types import Record
from airbyte_cdk.sources.utils import files_directory

MARKER = b"synthetic-file-uploader-canary\n"


class SyntheticDownloadTargetExtractor:
    def extract_records(self, _response: Any) -> list[str]:
        return ["synthetic://local-canary"]


class SyntheticRequester:
    def send_request(self, **_kwargs: Any) -> Response:
        response = Response()
        response.raw = BytesIO(MARKER)
        return response


def upload(
    filename_extractor: str,
    *,
    stream_name: str = "safe_stream",
    record_data: Mapping[str, Any] | None = None,
) -> Record:
    uploader = DefaultFileUploader(
        requester=SyntheticRequester(),  # type: ignore[arg-type]
        download_target_extractor=SyntheticDownloadTargetExtractor(),  # type: ignore[arg-type]
        config={},
        file_writer=LocalFileSystemFileWriter(),
        parameters={},
        filename_extractor=filename_extractor,
    )
    record = Record(data=dict(record_data or {}), stream_name=stream_name)
    uploader.upload(record)
    return record


@pytest.fixture
def files_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    selected_root = tmp_path / "staging"
    selected_root.mkdir()
    monkeypatch.setattr(files_directory, "AIRBYTE_STAGING_DIRECTORY", str(selected_root))
    return selected_root


def test_nested_relative_filename_stays_within_files_root(files_root: Path) -> None:
    record = upload(
        "{{ record.name }}",
        record_data={"name": "nested/./control.bin"},
    )

    expected_path = files_root / "safe_stream" / "nested" / "control.bin"
    assert expected_path.read_bytes() == MARKER
    assert record.file_reference
    assert record.file_reference.source_file_relative_path == "safe_stream/nested/control.bin"


def test_rejects_filename_path_traversal(files_root: Path, tmp_path: Path) -> None:
    escaped_path = tmp_path / "escaped.bin"

    with pytest.raises(ValueError, match="filename must remain within"):
        upload(
            "{{ record.name }}",
            record_data={"name": "../../escaped.bin"},
        )

    assert not escaped_path.exists()


def test_rejects_stream_name_path_traversal(files_root: Path, tmp_path: Path) -> None:
    escaped_path = tmp_path / "escaped.bin"

    with pytest.raises(ValueError, match="stream name must remain within"):
        upload("escaped.bin", stream_name="..")

    assert not escaped_path.exists()


@pytest.mark.parametrize("path_field", ["filename", "stream_name"])
def test_rejects_absolute_paths(
    files_root: Path,
    tmp_path: Path,
    path_field: str,
) -> None:
    escaped_path = tmp_path / "escaped.bin"

    with pytest.raises(ValueError, match="must remain within"):
        if path_field == "filename":
            upload(str(escaped_path))
        else:
            upload("escaped.bin", stream_name=str(tmp_path))

    assert not escaped_path.exists()


def test_rejects_symlinked_parent_directory(files_root: Path, tmp_path: Path) -> None:
    outside_directory = tmp_path / "outside"
    outside_directory.mkdir()
    stream_directory = files_root / "safe_stream"
    stream_directory.mkdir()
    (stream_directory / "linked").symlink_to(outside_directory, target_is_directory=True)

    with pytest.raises(ValueError, match="must not contain symlinks"):
        upload("linked/escaped.bin")

    assert not (outside_directory / "escaped.bin").exists()


def test_rejects_symlinked_destination_file(files_root: Path, tmp_path: Path) -> None:
    outside_file = tmp_path / "outside.bin"
    outside_file.write_bytes(b"unchanged")
    stream_directory = files_root / "safe_stream"
    stream_directory.mkdir()
    (stream_directory / "linked.bin").symlink_to(outside_file)

    with pytest.raises(ValueError, match="must not contain symlinks"):
        upload("linked.bin")

    assert outside_file.read_bytes() == b"unchanged"


def test_writer_rejects_path_outside_files_root(files_root: Path, tmp_path: Path) -> None:
    escaped_path = tmp_path / "escaped.bin"

    with pytest.raises(ValueError, match="must remain within"):
        LocalFileSystemFileWriter().write(escaped_path, (MARKER,))

    assert not escaped_path.exists()


def test_fallback_files_root_is_contained(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fallback_root = tmp_path / "fallback"
    monkeypatch.setattr(
        files_directory,
        "AIRBYTE_STAGING_DIRECTORY",
        str(tmp_path / "missing-staging"),
    )
    monkeypatch.setattr(files_directory, "DEFAULT_LOCAL_DIRECTORY", str(fallback_root))

    record = upload("nested/control.bin")

    assert (fallback_root / "safe_stream" / "nested" / "control.bin").read_bytes() == MARKER
    assert record.file_reference
    assert record.file_reference.source_file_relative_path == "safe_stream/nested/control.bin"

    escaped_path = tmp_path / "escaped.bin"
    with pytest.raises(ValueError, match="must remain within"):
        LocalFileSystemFileWriter().write(escaped_path, (MARKER,))
    assert not escaped_path.exists()
