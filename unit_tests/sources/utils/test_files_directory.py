#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from pathlib import Path

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.utils.files_directory import get_files_directory
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


@pytest.mark.parametrize(
    "is_file_transfer",
    [
        pytest.param(False, id="local_usage"),
        pytest.param(True, id="file_transfer"),
    ],
)
def test_get_files_directory_uses_configured_staging_directory(
    monkeypatch, tmp_path, is_file_transfer
) -> None:
    monkeypatch.setenv("AIRBYTE_STAGING_DIRECTORY", str(tmp_path))

    assert get_files_directory(is_file_transfer=is_file_transfer) == str(tmp_path)


@pytest.mark.parametrize(
    "is_file_transfer",
    [
        pytest.param(False, id="local_usage"),
        pytest.param(True, id="file_transfer"),
    ],
)
def test_get_files_directory_raises_when_explicit_staging_directory_is_missing(
    monkeypatch, tmp_path, is_file_transfer
) -> None:
    missing_staging_directory = tmp_path / "missing"
    monkeypatch.setenv("AIRBYTE_STAGING_DIRECTORY", str(missing_staging_directory))

    with pytest.raises(AirbyteTracedException) as exc_info:
        get_files_directory(is_file_transfer=is_file_transfer)

    assert exc_info.value.failure_type == FailureType.system_error
    assert exc_info.value.message == "File transfer staging directory is unavailable."
    assert exc_info.value.internal_message == (
        f"Configured AIRBYTE_STAGING_DIRECTORY does not exist: {missing_staging_directory}"
    )


def test_get_files_directory_falls_back_for_local_usage_without_configured_staging_directory(
    monkeypatch,
) -> None:
    monkeypatch.delenv("AIRBYTE_STAGING_DIRECTORY", raising=False)

    assert get_files_directory() == "/tmp/airbyte-file-transfer"


def test_get_files_directory_raises_for_file_transfer_without_shared_staging_directory(
    monkeypatch,
) -> None:
    monkeypatch.delenv("AIRBYTE_STAGING_DIRECTORY", raising=False)

    with pytest.raises(AirbyteTracedException) as exc_info:
        get_files_directory(is_file_transfer=True)

    assert exc_info.value.failure_type == FailureType.system_error
    assert exc_info.value.message == "File transfer staging directory is unavailable."
    assert exc_info.value.internal_message == (
        "Configured AIRBYTE_STAGING_DIRECTORY does not exist: /staging/files"
    )


@pytest.mark.parametrize(
    "is_file_transfer",
    [
        pytest.param(False, id="local_usage"),
        pytest.param(True, id="file_transfer"),
    ],
)
def test_get_files_directory_raises_when_explicit_staging_directory_is_a_file(
    monkeypatch, tmp_path, is_file_transfer
) -> None:
    staging_directory = tmp_path / "stage"
    staging_directory.write_text("not a directory")
    monkeypatch.setenv("AIRBYTE_STAGING_DIRECTORY", str(staging_directory))

    with pytest.raises(AirbyteTracedException) as exc_info:
        get_files_directory(is_file_transfer=is_file_transfer)

    assert exc_info.value.failure_type == FailureType.system_error
    assert exc_info.value.message == "File transfer staging directory is unavailable."
    assert exc_info.value.internal_message == (
        f"Configured AIRBYTE_STAGING_DIRECTORY is not a directory: {staging_directory}"
    )
