#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.file_based.file_types.file_transfer import FileTransfer
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


def test_file_transfer_raises_when_shared_staging_directory_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("AIRBYTE_STAGING_DIRECTORY", raising=False)

    with pytest.raises(AirbyteTracedException) as exc_info:
        FileTransfer()

    assert exc_info.value.failure_type == FailureType.system_error
    assert exc_info.value.message == "File transfer staging directory is unavailable."
    assert exc_info.value.internal_message == (
        "Configured AIRBYTE_STAGING_DIRECTORY does not exist: /staging/files"
    )
