#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from os import getenv
from pathlib import Path

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

AIRBYTE_STAGING_DIRECTORY_ENV_VAR = "AIRBYTE_STAGING_DIRECTORY"
DEFAULT_AIRBYTE_STAGING_DIRECTORY = "/staging/files"
DEFAULT_LOCAL_DIRECTORY = "/tmp/airbyte-file-transfer"
UNAVAILABLE_STAGING_DIRECTORY_MESSAGE = "File transfer staging directory is unavailable."


def _validate_staging_directory(staging_directory: str) -> None:
    staging_directory_path = Path(staging_directory)
    if not staging_directory_path.exists():
        raise AirbyteTracedException(
            message=UNAVAILABLE_STAGING_DIRECTORY_MESSAGE,
            internal_message=(
                f"Configured {AIRBYTE_STAGING_DIRECTORY_ENV_VAR} does not exist: "
                f"{staging_directory}"
            ),
            failure_type=FailureType.system_error,
        )

    if not staging_directory_path.is_dir():
        raise AirbyteTracedException(
            message=UNAVAILABLE_STAGING_DIRECTORY_MESSAGE,
            internal_message=(
                f"Configured {AIRBYTE_STAGING_DIRECTORY_ENV_VAR} is not a directory: "
                f"{staging_directory}"
            ),
            failure_type=FailureType.system_error,
        )


def get_files_directory(is_file_transfer: bool = False) -> str:
    configured_staging_directory = getenv(AIRBYTE_STAGING_DIRECTORY_ENV_VAR)
    if configured_staging_directory:
        _validate_staging_directory(configured_staging_directory)
        return configured_staging_directory

    if not is_file_transfer:
        return DEFAULT_LOCAL_DIRECTORY

    _validate_staging_directory(DEFAULT_AIRBYTE_STAGING_DIRECTORY)
    return DEFAULT_AIRBYTE_STAGING_DIRECTORY
