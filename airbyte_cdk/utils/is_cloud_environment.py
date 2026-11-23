#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import os

CLOUD_DEPLOYMENT_MODE = "cloud"
_CLOUD_ENV_VARS = ("AIRBYTE_EDITION", "DEPLOYMENT_MODE")


def is_cloud_environment() -> bool:
    """
    Returns True if the connector is running in a cloud environment, False otherwise.

    The platform sets AIRBYTE_EDITION, and still sets the legacy DEPLOYMENT_MODE on connector pods. Either one set to
    "cloud" (case-insensitive) counts, so the check holds on services that only set AIRBYTE_EDITION.
    This function can be used to determine whether stricter security measures should be applied.
    """
    return any(
        os.environ.get(name, "").casefold() == CLOUD_DEPLOYMENT_MODE for name in _CLOUD_ENV_VARS
    )
