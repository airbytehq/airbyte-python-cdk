#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import os
from typing import Tuple

DEFAULT_CONNECT_TIMEOUT_SECONDS: float = 30.0
DEFAULT_READ_TIMEOUT_SECONDS: float = 300.0
ENV_HTTP_CONNECT_TIMEOUT_SECONDS = "AIRBYTE_HTTP_CONNECT_TIMEOUT_SECONDS"
ENV_HTTP_READ_TIMEOUT_SECONDS = "AIRBYTE_HTTP_READ_TIMEOUT_SECONDS"


def default_request_timeout() -> Tuple[float, float]:
    """Returns the `(connect, read)` timeout in seconds applied to requests that do not set their own.

    Both values can be overridden with the `AIRBYTE_HTTP_CONNECT_TIMEOUT_SECONDS` and
    `AIRBYTE_HTTP_READ_TIMEOUT_SECONDS` environment variables.
    """
    connect = float(os.getenv(ENV_HTTP_CONNECT_TIMEOUT_SECONDS, DEFAULT_CONNECT_TIMEOUT_SECONDS))
    read = float(os.getenv(ENV_HTTP_READ_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS))
    return (connect, read)
