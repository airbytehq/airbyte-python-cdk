#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Tuple

DEFAULT_CONNECT_TIMEOUT_SECONDS: float = 30.0
DEFAULT_READ_TIMEOUT_SECONDS: float = 300.0


def default_request_timeout() -> Tuple[float, float]:
    """Returns the `(connect, read)` timeout in seconds applied to requests that do not set their own."""
    return (DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS)


def connect_only_request_timeout() -> Tuple[float, None]:
    """Returns a `(connect, None)` timeout: the connection attempt is bounded, the read is not.

    For endpoints whose response time is the server-side processing time of the request
    (e.g. synchronous document partitioning), a read timeout would be a processing-time
    limit rather than a liveness check.
    """
    connect, _ = default_request_timeout()
    return (connect, None)
