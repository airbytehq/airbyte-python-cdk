#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from typing import Optional

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


class OpaqueTokenResetRequiredException(Exception):
    def __init__(self, error_message: Optional[str] = None) -> None:
        self.error_message = error_message
        super().__init__(error_message)


class OpaqueTokenResetNotSupportedException(AirbyteTracedException):
    def __init__(self, stream_name: Optional[str] = None) -> None:
        stream = f"Stream {stream_name}" if stream_name else "The stream"
        message = (
            f"{stream} resolves to a token reset but does not use an OpaqueTokenCursor. "
            "Remove the RESET_OPAQUE_TOKEN action from its error handler."
        )
        super().__init__(
            internal_message=message,
            message=message,
            failure_type=FailureType.config_error,
        )
