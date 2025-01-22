#  # noqa: A005
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from __future__ import annotations

from airbyte_cdk.sources.types import (
    Config,
    ConnectionDefinition,
    FieldPointer,
    Record,
    StreamSlice,
    StreamState,
)


# Note: This package originally contained class definitions for low-code CDK types, but we promoted them into the Python CDK.
# We've migrated connectors in the repository to reference the new location, but these assignments are used to retain backwards
# compatibility for sources created by OSS customers or on forks. This can be removed when we start bumping major versions.

FieldPointer = FieldPointer  # noqa: PLW0127
Config = Config  # noqa: PLW0127
ConnectionDefinition = ConnectionDefinition  # noqa: PLW0127
StreamState = StreamState  # noqa: PLW0127
Record = Record  # noqa: PLW0127
StreamSlice = StreamSlice  # noqa: PLW0127
