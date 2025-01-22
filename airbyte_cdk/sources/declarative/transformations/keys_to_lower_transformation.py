#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class KeysToLowerTransformation(RecordTransformation):
    def transform(
        self,
        record: dict[str, Any],
        config: Config | None = None,  # noqa: ARG002
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
    ) -> None:
        for key in set(record.keys()):
            record[key.lower()] = record.pop(key)
