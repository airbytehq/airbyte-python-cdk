#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class FlattenFields(RecordTransformation):
    flatten_lists: bool = True

    def transform(
        self,
        record: dict[str, Any],
        config: Config | None = None,  # noqa: ARG002
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
    ) -> None:
        transformed_record = self.flatten_record(record)
        record.clear()
        record.update(transformed_record)

    def flatten_record(self, record: dict[str, Any]) -> dict[str, Any]:
        stack = [(record, "_")]
        transformed_record: dict[str, Any] = {}
        force_with_parent_name = False

        while stack:
            current_record, parent_key = stack.pop()

            if isinstance(current_record, dict):
                for current_key, value in current_record.items():
                    new_key = (
                        f"{parent_key}.{current_key}"
                        if (current_key in transformed_record or force_with_parent_name)
                        else current_key
                    )
                    stack.append((value, new_key))

            elif isinstance(current_record, list) and self.flatten_lists:
                for i, item in enumerate(current_record):
                    force_with_parent_name = True
                    stack.append((item, f"{parent_key}.{i}"))

            else:
                transformed_record[parent_key] = current_record

        return transformed_record
