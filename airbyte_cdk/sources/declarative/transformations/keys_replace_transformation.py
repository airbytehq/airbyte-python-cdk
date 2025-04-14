#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from typing import Any, Dict, Optional

from airbyte_cdk import InterpolatedString
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class KeysReplaceTransformation(RecordTransformation):
    """
    Transformation that applies keys names replacement.

    Example usage:
    - type: KeysReplace
      old: " "
      new: "_"
    Result:
    from: {"created time": ..., "customer id": ..., "user id": ...}
    to: {"created_time": ..., "customer_id": ..., "user_id": ...}
    """

    old: str
    new: str
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._old = InterpolatedString.create(self.old, parameters=parameters)
        self._new = InterpolatedString.create(self.new, parameters=parameters)

    def transform(
        self,
        record: dict[str, Any],
        config: Config | None = None,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
    ) -> None:
        if config is None:
            config = {}

        kwargs = {"record": record, "stream_state": stream_state, "stream_slice": stream_slice}
        old_key = str(self._old.eval(config, **kwargs))
        new_key = str(self._new.eval(config, **kwargs))

        def _transform(data: dict[str, Any]) -> dict[str, Any]:
            result = {}
            for key, value in data.items():
                updated_key = key.replace(old_key, new_key)
                if isinstance(value, dict):
                    result[updated_key] = _transform(value)
                else:
                    result[updated_key] = value
            return result

        transformed_record = _transform(record)
        record.clear()
        record.update(transformed_record)
