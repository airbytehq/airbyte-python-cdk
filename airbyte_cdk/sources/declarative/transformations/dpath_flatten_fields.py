from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional, Union

import dpath

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class DpathFlattenFields(RecordTransformation):
    """
    Flatten fields only for provided path.

    field_path: List[Union[InterpolatedString, str]] path to the field to flatten.
    delete_origin_value: bool = False whether to delete origin field or keep it. Default is False.
    replace_record: bool = False whether to replace origin record or not. Default is False.

    """

    config: Config
    field_path: List[Union[InterpolatedString, str]]
    parameters: InitVar[Mapping[str, Any]]
    delete_origin_value: bool = False
    replace_record: bool = False

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._field_path = [
            InterpolatedString.create(path, parameters=parameters) for path in self.field_path
        ]
        for path_index in range(len(self.field_path)):
            if isinstance(self.field_path[path_index], str):
                self._field_path[path_index] = InterpolatedString.create(
                    self.field_path[path_index], parameters=parameters
                )

    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> None:
        path = [path.eval(self.config) for path in self._field_path]
        if "*" in path:
            matched = dpath.values(record, path)
            extracted = matched[0] if matched else None
        else:
            extracted = dpath.get(record, path, default=[])

        if isinstance(extracted, dict):
            if self.replace_record and extracted:
                dpath.delete(record, "**")
                record.update(extracted)
            else:
                conflicts = set(extracted.keys()) & set(record.keys())
                if not conflicts:
                    if self.delete_origin_value:
                        dpath.delete(record, path)
                    record.update(extracted)
