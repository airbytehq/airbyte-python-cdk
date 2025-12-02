#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import dpath
import requests

from airbyte_cdk.sources.declarative.decoders import Decoder, JsonDecoder
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


@dataclass
class DpathExtractor(RecordExtractor):
    """
    Record extractor that searches a decoded response over a path defined as an array of fields.

    If the field path points to an array, that array is returned.
    If the field path points to an object, that object is returned wrapped as an array.
    If the field path points to an empty object, an empty array is returned.
    If the field path points to a non-existing path, an empty array is returned.

    Optionally, records can be expanded by extracting items from a nested array field.
    When expand_records_from_field is configured, each extracted record is expanded by
    extracting items from the specified nested array path and emitting each item as a
    separate record. If remain_original_record is True, each expanded record will include
    the original parent record in an "original_record" field.

    Examples of instantiating this transform:
    ```
      extractor:
        type: DpathExtractor
        field_path:
          - "root"
          - "data"
    ```

    ```
      extractor:
        type: DpathExtractor
        field_path:
          - "root"
          - "{{ parameters['field'] }}"
    ```

    ```
      extractor:
        type: DpathExtractor
        field_path: []
    ```

    ```
      extractor:
        type: DpathExtractor
        field_path:
          - "data"
          - "object"
        expand_records_from_field:
          - "lines"
          - "data"
        remain_original_record: true
    ```

    Attributes:
        field_path (Union[InterpolatedString, str]): Path to the field that should be extracted
        config (Config): The user-provided configuration as specified by the source's spec
        decoder (Decoder): The decoder responsible to transfom the response in a Mapping
        expand_records_from_field (Optional[List[Union[InterpolatedString, str]]]): Path to a nested array field within each extracted record. If provided, items from this array will be extracted and emitted as separate records.
        remain_original_record (bool): If True and expand_records_from_field is set, each expanded record will include the original parent record in an "original_record" field. Defaults to False.
    """

    field_path: List[Union[InterpolatedString, str]]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    decoder: Decoder = field(default_factory=lambda: JsonDecoder(parameters={}))
    expand_records_from_field: Optional[List[Union[InterpolatedString, str]]] = None
    remain_original_record: bool = False

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._field_path = [
            InterpolatedString.create(path, parameters=parameters) for path in self.field_path
        ]
        for path_index in range(len(self.field_path)):
            if isinstance(self.field_path[path_index], str):
                self._field_path[path_index] = InterpolatedString.create(
                    self.field_path[path_index], parameters=parameters
                )

        if self.expand_records_from_field:
            self._expand_path = [
                InterpolatedString.create(path, parameters=parameters)
                for path in self.expand_records_from_field
            ]
        else:
            self._expand_path = None

    def _expand_record(
        self, record: MutableMapping[Any, Any]
    ) -> Iterable[MutableMapping[Any, Any]]:
        """Expand a record by extracting items from a nested array field."""
        if not self._expand_path:
            yield record
            return

        expand_path = [path.eval(self.config) for path in self._expand_path]

        try:
            nested_array = dpath.get(record, expand_path)
        except (KeyError, TypeError):
            yield record
            return

        if not isinstance(nested_array, list):
            yield record
            return

        if len(nested_array) == 0:
            return

        for item in nested_array:
            if isinstance(item, dict):
                expanded_record = dict(item)
                if self.remain_original_record:
                    expanded_record["original_record"] = record
                yield expanded_record
            else:
                yield item

    def extract_records(self, response: requests.Response) -> Iterable[MutableMapping[Any, Any]]:
        for body in self.decoder.decode(response):
            if len(self._field_path) == 0:
                extracted = body
            else:
                path = [path.eval(self.config) for path in self._field_path]
                if "*" in path:
                    extracted = dpath.values(body, path)
                else:
                    extracted = dpath.get(body, path, default=[])  # type: ignore # extracted will be a MutableMapping, given input data structure
            if isinstance(extracted, list):
                for record in extracted:
                    yield from self._expand_record(record)
            elif extracted:
                yield from self._expand_record(extracted)
            else:
                yield from []
