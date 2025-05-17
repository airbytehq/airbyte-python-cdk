#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Iterable, List, Mapping, MutableMapping, Union

import dpath
import requests
from itertools import islice
from airbyte_cdk.sources.declarative.decoders import Decoder, JsonDecoder
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


@dataclass
class CombinedExtractor(RecordExtractor):
    """
    Extractor that combines keys and values from two separate extractors.

    The `keys_extractor` and `values_extractor` extract records independently
    from the response. Their outputs are zipped together to form key-value mappings.

    Each key from `keys_extractor` should correspond to a key in the resulting dictionary,
    and each value from `values_extractor` is the value for that key.

    Example:
      keys_extractor -> yields: ["name", "age"]
      values_extractor -> yields: ["Alice", 30]
      result: { "name": "Alice", "age": 30 }
    """

    extractors: List[RecordExtractor]

    def extract_records(self, response: requests.Response) -> Iterable[MutableMapping[Any, Any]]:
        extractors_records = [extractor.extract_records(response) for extractor in self.extractors]

        for records in zip(*extractors_records):
            merged = {}
            for record in records:
                merged.update(record)  # merge all fields
            yield merged