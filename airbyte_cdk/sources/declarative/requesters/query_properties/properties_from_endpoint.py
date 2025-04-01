# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, List, Mapping, Optional, Union

import dpath

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.types import Config, StreamSlice


@dataclass
class PropertiesFromEndpoint:
    """
    tbd
    """

    property_field_path: List[str]
    retriever: Retriever
    config: Config
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._property_field_path = [
            InterpolatedString(string=property_field, parameters=parameters)
            for property_field in self.property_field_path
        ]

    def get_properties_from_endpoint(self, stream_slice: Optional[StreamSlice]) -> Iterable[str]:
        response_properties = self.retriever.read_records(
            records_schema={}, stream_slice=stream_slice
        )
        for property_obj in response_properties:
            path = [
                node.eval(self.config) if not isinstance(node, str) else node
                for node in self._property_field_path
            ]

            yield dpath.get(property_obj, path)  # type: ignore # extracted will be a MutableMapping, given input data structure
