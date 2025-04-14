# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from collections.abc import Iterable, Mapping
from dataclasses import InitVar, dataclass
from typing import Any, List, Optional

import dpath

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.types import Config, StreamSlice


@dataclass
class PropertiesFromEndpoint:
    """
    Component that defines the behavior around how to dynamically retrieve a set of request properties from an
    API endpoint. The set retrieved can then be injected into the requests to extract records from an API source.
    """

    property_field_path: list[str]
    retriever: Retriever
    config: Config
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._property_field_path = [
            InterpolatedString(string=property_field, parameters=parameters)
            for property_field in self.property_field_path
        ]

    def get_properties_from_endpoint(self, stream_slice: StreamSlice | None) -> Iterable[str]:
        response_properties = self.retriever.read_records(
            records_schema={}, stream_slice=stream_slice
        )
        for property_obj in response_properties:
            path = [
                node.eval(self.config) if not isinstance(node, str) else node
                for node in self._property_field_path
            ]
            yield dpath.get(property_obj, path, default=[])  # type: ignore # extracted will be a MutableMapping, given input data structure
