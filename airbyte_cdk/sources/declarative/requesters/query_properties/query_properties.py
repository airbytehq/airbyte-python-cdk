# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, List, Mapping, Optional, Union

from airbyte_cdk.sources.declarative.requesters.query_properties import (
    PropertiesFromEndpoint,
    PropertyChunking,
)
from airbyte_cdk.sources.types import Config, StreamSlice


@dataclass
class QueryProperties:
    """
    Low-code component that encompasses the behavior to inject additional property values into the outbound API
    requests. Property values can be defined statically within the manifest or dynamically by making requests
    to a partner API to retrieve the properties. Query properties also allow for splitting of the total set of
    properties into smaller chunks to satisfy API restrictions around the total amount of data retrieved
    """

    property_list: Optional[Union[List[str], PropertiesFromEndpoint]]
    always_include_properties: Optional[List[str]]
    property_chunking: Optional[PropertyChunking]
    config: Config
    parameters: InitVar[Mapping[str, Any]]

    def get_request_property_chunks(
        self, stream_slice: Optional[StreamSlice] = None
    ) -> Iterable[List[str]]:
        fields: Union[Iterable[str], List[str]]
        if isinstance(self.property_list, PropertiesFromEndpoint):
            fields = self.property_list.get_properties_from_endpoint(stream_slice=stream_slice)
        else:
            fields = self.property_list if self.property_list else []

        if self.property_chunking:
            yield from self.property_chunking.get_request_property_chunks(
                property_fields=fields, always_include_properties=self.always_include_properties
            )
        else:
            yield list(fields)

    def has_multiple_chunks(self, stream_slice: Optional[StreamSlice]) -> bool:
        property_chunks = iter(self.get_request_property_chunks(stream_slice=stream_slice))
        try:
            next(property_chunks)
            next(property_chunks)
            return True
        except StopIteration:
            return False
