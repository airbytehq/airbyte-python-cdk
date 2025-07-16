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
        """
        Uses the defined property_list to fetch the total set of properties dynamically or from a static list
        and based on the resulting properties, performs property chunking if applicable.
        :param stream_slice: The StreamSlice of the current partition being processed during the sync. This is included
        because subcomponents of QueryProperties can make use of interpolation of the top-level StreamSlice object
        """
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
