# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
import copy
from dataclasses import InitVar, dataclass, field
from typing import Any, List, Mapping, Optional, Set

from airbyte_protocol_dataclasses.models import ConfiguredAirbyteStream

from airbyte_cdk.sources.declarative.requesters.query_properties.property_selector.property_selector import (
    PropertySelector,
)
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config


@dataclass
class JsonSchemaPropertySelector(PropertySelector):
    """
    A class that contains a list of transformations to apply to properties.
    """

    config: Config
    parameters: InitVar[Mapping[str, Any]]
    # For other non-read operations, there is no configured catalog and therefore no schema selection
    configured_stream: Optional[ConfiguredAirbyteStream] = None
    properties_transformations: List[RecordTransformation] = field(default_factory=lambda: [])

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    def select(self) -> Optional[Set[str]]:
        """
        Returns the set of properties that have been selected for the configured stream. The intent being that
        we should only query for selected properties not all since disabled properties are discarded.

        When configured_stream is None, then there was no incoming catalog and all fields should be retrieved.
        This is different from the empty set where the json_schema was empty and no schema fields were selected.
        """

        # For CHECK/DISCOVER operations, there is no catalog and therefore no configured stream or selected
        # columns. In this case we return None which is interpreted by the QueryProperties component to not
        # perform any filtering of schema properties and fetch all of them
        if self.configured_stream is None:
            return None

        schema_properties = copy.deepcopy(
            self.configured_stream.stream.json_schema.get("properties", {})
        )
        if self.properties_transformations:
            for transformation in self.properties_transformations:
                transformation.transform(
                    record=schema_properties,
                    config=self.config,
                )
        return set(schema_properties.keys())
