# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass, field
from typing import Any, List, Mapping, Optional, Set

from airbyte_protocol_dataclasses.models import ConfiguredAirbyteStream

from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config


@dataclass
class JsonSchemaPropertySelector:
    """
    A class that contains a list of transformations to apply to properties.
    """

    configured_stream: ConfiguredAirbyteStream
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    properties_transformations: List[RecordTransformation] = field(default_factory=lambda: [])

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    def select(self) -> Set[str]:
        properties = set()
        for schema_property in self.configured_stream.stream.json_schema.get(
            "properties", {}
        ).keys():
            if self.properties_transformations:
                for transformation in self.properties_transformations:
                    transformation.transform(
                        schema_property,  # type: ignore  # record has type Mapping[str, Any], but Dict[str, Any] expected
                        config=self.config,
                    )
            properties.add(schema_property)
        return properties
