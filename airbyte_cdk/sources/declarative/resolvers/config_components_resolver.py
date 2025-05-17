#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from copy import deepcopy
from dataclasses import InitVar, dataclass, field
from itertools import product
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

import dpath
from typing_extensions import deprecated

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.resolvers.components_resolver import (
    ComponentMappingDefinition,
    ComponentsResolver,
    ResolvedComponentMappingDefinition,
)
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.types import Config


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class StreamConfig:
    """
    Identifies stream config details for dynamic schema extraction and processing.
    """

    configs_pointer: List[Union[InterpolatedString, str]]
    parameters: InitVar[Mapping[str, Any]]
    default_values: Optional[List[Any]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.configs_pointer = [
            InterpolatedString.create(path, parameters=parameters) for path in self.configs_pointer
        ]


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class ConfigComponentsResolver(ComponentsResolver):
    """
    Resolves and populates stream templates with components fetched via source config.

    Attributes:
        stream_config (StreamConfig): The description of stream configuration used to fetch stream config from source config.
        config (Config): Configuration object for the resolver.
        components_mapping (List[ComponentMappingDefinition]): List of mappings to resolve.
        parameters (InitVar[Mapping[str, Any]]): Additional parameters for interpolation.
    """

    stream_configs: List[StreamConfig]
    config: Config
    components_mapping: List[ComponentMappingDefinition]
    parameters: InitVar[Mapping[str, Any]]
    _resolved_components: List[ResolvedComponentMappingDefinition] = field(
        init=False, repr=False, default_factory=list
    )

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        """
        Initializes and parses component mappings, converting them to resolved definitions.

        Args:
            parameters (Mapping[str, Any]): Parameters for interpolation.
        """

        for component_mapping in self.components_mapping:
            if isinstance(component_mapping.value, (str, InterpolatedString)):
                interpolated_value = (
                    InterpolatedString.create(component_mapping.value, parameters=parameters)
                    if isinstance(component_mapping.value, str)
                    else component_mapping.value
                )

                field_path = [
                    InterpolatedString.create(path, parameters=parameters)
                    for path in component_mapping.field_path
                ]

                self._resolved_components.append(
                    ResolvedComponentMappingDefinition(
                        field_path=field_path,
                        value=interpolated_value,
                        value_type=component_mapping.value_type,
                        create_or_update=component_mapping.create_or_update,
                        parameters=parameters,
                    )
                )
            else:
                raise ValueError(
                    f"Expected a string or InterpolatedString for value in mapping: {component_mapping}"
                )

    @property
    def _stream_config(self):
        def resolve_path(pointer):
            return [
                node.eval(self.config) if not isinstance(node, str) else node for node in pointer
            ]

        def normalize_configs(configs):
            return configs if isinstance(configs, list) else [configs]

        def prepare_streams():
            for stream_config in self.stream_configs:
                path = resolve_path(stream_config.configs_pointer)
                stream_configs = dpath.get(dict(self.config), path, default=[])
                stream_configs = normalize_configs(stream_configs)
                if stream_config.default_values:
                    stream_configs += stream_config.default_values
                yield [(i, item) for i, item in enumerate(stream_configs)]

        def merge_combination(combo):
            result = {}
            for config_index, (elem_index, elem) in enumerate(combo):
                if isinstance(elem, dict):
                    result.update(elem)
                else:
                    result.setdefault(f"source_config_{config_index}", (elem_index, elem))
            return result

        all_indexed_streams = list(prepare_streams())
        return [merge_combination(combo) for combo in product(*all_indexed_streams)]

    def resolve_components(
        self, stream_template_config: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        """
        Resolves components in the stream template configuration by populating values.

        Args:
            stream_template_config (Dict[str, Any]): Stream template to populate.

        Yields:
            Dict[str, Any]: Updated configurations with resolved components.
        """
        kwargs = {"stream_template_config": stream_template_config}

        for components_values in self._stream_config:
            updated_config = deepcopy(stream_template_config)
            kwargs["components_values"] = components_values  # type: ignore[assignment] # component_values will always be of type Mapping[str, Any]

            for resolved_component in self._resolved_components:
                valid_types = (
                    (resolved_component.value_type,) if resolved_component.value_type else None
                )
                value = resolved_component.value.eval(
                    self.config, valid_types=valid_types, **kwargs
                )

                path = [path.eval(self.config, **kwargs) for path in resolved_component.field_path]
                parsed_value = self._parse_yaml_if_possible(value)
                updated = dpath.set(updated_config, path, parsed_value)

                if parsed_value and not updated and resolved_component.create_or_update:
                    dpath.new(updated_config, path, parsed_value)

            yield updated_config

    @staticmethod
    def _parse_yaml_if_possible(value: Any) -> Any:
        if isinstance(value, str):
            try:
                import yaml

                return yaml.safe_load(value)
            except Exception:
                return value
        return value
