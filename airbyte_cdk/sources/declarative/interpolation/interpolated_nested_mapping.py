#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from typing import Any, Union

from airbyte_cdk.sources.declarative.interpolation.jinja import JinjaInterpolation
from airbyte_cdk.sources.types import Config


NestedMappingEntry = Union[  # noqa: UP007
    dict[str, "NestedMapping"], list["NestedMapping"], str, int, float, bool, None
]
NestedMapping = Union[dict[str, NestedMappingEntry], str]  # noqa: UP007


@dataclass
class InterpolatedNestedMapping:
    """
    Wrapper around a nested dict which can contain lists and primitive values where both the keys and values are interpolated recursively.

    Attributes:
        mapping (NestedMapping): to be evaluated
    """

    mapping: NestedMapping
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any] | None) -> None:
        self._interpolation = JinjaInterpolation()
        self._parameters = parameters

    def eval(self, config: Config, **additional_parameters: Any) -> Any:  # noqa: ANN401
        return self._eval(self.mapping, config, **additional_parameters)

    def _eval(
        self,
        value: NestedMapping | NestedMappingEntry,
        config: Config,
        **kwargs: Any,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        # Recursively interpolate dictionaries and lists
        if isinstance(value, str):
            return self._interpolation.eval(value, config, parameters=self._parameters, **kwargs)
        if isinstance(value, dict):
            interpolated_dict = {
                self._eval(k, config, **kwargs): self._eval(v, config, **kwargs)
                for k, v in value.items()
            }
            return {k: v for k, v in interpolated_dict.items() if v is not None}
        if isinstance(value, list):
            return [self._eval(v, config, **kwargs) for v in value]
        return value
