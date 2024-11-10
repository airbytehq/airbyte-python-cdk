#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from dataclasses import InitVar, dataclass
from typing import TYPE_CHECKING, Any

from airbyte_cdk.sources.declarative.interpolation.jinja import JinjaInterpolation


if TYPE_CHECKING:
    from collections.abc import Mapping

    from airbyte_cdk.sources.types import Config


NestedMappingEntry = (
    dict[str, "NestedMapping"] | list["NestedMapping"] | str | int | float | bool | None
)
NestedMapping = dict[str, NestedMappingEntry] | str


@dataclass
class InterpolatedNestedMapping:
    """Wrapper around a nested dict which can contain lists and primitive values where both the keys and values are interpolated recursively.

    Attributes:
        mapping (NestedMapping): to be evaluated
    """

    mapping: NestedMapping
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any] | None) -> None:
        self._interpolation = JinjaInterpolation()
        self._parameters = parameters

    def eval(self, config: Config, **additional_parameters: Any) -> Any:
        return self._eval(self.mapping, config, **additional_parameters)

    def _eval(
        self, value: NestedMapping | NestedMappingEntry, config: Config, **kwargs: Any
    ) -> Any:
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
