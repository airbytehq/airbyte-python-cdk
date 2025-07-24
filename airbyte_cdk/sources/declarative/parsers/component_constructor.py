#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, Callable, Generic, Mapping, Optional, Type, TypeVar

from airbyte_cdk.sources.declarative.models.declarative_component_schema import ValueType
from airbyte_cdk.sources.types import Config
from pydantic.v1 import BaseModel

M = TypeVar("M", bound=BaseModel)


@dataclass
class ComponentConstructor(Generic[M]):
    @classmethod
    def resolve_dependencies(
        cls,
        model: M,
        config: Config,
        dependency_constructor: Callable[..., Any],
        additional_flags: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        """
        Resolves the component's dependencies, this method should be created in the component,
        if there are any dependencies on other components, or we need to adopt / change / adjust / fine-tune
        specific component's behavior.
        """
        return {}

    @classmethod
    def build(
        cls,
        model: M,
        config: Config,
        dependency_constructor: Callable[..., Any],
        additional_flags: Optional[Mapping[str, Any]],
        **kwargs: Any,
    ) -> "ComponentConstructor[M]":
        """
        Builds up the Component and it's component-specific dependencies.
        Order of operations:
        - build the dependencies first
        - build the component with the resolved dependencies
        """

        # resolve the component dependencies first
        resolved_dependencies: Mapping[str, Any] = cls.resolve_dependencies(
            model=model,
            config=config,
            dependency_constructor=dependency_constructor,
            additional_flags=additional_flags,
            **kwargs,
        )

        # returns the instance of the component class,
        # with resolved dependencies and model-specific arguments.
        return cls(**resolved_dependencies)

    @staticmethod
    def _json_schema_type_name_to_type(value_type: Optional[ValueType]) -> Optional[Type[Any]]:
        if not value_type:
            return None
        names_to_types = {
            ValueType.string: str,
            ValueType.number: float,
            ValueType.integer: int,
            ValueType.boolean: bool,
        }
        return names_to_types[value_type]
