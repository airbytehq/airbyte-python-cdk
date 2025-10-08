#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, Callable, Generic, Mapping, Optional, Type, TypeVar

from pydantic.v1 import BaseModel

from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.models.declarative_component_schema import ValueType
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.types import Config

M = TypeVar("M", bound=BaseModel)


@dataclass
class AdditionalFlags:
    def __init__(
        self,
        emit_connector_builder_messages: bool,
        disable_retries: bool,
        message_repository: MessageRepository,
        connector_state_manager: ConnectorStateManager,
        limit_pages_fetched_per_slice: Optional[int],
        limit_slices_fetched: Optional[int],
    ):
        self.emit_connector_builder_messages = emit_connector_builder_messages
        self.disable_retries = disable_retries
        self.message_repository = message_repository
        self.connector_state_manager = connector_state_manager
        self.limit_pages_fetched_per_slice = limit_pages_fetched_per_slice
        self.limit_slices_fetched = limit_slices_fetched

    @property
    def should_limit_slices_fetched(self) -> bool:
        """
        Returns True if the number of slices fetched should be limited, False otherwise.
        This is used to limit the number of slices fetched during tests.
        """
        return bool(self.limit_slices_fetched or self.emit_connector_builder_messages)


@dataclass
class ComponentConstructor(Generic[M]):
    @classmethod
    def resolve_dependencies(
        cls,
        model: M,
        config: Config,
        dependency_constructor: Callable[..., Any],
        additional_flags: AdditionalFlags,
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
        additional_flags: AdditionalFlags,
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
