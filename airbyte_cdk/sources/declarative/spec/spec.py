#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from dataclasses import InitVar, dataclass, field
from typing import Any, List, Mapping, MutableMapping, Optional

import orjson

from airbyte_cdk.config_observation import create_connector_config_control_message
from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.models import (
    AdvancedAuth,
    ConnectorSpecification,
    ConnectorSpecificationSerializer,
)
from airbyte_cdk.models.airbyte_protocol_serializers import AirbyteMessageSerializer
from airbyte_cdk.sources.declarative.models.declarative_component_schema import AuthFlow
from airbyte_cdk.sources.declarative.transformations.config_transformations.config_transformation import (
    ConfigTransformation,
)
from airbyte_cdk.sources.declarative.validators.validator import Validator
from airbyte_cdk.sources.message.repository import InMemoryMessageRepository, MessageRepository
from airbyte_cdk.sources.source import Source


@dataclass
class ConfigMigration:
    transformations: List[ConfigTransformation]
    description: Optional[str] = None


@dataclass
class Spec:
    """
    Returns a connection specification made up of information about the connector and how it can be configured

    Attributes:
        connection_specification (Mapping[str, Any]): information related to how a connector can be configured
        documentation_url (Optional[str]): The link the Airbyte documentation about this connector
    """

    connection_specification: Mapping[str, Any]
    parameters: InitVar[Mapping[str, Any]]
    documentation_url: Optional[str] = None
    advanced_auth: Optional[AuthFlow] = None
    config_migrations: List[ConfigMigration] = field(default_factory=list)
    config_transformations: List[ConfigTransformation] = field(default_factory=list)
    config_validations: List[Validator] = field(default_factory=list)
    message_repository: MessageRepository = InMemoryMessageRepository()

    def generate_spec(self) -> ConnectorSpecification:
        """
        Returns the connector specification according the spec block defined in the low code connector manifest.
        """

        obj: dict[str, Mapping[str, Any] | str | AdvancedAuth] = {
            "connectionSpecification": self.connection_specification
        }

        if self.documentation_url:
            obj["documentationUrl"] = self.documentation_url
        if self.advanced_auth:
            self.advanced_auth.auth_flow_type = self.advanced_auth.auth_flow_type.value  # type: ignore # We know this is always assigned to an AuthFlow which has the auth_flow_type field
            # Map CDK AuthFlow model to protocol AdvancedAuth model
            obj["advanced_auth"] = self.advanced_auth.dict()

        # We remap these keys to camel case because that's the existing format expected by the rest of the platform
        return ConnectorSpecificationSerializer.load(obj)

    def migrate_config(
        self, args: List[str], source: Source, config: MutableMapping[str, Any]
    ) -> None:
        """
        Apply all specified config transformations to the provided config and save the modified config to the given path and emit a control message.

        :param args: Command line arguments
        :param source: Source instance
        :param config: The user-provided config to migrate
        """
        config_path = AirbyteEntrypoint(source).extract_config(args)

        if not config_path:
            return

        mutable_config = dict(config)
        for migration in self.config_migrations:
            for transformation in migration.transformations:
                transformation.transform(mutable_config)

        if mutable_config != config:
            with open(config_path, "w") as f:
                json.dump(mutable_config, f)
            self.message_repository.emit_message(
                create_connector_config_control_message(mutable_config)
            )
            for message in self.message_repository.consume_queue():
                print(orjson.dumps(AirbyteMessageSerializer.dump(message)).decode())

    def transform_config(self, config: MutableMapping[str, Any]) -> None:
        """
        Apply all config transformations to the provided config.

        :param config: The user-provided configuration
        """
        for transformation in self.config_transformations:
            transformation.transform(config)

    def validate_config(self, config: MutableMapping[str, Any]) -> None:
        """
        Apply all config validations to the provided config.

        :param config: The user-provided configuration
        """
        for validator in self.config_validations:
            validator.validate(config)
