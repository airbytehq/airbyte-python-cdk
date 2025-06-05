#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import logging
import os
import pkgutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, Mapping, Optional, TypeVar

import yaml
from typing_extensions import Self

from airbyte_cdk.models import AirbyteConnectionStatus
from airbyte_cdk.models.airbyte_protocol import AirbyteMessage, ConnectorSpecification, Type
from airbyte_cdk.sources.message.repository import MessageRepository, PassthroughMessageRepository
from airbyte_cdk.utils.cli_arg_parse import ConnectorCLIArgs, parse_cli_args


def _load_optional_package_file(package: str, filename: str) -> Optional[bytes]:
    """Gets a resource from a package, returning None if it does not exist"""
    try:
        return pkgutil.get_data(package, filename)
    except FileNotFoundError:
        return None


def _write_config(config: Mapping[str, Any], config_path: str) -> None:
    Path(config_path).write_text(json.dumps(config))


TConfig = TypeVar("TConfig", bound=Mapping[str, Any])


class BaseConnector(ABC, Generic[TConfig]):
    # configure whether the `check_config_against_spec_or_exit()` needs to be called
    check_config_against_spec: bool = True

    @abstractmethod
    @classmethod
    def to_typed_config(
        cls,
        config: Mapping[str, Any],
    ) -> TConfig:
        """Return a typed config object from a config dictionary."""
        ...

    @classmethod
    def configure(cls, config: Mapping[str, Any], temp_dir: str) -> TConfig:
        config_path = os.path.join(temp_dir, "config.json")
        _write_config(config, config_path)
        return cls.to_typed_config(config)

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        """
        Returns the spec for this integration. The spec is a JSON-Schema object describing the required configurations (e.g: username and password)
        required to run this integration. By default, this will be loaded from a "spec.yaml" or a "spec.json" in the package root.
        """

        package = self.__class__.__module__.split(".")[0]

        yaml_spec = _load_optional_package_file(package, "spec.yaml")
        json_spec = _load_optional_package_file(package, "spec.json")

        if yaml_spec and json_spec:
            raise RuntimeError(
                "Found multiple spec files in the package. Only one of spec.yaml or spec.json should be provided."
            )

        if yaml_spec:
            spec_obj = yaml.load(yaml_spec, Loader=yaml.SafeLoader)
        elif json_spec:
            try:
                spec_obj = json.loads(json_spec)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Could not read json spec file: {error}. Please ensure that it is a valid JSON."
                )
        else:
            raise FileNotFoundError("Unable to find spec.yaml or spec.json in the package.")

        return ConnectorSpecification.from_dict(spec_obj)

    @abstractmethod
    def check(self, logger: logging.Logger, config: TConfig) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration e.g: if a provided Stripe API token can be used to connect
        to the Stripe API.
        """

    @abstractmethod
    @classmethod
    def create_with_cli_args(
        cls,
        cli_args: ConnectorCLIArgs,
    ) -> Self:
        """Return an instance of the connector, using the provided CLI args."""
        ...

    @classmethod
    def launch_with_cli_args(
        cls,
        args: list[str],
        *,
        logger: logging.Logger | None = None,
        message_repository: MessageRepository | None = None,
        # TODO: Add support for inputs:
        # stdin: StringIO | MessageRepository | None = None,
    ) -> None:
        """Launches the connector with the provided configuration."""
        logger = logger or logging.getLogger(f"airbyte.{type(cls).__name__}")
        message_repository = message_repository or PassthroughMessageRepository()
        parsed_cli_args: ConnectorCLIArgs = parse_cli_args(
            args,
            with_read=True if getattr(cls, "read", False) else False,
            with_write=True if getattr(cls, "write", False) else False,
            with_discover=True if getattr(cls, "discover", False) else False,
        )
        logger.info(f"Launching connector with args: {parsed_cli_args}")
        verb = parsed_cli_args.command

        spec: ConnectorSpecification
        if verb == "check":
            config = cls.to_typed_config(parsed_cli_args.get_config_dict())
            connector = cls.create_with_cli_args(parsed_cli_args)
            connector.check(logger, config)
        elif verb == "spec":
            connector = cls()
            spec = connector.spec(logger)
            message_repository.emit_message(
                AirbyteMessage(
                    type=Type.SPEC,
                    spec=spec,
                )
            )
        elif verb == "discover":
            connector = cls()
            spec = connector.spec(logger)
            print(json.dumps(spec.to_dict(), indent=2))
        elif verb == "read":
            # Implementation for reading data goes here
            pass
        elif verb == "write":
            # Implementation for writing data goes here
            pass
        else:
            raise ValueError(f"Unknown command: {verb}")
        # Implementation for launching the connector goes here
