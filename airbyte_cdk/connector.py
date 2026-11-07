#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import logging
import os
import pkgutil
from abc import ABC, abstractmethod
from typing import Any, Generic, Mapping, MutableMapping, Optional, Protocol, TypeVar

import yaml

from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    ConnectorSpecification,
    ConnectorSpecificationSerializer,
    Status,
)


def load_optional_package_file(package: str, filename: str) -> Optional[bytes]:
    """Gets a resource from a package, returning None if it does not exist"""
    try:
        return pkgutil.get_data(package, filename)
    except FileNotFoundError:
        return None


TConfig = TypeVar("TConfig", bound=Mapping[str, Any])

CHECK_SECTION_NOT_IMPLEMENTED = (
    "The connector specification declares sections but the connector does not implement "
    "a section check (section '{section}')."
)


class BaseConnector(ABC, Generic[TConfig]):
    # configure whether the `check_config_against_spec_or_exit()` needs to be called
    check_config_against_spec: bool = True

    @abstractmethod
    def configure(self, config: Mapping[str, Any], temp_dir: str) -> TConfig:
        """
        Persist config in temporary directory to run the Source job
        """

    @staticmethod
    def read_config(config_path: str) -> MutableMapping[str, Any]:
        config = BaseConnector._read_json_file(config_path)
        if isinstance(config, MutableMapping):
            return config
        else:
            raise ValueError(
                f"The content of {config_path} is not an object and therefore is not a valid config. Please ensure the file represent a config."
            )

    @staticmethod
    def _read_json_file(file_path: str) -> Any:
        with open(file_path, "r") as file:
            contents = file.read()

        try:
            return json.loads(contents)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"Could not read json file {file_path}: {error}. Please ensure that it is a valid JSON."
            )

    @staticmethod
    def write_config(config: TConfig, config_path: str) -> None:
        with open(config_path, "w") as fh:
            fh.write(json.dumps(config))

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        """
        Returns the spec for this integration. The spec is a JSON-Schema object describing the required configurations (e.g: username and password)
        required to run this integration. By default, this will be loaded from a "spec.yaml" or a "spec.json" in the package root.
        """

        package = self.__class__.__module__.split(".")[0]

        yaml_spec = load_optional_package_file(package, "spec.yaml")
        json_spec = load_optional_package_file(package, "spec.json")

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

        return ConnectorSpecificationSerializer.load(spec_obj)

    @abstractmethod
    def check(self, logger: logging.Logger, config: TConfig) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration e.g: if a provided Stripe API token can be used to connect
        to the Stripe API.
        """

    def check_section(
        self, logger: logging.Logger, section: str, config: TConfig
    ) -> AirbyteConnectionStatus:
        """
        Checks the configuration up to `section` only, for the stepped setup form
        (see `airbyte_cdk.sources.utils.check_section`). The entrypoint calls this instead of `check` when the config
        carries a `check-section` key naming a declared section other than the last one; `config` then only holds
        the fields of that section and of the sections before it.

        Override it in a connector whose specification declares sections.
        """
        return AirbyteConnectionStatus(
            status=Status.FAILED, message=CHECK_SECTION_NOT_IMPLEMENTED.format(section=section)
        )


class _WriteConfigProtocol(Protocol):
    @staticmethod
    def write_config(config: Mapping[str, Any], config_path: str) -> None: ...


class DefaultConnectorMixin:
    # can be overridden to change an input config
    def configure(
        self: _WriteConfigProtocol, config: Mapping[str, Any], temp_dir: str
    ) -> Mapping[str, Any]:
        config_path = os.path.join(temp_dir, "config.json")
        self.write_config(config, config_path)
        return config


class Connector(DefaultConnectorMixin, BaseConnector[Mapping[str, Any]], ABC): ...
