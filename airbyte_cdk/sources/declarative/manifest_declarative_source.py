#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
import pkgutil
from copy import deepcopy
from importlib import metadata
from types import ModuleType
from typing import Any, Dict, Iterator, List, Mapping, Optional, Set

import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from packaging.version import InvalidVersion, Version

from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    FailureType,
)
from airbyte_cdk.sources.declarative.checks import COMPONENTS_CHECKER_TYPE_MAPPING
from airbyte_cdk.sources.declarative.checks.connection_checker import ConnectionChecker
from airbyte_cdk.sources.declarative.declarative_source import DeclarativeSource
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    DeclarativeStream as DeclarativeStreamModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import Spec as SpecModel
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    StateDelegatingStream as StateDelegatingStreamModel,
)
from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import (
    get_registered_components_module,
)
from airbyte_cdk.sources.declarative.parsers.manifest_component_transformer import (
    ManifestComponentTransformer,
)
from airbyte_cdk.sources.declarative.parsers.manifest_reference_resolver import (
    ManifestReferenceResolver,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.sources.declarative.resolvers import COMPONENTS_RESOLVER_TYPE_MAPPING
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.sources.types import ConnectionDefinition
from airbyte_cdk.sources.utils.slice_logger import (
    AlwaysLogSliceLogger,
    DebugSliceLogger,
    SliceLogger,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


class ManifestDeclarativeSource(DeclarativeSource):
    """Declarative source defined by a manifest of low-code components that define source connector behavior"""

    def __init__(
        self,
        source_config: ConnectionDefinition,
        *,
        config: Mapping[str, Any] | None = None,
        debug: bool = False,
        emit_connector_builder_messages: bool = False,
        component_factory: Optional[ModelToComponentFactory] = None,
    ):
        """
        Args:
            config: The provided config dict.
            source_config: The manifest of low-code components that describe the source connector.
            debug: True if debug mode is enabled.
            emit_connector_builder_messages: True if messages should be emitted to the connector builder.
            component_factory: optional factory if ModelToComponentFactory's default behavior needs to be tweaked.
        """
        self.logger = logging.getLogger(f"airbyte.{self.name}")
        # For ease of use we don't require the type to be specified at the top level manifest, but it should be included during processing
        manifest = dict(source_config)
        if "type" not in manifest:
            manifest["type"] = "DeclarativeSource"

        # If custom components are needed, locate and/or register them.
        self.components_module: ModuleType | None = get_registered_components_module(config=config)

        resolved_source_config = ManifestReferenceResolver().preprocess_manifest(manifest)
        propagated_source_config = ManifestComponentTransformer().propagate_types_and_parameters(
            "", resolved_source_config, {}
        )
        self._source_config = propagated_source_config
        self._debug = debug
        self._emit_connector_builder_messages = emit_connector_builder_messages
        self._constructor = (
            component_factory
            if component_factory
            else ModelToComponentFactory(
                emit_connector_builder_messages,
                max_concurrent_async_job_count=source_config.get("max_concurrent_async_job_count"),
            )
        )
        self._message_repository = self._constructor.get_message_repository()
        self._slice_logger: SliceLogger = (
            AlwaysLogSliceLogger() if emit_connector_builder_messages else DebugSliceLogger()
        )

        self._config = config or {}
        self._validate_source()

    @property
    def resolved_manifest(self) -> Mapping[str, Any]:
        return self._source_config

    @property
    def message_repository(self) -> MessageRepository:
        return self._message_repository

    @property
    def dynamic_streams(self) -> List[Dict[str, Any]]:
        return self._dynamic_stream_configs(
            manifest=self._source_config, config=self._config, with_dynamic_stream_name=True
        )

    @property
    def connection_checker(self) -> ConnectionChecker:
        check = self._source_config["check"]
        if "type" not in check:
            check["type"] = "CheckStream"
        check_stream = self._constructor.create_component(
            COMPONENTS_CHECKER_TYPE_MAPPING[check["type"]],
            check,
            dict(),
            emit_connector_builder_messages=self._emit_connector_builder_messages,
        )
        if isinstance(check_stream, ConnectionChecker):
            return check_stream
        else:
            raise ValueError(
                f"Expected to generate a ConnectionChecker component, but received {check_stream.__class__}"
            )

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        self._emit_manifest_debug_message(
            extra_args={"source_name": self.name, "parsed_config": json.dumps(self._source_config)}
        )

        stream_configs = self._stream_configs(self._source_config) + self._dynamic_stream_configs(
            self._source_config, config
        )

        api_budget_model = self._source_config.get("api_budget")
        if api_budget_model:
            self._constructor.set_api_budget(api_budget_model, config)

        source_streams = [
            self._constructor.create_component(
                StateDelegatingStreamModel
                if stream_config.get("type") == StateDelegatingStreamModel.__name__
                else DeclarativeStreamModel,
                stream_config,
                config,
                emit_connector_builder_messages=self._emit_connector_builder_messages,
            )
            for stream_config in self._initialize_cache_for_parent_streams(deepcopy(stream_configs))
        ]

        return source_streams

    @staticmethod
    def _initialize_cache_for_parent_streams(
        stream_configs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        parent_streams = set()

        def update_with_cache_parent_configs(parent_configs: list[dict[str, Any]]) -> None:
            for parent_config in parent_configs:
                parent_streams.add(parent_config["stream"]["name"])
                if parent_config["stream"]["type"] == "StateDelegatingStream":
                    parent_config["stream"]["full_refresh_stream"]["retriever"]["requester"][
                        "use_cache"
                    ] = True
                    parent_config["stream"]["incremental_stream"]["retriever"]["requester"][
                        "use_cache"
                    ] = True
                else:
                    parent_config["stream"]["retriever"]["requester"]["use_cache"] = True

        for stream_config in stream_configs:
            if stream_config.get("incremental_sync", {}).get("parent_stream"):
                parent_streams.add(stream_config["incremental_sync"]["parent_stream"]["name"])
                stream_config["incremental_sync"]["parent_stream"]["retriever"]["requester"][
                    "use_cache"
                ] = True

            elif stream_config.get("retriever", {}).get("partition_router", {}):
                partition_router = stream_config["retriever"]["partition_router"]

                if isinstance(partition_router, dict) and partition_router.get(
                    "parent_stream_configs"
                ):
                    update_with_cache_parent_configs(partition_router["parent_stream_configs"])
                elif isinstance(partition_router, list):
                    for router in partition_router:
                        if router.get("parent_stream_configs"):
                            update_with_cache_parent_configs(router["parent_stream_configs"])

        for stream_config in stream_configs:
            if stream_config["name"] in parent_streams:
                if stream_config["type"] == "StateDelegatingStream":
                    stream_config["full_refresh_stream"]["retriever"]["requester"]["use_cache"] = (
                        True
                    )
                    stream_config["incremental_stream"]["retriever"]["requester"]["use_cache"] = (
                        True
                    )
                else:
                    stream_config["retriever"]["requester"]["use_cache"] = True

        return stream_configs

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        """
        Returns the connector specification (spec) as defined in the Airbyte Protocol. The spec is an object describing the possible
        configurations (e.g: username and password) which can be configured when running this connector. For low-code connectors, this
        will first attempt to load the spec from the manifest's spec block, otherwise it will load it from "spec.yaml" or "spec.json"
        in the project root.
        """
        self._configure_logger_level(logger)
        self._emit_manifest_debug_message(
            extra_args={"source_name": self.name, "parsed_config": json.dumps(self._source_config)}
        )

        spec = self._source_config.get("spec")
        if spec:
            if "type" not in spec:
                spec["type"] = "Spec"
            spec_component = self._constructor.create_component(SpecModel, spec, dict())
            return spec_component.generate_spec()
        else:
            return super().spec(logger)

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        self._configure_logger_level(logger)
        return super().check(logger, config)

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        self._configure_logger_level(logger)
        yield from super().read(logger, config, catalog, state)

    def _configure_logger_level(self, logger: logging.Logger) -> None:
        """
        Set the log level to logging.DEBUG if debug mode is enabled
        """
        if self._debug:
            logger.setLevel(logging.DEBUG)

    def _validate_source(self) -> None:
        """
        Validates the connector manifest against the declarative component schema
        """
        try:
            raw_component_schema = pkgutil.get_data(
                "airbyte_cdk", "sources/declarative/declarative_component_schema.yaml"
            )
            if raw_component_schema is not None:
                declarative_component_schema = yaml.load(
                    raw_component_schema, Loader=yaml.SafeLoader
                )
            else:
                raise RuntimeError(
                    "Failed to read manifest component json schema required for validation"
                )
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Failed to read manifest component json schema required for validation: {e}"
            )

        streams = self._source_config.get("streams")
        dynamic_streams = self._source_config.get("dynamic_streams")
        if not (streams or dynamic_streams):
            raise ValidationError(
                f"A valid manifest should have at least one stream defined. Got {streams}"
            )

        try:
            validate(self._source_config, declarative_component_schema)
        except ValidationError as e:
            raise ValidationError(
                "Validation against json schema defined in declarative_component_schema.yaml schema failed"
            ) from e

        cdk_version_str = metadata.version("airbyte_cdk")
        cdk_version = self._parse_version(cdk_version_str, "airbyte-cdk")
        manifest_version_str = self._source_config.get("version")
        if manifest_version_str is None:
            raise RuntimeError(
                "Manifest version is not defined in the manifest. This is unexpected since it should be a required field. Please contact support."
            )
        manifest_version = self._parse_version(manifest_version_str, "manifest")

        if (cdk_version.major, cdk_version.minor, cdk_version.micro) == (0, 0, 0):
            # Skipping version compatibility check on unreleased dev branch
            pass
        elif (cdk_version.major, cdk_version.minor) < (
            manifest_version.major,
            manifest_version.minor,
        ):
            raise ValidationError(
                f"The manifest version {manifest_version!s} is greater than the airbyte-cdk package version ({cdk_version!s}). Your "
                f"manifest may contain features that are not in the current CDK version."
            )
        elif (manifest_version.major, manifest_version.minor) < (0, 29):
            raise ValidationError(
                f"The low-code framework was promoted to Beta in airbyte-cdk version 0.29.0 and contains many breaking changes to the "
                f"language. The manifest version {manifest_version!s} is incompatible with the airbyte-cdk package version "
                f"{cdk_version!s} which contains these breaking changes."
            )

    @staticmethod
    def _parse_version(
        version: str,
        version_type: str,
    ) -> Version:
        """Takes a semantic version represented as a string and splits it into a tuple.

        The fourth part (prerelease) is not returned in the tuple.

        Returns:
            Version: the parsed version object
        """
        try:
            parsed_version = Version(version)
        except InvalidVersion as ex:
            raise ValidationError(
                f"The {version_type} version '{version}' is not a valid version format."
            ) from ex
        else:
            # No exception
            return parsed_version

    def _stream_configs(self, manifest: Mapping[str, Any]) -> List[Dict[str, Any]]:
        # This has a warning flag for static, but after we finish part 4 we'll replace manifest with self._source_config
        stream_configs: List[Dict[str, Any]] = manifest.get("streams", [])
        for s in stream_configs:
            if "type" not in s:
                s["type"] = "DeclarativeStream"
        return stream_configs

    def _dynamic_stream_configs(
        self,
        manifest: Mapping[str, Any],
        config: Mapping[str, Any],
        with_dynamic_stream_name: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        dynamic_stream_definitions: List[Dict[str, Any]] = manifest.get("dynamic_streams", [])
        dynamic_stream_configs: List[Dict[str, Any]] = []
        seen_dynamic_streams: Set[str] = set()

        for dynamic_definition_index, dynamic_definition in enumerate(dynamic_stream_definitions):
            components_resolver_config = dynamic_definition["components_resolver"]

            if not components_resolver_config:
                raise ValueError(
                    f"Missing 'components_resolver' in dynamic definition: {dynamic_definition}"
                )

            resolver_type = components_resolver_config.get("type")
            if not resolver_type:
                raise ValueError(
                    f"Missing 'type' in components resolver configuration: {components_resolver_config}"
                )

            if resolver_type not in COMPONENTS_RESOLVER_TYPE_MAPPING:
                raise ValueError(
                    f"Invalid components resolver type '{resolver_type}'. "
                    f"Expected one of {list(COMPONENTS_RESOLVER_TYPE_MAPPING.keys())}."
                )

            if "retriever" in components_resolver_config:
                components_resolver_config["retriever"]["requester"]["use_cache"] = True

            # Create a resolver for dynamic components based on type
            components_resolver = self._constructor.create_component(
                COMPONENTS_RESOLVER_TYPE_MAPPING[resolver_type], components_resolver_config, config
            )

            stream_template_config = dynamic_definition["stream_template"]

            for dynamic_stream in components_resolver.resolve_components(
                stream_template_config=stream_template_config
            ):
                dynamic_stream = {
                    **ManifestComponentTransformer().propagate_types_and_parameters(
                        "", dynamic_stream, {}, use_parent_parameters=True
                    )
                }

                if "type" not in dynamic_stream:
                    dynamic_stream["type"] = "DeclarativeStream"

                # Ensure that each stream is created with a unique name
                name = dynamic_stream.get("name")

                if with_dynamic_stream_name:
                    dynamic_stream["dynamic_stream_name"] = dynamic_definition.get(
                        "name", f"dynamic_stream_{dynamic_definition_index}"
                    )

                if not isinstance(name, str):
                    raise ValueError(
                        f"Expected stream name {name} to be a string, got {type(name)}."
                    )

                if name in seen_dynamic_streams:
                    error_message = f"Dynamic streams list contains a duplicate name: {name}. Please contact Airbyte Support."
                    failure_type = FailureType.system_error

                    if resolver_type == "ConfigComponentsResolver":
                        error_message = f"Dynamic streams list contains a duplicate name: {name}. Please check your configuration."
                        failure_type = FailureType.config_error

                    raise AirbyteTracedException(
                        message=error_message,
                        internal_message=error_message,
                        failure_type=failure_type,
                    )

                seen_dynamic_streams.add(name)
                dynamic_stream_configs.append(dynamic_stream)

        return dynamic_stream_configs

    def _emit_manifest_debug_message(self, extra_args: dict[str, Any]) -> None:
        self.logger.debug("declarative source created from manifest", extra=extra_args)
