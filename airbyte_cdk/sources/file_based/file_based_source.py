#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import traceback
from abc import ABC
from collections import Counter
from typing import Any, Iterator, List, Mapping, Optional, Tuple, Type, Union

from pydantic.v1.error_wrappers import ValidationError

from airbyte_cdk.logger import AirbyteLogFormatter, init_logger
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    FailureType,
    Level,
    SyncMode,
)
from airbyte_cdk.sources.concurrent_source.concurrent_source import ConcurrentSource
from airbyte_cdk.sources.concurrent_source.concurrent_source_adapter import ConcurrentSourceAdapter
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.file_based.availability_strategy import (
    AbstractFileBasedAvailabilityStrategy,
    DefaultFileBasedAvailabilityStrategy,
)
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from airbyte_cdk.sources.file_based.config.file_based_stream_config import (
    FileBasedStreamConfig,
    ValidationPolicy,
)
from airbyte_cdk.sources.file_based.config.validate_config_transfer_modes import (
    include_identities_stream,
    preserve_directory_structure,
    use_file_transfer,
    use_permissions_transfer,
)
from airbyte_cdk.sources.file_based.discovery_policy import (
    AbstractDiscoveryPolicy,
    DefaultDiscoveryPolicy,
)
from airbyte_cdk.sources.file_based.exceptions import (
    ConfigValidationError,
    FileBasedErrorsCollector,
    FileBasedSourceError,
)
from airbyte_cdk.sources.file_based.file_based_stream_permissions_reader import (
    AbstractFileBasedStreamPermissionsReader,
)
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.file_types import default_parsers
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.schema_validation_policies import (
    DEFAULT_SCHEMA_VALIDATION_POLICIES,
    AbstractSchemaValidationPolicy,
)
from airbyte_cdk.sources.file_based.stream import (
    AbstractFileBasedStream,
    DefaultFileBasedStream,
    FileIdentitiesStream,
    PermissionsFileBasedStream,
)
from airbyte_cdk.sources.file_based.stream.concurrent.adapters import FileBasedStreamFacade
from airbyte_cdk.sources.file_based.stream.concurrent.cursor import (
    AbstractConcurrentFileBasedCursor,
    FileBasedConcurrentCursor,
    FileBasedFinalStateCursor,
)
from airbyte_cdk.sources.file_based.stream.cursor import AbstractFileBasedCursor
from airbyte_cdk.sources.message.repository import InMemoryMessageRepository, MessageRepository
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.concurrent.cursor import CursorField
from airbyte_cdk.utils.analytics_message import create_analytics_message
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

DEFAULT_CONCURRENCY = 100
MAX_CONCURRENCY = 100
INITIAL_N_PARTITIONS = MAX_CONCURRENCY // 2
IDENTITIES_STREAM = "identities"


class FileBasedSource(ConcurrentSourceAdapter, ABC):
    # We make each source override the concurrency level to give control over when they are upgraded.
    _concurrency_level = None

    def __init__(
        self,
        stream_reader: AbstractFileBasedStreamReader,
        spec_class: Type[AbstractFileBasedSpec],
        catalog: Optional[ConfiguredAirbyteCatalog],
        config: Optional[Mapping[str, Any]],
        state: Optional[List[AirbyteStateMessage]],
        availability_strategy: Optional[AbstractFileBasedAvailabilityStrategy] = None,
        discovery_policy: AbstractDiscoveryPolicy = DefaultDiscoveryPolicy(),
        parsers: Mapping[Type[Any], FileTypeParser] = default_parsers,
        validation_policies: Mapping[
            ValidationPolicy, AbstractSchemaValidationPolicy
        ] = DEFAULT_SCHEMA_VALIDATION_POLICIES,
        cursor_cls: Type[
            Union[AbstractConcurrentFileBasedCursor, AbstractFileBasedCursor]
        ] = FileBasedConcurrentCursor,
        stream_permissions_reader: Optional[AbstractFileBasedStreamPermissionsReader] = None,
    ):
        self.stream_reader = stream_reader
        self.stream_permissions_reader = stream_permissions_reader
        self.spec_class = spec_class
        self.config = config
        self.catalog = catalog
        self.state = state
        self.availability_strategy = availability_strategy or DefaultFileBasedAvailabilityStrategy(
            stream_reader
        )
        self.discovery_policy = discovery_policy
        self.parsers = parsers
        self.validation_policies = validation_policies
        self.stream_schemas = (
            {s.stream.name: s.stream.json_schema for s in catalog.streams} if catalog else {}
        )
        self.cursor_cls = cursor_cls
        self.logger = init_logger(f"airbyte.{self.name}")
        self.errors_collector: FileBasedErrorsCollector = FileBasedErrorsCollector()
        self._message_repository: Optional[MessageRepository] = None
        concurrent_source = ConcurrentSource.create(
            MAX_CONCURRENCY,
            INITIAL_N_PARTITIONS,
            self.logger,
            self._slice_logger,
            self.message_repository,
        )
        self._state = None
        super().__init__(concurrent_source)

    @property
    def message_repository(self) -> MessageRepository:
        if self._message_repository is None:
            self._message_repository = InMemoryMessageRepository(
                Level(AirbyteLogFormatter.level_mapping[self.logger.level])
            )
        return self._message_repository

    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        """
        Check that the source can be accessed using the user-provided configuration.

        For each stream, verify that we can list and read files.

        Returns (True, None) if the connection check is successful.

        Otherwise, the "error" object should describe what went wrong.
        """
        try:
            streams = self.streams(config)
        except Exception as config_exception:
            raise AirbyteTracedException(
                internal_message="Please check the logged errors for more information.",
                message=FileBasedSourceError.CONFIG_VALIDATION_ERROR.value,
                exception=AirbyteTracedException(exception=config_exception),
                failure_type=FailureType.config_error,
            )
        if len(streams) == 0:
            return (
                False,
                f"No streams are available for source {self.name}. This is probably an issue with the connector. Please verify that your "
                f"configuration provides permissions to list and read files from the source. Contact support if you are unable to "
                f"resolve this issue.",
            )

        errors = []
        tracebacks = []
        for stream in streams:
            if isinstance(stream, FileIdentitiesStream):
                identity = next(iter(stream.load_identity_groups()))
                if not identity:
                    errors.append(
                        "Unable to get identities for current configuration, please check your credentials"
                    )
                continue
            if not isinstance(stream, AbstractFileBasedStream):
                raise ValueError(f"Stream {stream} is not a file-based stream.")
            try:
                parsed_config = self._get_parsed_config(config)
                availability_method = (
                    stream.availability_strategy.check_availability
                    if use_file_transfer(parsed_config) or use_permissions_transfer(parsed_config)
                    else stream.availability_strategy.check_availability_and_parsability
                )
                (
                    stream_is_available,
                    reason,
                ) = availability_method(stream, logger, self)
            except AirbyteTracedException as ate:
                errors.append(f"Unable to connect to stream {stream.name} - {ate.message}")
                tracebacks.append(traceback.format_exc())
            except Exception:
                errors.append(f"Unable to connect to stream {stream.name}")
                tracebacks.append(traceback.format_exc())
            else:
                if not stream_is_available and reason:
                    errors.append(reason)

        if len(errors) == 1 and len(tracebacks) == 1:
            raise AirbyteTracedException(
                internal_message=tracebacks[0],
                message=f"{errors[0]}",
                failure_type=FailureType.config_error,
            )
        if len(errors) == 1 and len(tracebacks) == 0:
            raise AirbyteTracedException(
                message=f"{errors[0]}",
                failure_type=FailureType.config_error,
            )
        elif len(errors) > 1:
            raise AirbyteTracedException(
                internal_message="\n".join(tracebacks),
                message=f"{len(errors)} streams with errors: {', '.join(error for error in errors)}",
                failure_type=FailureType.config_error,
            )

        return not bool(errors), (errors or None)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Return a list of this source's streams.
        """

        if self.catalog:
            state_manager = ConnectorStateManager(state=self.state)
        else:
            # During `check` operations we don't have a catalog so cannot create a state manager.
            # Since the state manager is only required for incremental syncs, this is fine.
            state_manager = None

        try:
            parsed_config = self._get_parsed_config(config)
            self.stream_reader.config = parsed_config
            if self.stream_permissions_reader:
                self.stream_permissions_reader.config = parsed_config
            streams: List[Stream] = []
            for stream_config in parsed_config.streams:
                # Like state_manager, `catalog_stream` may be None during `check`
                catalog_stream = self._get_stream_from_catalog(stream_config)
                stream_state = (
                    state_manager.get_stream_state(catalog_stream.name, catalog_stream.namespace)
                    if (state_manager and catalog_stream)
                    else None
                )
                self._validate_input_schema(stream_config)

                sync_mode = self._get_sync_mode_from_catalog(stream_config.name)

                if (
                    sync_mode == SyncMode.full_refresh
                    and hasattr(self, "_concurrency_level")
                    and self._concurrency_level is not None
                ):
                    cursor = FileBasedFinalStateCursor(
                        stream_config=stream_config,
                        stream_namespace=None,
                        message_repository=self.message_repository,
                    )
                    stream = FileBasedStreamFacade.create_from_stream(
                        stream=self._make_file_based_stream(
                            stream_config=stream_config,
                            cursor=cursor,
                            parsed_config=parsed_config,
                        ),
                        source=self,
                        logger=self.logger,
                        state=stream_state,
                        cursor=cursor,
                    )

                elif (
                    sync_mode == SyncMode.incremental
                    and issubclass(self.cursor_cls, AbstractConcurrentFileBasedCursor)
                    and hasattr(self, "_concurrency_level")
                    and self._concurrency_level is not None
                ):
                    assert state_manager is not None, (
                        "No ConnectorStateManager was created, but it is required for incremental syncs. This is unexpected. Please contact Support."
                    )

                    cursor = self.cursor_cls(
                        stream_config,
                        stream_config.name,
                        None,
                        stream_state,
                        self.message_repository,
                        state_manager,
                        CursorField(DefaultFileBasedStream.ab_last_mod_col),
                    )
                    stream = FileBasedStreamFacade.create_from_stream(
                        stream=self._make_file_based_stream(
                            stream_config=stream_config,
                            cursor=cursor,
                            parsed_config=parsed_config,
                        ),
                        source=self,
                        logger=self.logger,
                        state=stream_state,
                        cursor=cursor,
                    )
                else:
                    cursor = self.cursor_cls(stream_config)
                    stream = self._make_file_based_stream(
                        stream_config=stream_config,
                        cursor=cursor,
                        parsed_config=parsed_config,
                    )

                streams.append(stream)

            if include_identities_stream(parsed_config):
                identities_stream = self._make_identities_stream()
                streams.append(identities_stream)
            return streams

        except ValidationError as exc:
            raise ConfigValidationError(FileBasedSourceError.CONFIG_VALIDATION_ERROR) from exc

    def _make_default_stream(
        self,
        stream_config: FileBasedStreamConfig,
        cursor: Optional[AbstractFileBasedCursor],
        parsed_config: AbstractFileBasedSpec,
    ) -> AbstractFileBasedStream:
        return DefaultFileBasedStream(
            config=stream_config,
            catalog_schema=self.stream_schemas.get(stream_config.name),
            stream_reader=self.stream_reader,
            availability_strategy=self.availability_strategy,
            discovery_policy=self.discovery_policy,
            parsers=self.parsers,
            validation_policy=self._validate_and_get_validation_policy(stream_config),
            errors_collector=self.errors_collector,
            cursor=cursor,
            use_file_transfer=use_file_transfer(parsed_config),
            preserve_directory_structure=preserve_directory_structure(parsed_config),
        )

    def _ensure_permissions_reader_available(self) -> None:
        """
        Validates that a stream permissions reader is available.
        Raises a ValueError if the reader is not provided.
        """
        if not self.stream_permissions_reader:
            raise ValueError(
                "Stream permissions reader is required for streams that use permissions transfer mode."
            )

    def _make_permissions_stream(
        self, stream_config: FileBasedStreamConfig, cursor: Optional[AbstractFileBasedCursor]
    ) -> AbstractFileBasedStream:
        """
        Creates a stream that reads permissions from files.
        """
        self._ensure_permissions_reader_available()
        return PermissionsFileBasedStream(
            config=stream_config,
            catalog_schema=self.stream_schemas.get(stream_config.name),
            stream_reader=self.stream_reader,
            availability_strategy=self.availability_strategy,
            discovery_policy=self.discovery_policy,
            parsers=self.parsers,
            validation_policy=self._validate_and_get_validation_policy(stream_config),
            errors_collector=self.errors_collector,
            cursor=cursor,
            stream_permissions_reader=self.stream_permissions_reader,  # type: ignore
        )

    def _make_file_based_stream(
        self,
        stream_config: FileBasedStreamConfig,
        cursor: Optional[AbstractFileBasedCursor],
        parsed_config: AbstractFileBasedSpec,
    ) -> AbstractFileBasedStream:
        """
        Creates different streams depending on the type of the transfer mode selected
        """
        if use_permissions_transfer(parsed_config):
            return self._make_permissions_stream(stream_config, cursor)
        # we should have a stream for File transfer mode to decouple from DefaultFileBasedStream
        else:
            return self._make_default_stream(stream_config, cursor, parsed_config)

    def _make_identities_stream(
        self,
    ) -> Stream:
        self._ensure_permissions_reader_available()
        return FileIdentitiesStream(
            catalog_schema=self.stream_schemas.get(FileIdentitiesStream.IDENTITIES_STREAM_NAME),
            stream_permissions_reader=self.stream_permissions_reader,  # type: ignore
            discovery_policy=self.discovery_policy,
            errors_collector=self.errors_collector,
        )

    def _get_stream_from_catalog(
        self, stream_config: FileBasedStreamConfig
    ) -> Optional[AirbyteStream]:
        if self.catalog:
            for stream in self.catalog.streams or []:
                if stream.stream.name == stream_config.name:
                    return stream.stream
        return None

    def _get_sync_mode_from_catalog(self, stream_name: str) -> Optional[SyncMode]:
        if self.catalog:
            for catalog_stream in self.catalog.streams:
                if stream_name == catalog_stream.stream.name:
                    return catalog_stream.sync_mode
            self.logger.warning(f"No sync mode was found for {stream_name}.")
        return None

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        yield from super().read(logger, config, catalog, state)
        # emit all the errors collected
        yield from self.errors_collector.yield_and_raise_collected()
        # count streams using a certain parser
        parsed_config = self._get_parsed_config(config)
        for parser, count in Counter(
            stream.format.filetype for stream in parsed_config.streams
        ).items():
            yield create_analytics_message(f"file-cdk-{parser}-stream-count", count)

    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        """
        Returns the specification describing what fields can be configured by a user when setting up a file-based source.
        """

        return ConnectorSpecification(
            documentationUrl=self.spec_class.documentation_url(),
            connectionSpecification=self.spec_class.schema(),
        )

    def _get_parsed_config(self, config: Mapping[str, Any]) -> AbstractFileBasedSpec:
        return self.spec_class(**config)

    def _validate_and_get_validation_policy(
        self, stream_config: FileBasedStreamConfig
    ) -> AbstractSchemaValidationPolicy:
        if stream_config.validation_policy not in self.validation_policies:
            # This should never happen because we validate the config against the schema's validation_policy enum
            raise ValidationError(
                f"`validation_policy` must be one of {list(self.validation_policies.keys())}",
                model=FileBasedStreamConfig,
            )
        return self.validation_policies[stream_config.validation_policy]

    def _validate_input_schema(self, stream_config: FileBasedStreamConfig) -> None:
        if stream_config.schemaless and stream_config.input_schema:
            raise ValidationError(
                "`input_schema` and `schemaless` options cannot both be set",
                model=FileBasedStreamConfig,
            )
