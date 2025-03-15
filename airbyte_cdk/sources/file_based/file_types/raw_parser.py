#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import SchemaType


class RawParser(FileTypeParser):
    """
    A parser that doesn't actually parse files. It's designed to be used with the "Copy Raw Files" delivery method.
    """

    @property
    def parser_max_n_files_for_schema_inference(self) -> Optional[int]:
        """
        Just check one file as the schema is static
        """
        return 1

    @property
    def parser_max_n_files_for_parsability(self) -> Optional[int]:
        """
        Do not check any files for parsability since we're not actually parsing them
        """
        return 0

    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        """
        Verify that this parser is only used with the "Copy Raw Files" delivery method.
        """
        from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
        from airbyte_cdk.sources.file_based.config.validate_config_transfer_modes import use_file_transfer

        # Create a mock config to check if the delivery method is set to use file transfer
        mock_config = type('MockConfig', (AbstractFileBasedSpec,), {
            'delivery_method': config.source_config.delivery_method,
            'documentation_url': staticmethod(lambda: ""),
        })()

        if not use_file_transfer(mock_config):
            return False, "The 'Raw Files' parser can only be used with the 'Copy Raw Files' delivery method."
        
        return True, None

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        """
        Return a minimal schema since we're not actually parsing the files.
        """
        return {}

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]],
    ) -> Iterable[Dict[str, Any]]:
        """
        This method should never be called since we're using the "Copy Raw Files" delivery method.
        """

        # This is a safeguard in case this method is called
        # Since we're not actually parsing files, just return an empty iterator
        # The validation that this format is only used with "Copy Raw Files" delivery method
        # will be handled at a higher level in the availability strategy
        # Return an empty iterable
        return iter([])

    @property
    def file_read_mode(self) -> FileReadMode:
        """
        We don't actually read the files, but if we did, we'd use binary mode.
        """
        return FileReadMode.READ_BINARY
