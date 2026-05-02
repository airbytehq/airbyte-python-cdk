#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import unittest
from datetime import datetime
from unittest.mock import Mock, PropertyMock

import pytest

from airbyte_cdk import AirbyteTracedException, FailureType
from airbyte_cdk.sources.file_based.availability_strategy.default_file_based_availability_strategy import (
    DefaultFileBasedAvailabilityStrategy,
)
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.jsonl_format import JsonlFormat
from airbyte_cdk.sources.file_based.exceptions import CustomFileBasedException
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.stream import AbstractFileBasedStream

_FILE_WITH_UNKNOWN_EXTENSION = RemoteFile(
    uri="a.unknown_extension", last_modified=datetime.now(), file_type="csv"
)
_ANY_CONFIG = FileBasedStreamConfig(
    name="config.name",
    file_type="parquet",
    format=JsonlFormat(),
)
_ANY_SCHEMA = {"key": "value"}


class DefaultFileBasedAvailabilityStrategyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._strategy = DefaultFileBasedAvailabilityStrategy(self._stream_reader)

        self._parser = Mock(spec=FileTypeParser)
        self._parser.check_config.return_value = (True, None)
        self._stream = Mock(spec=AbstractFileBasedStream)
        self._stream.get_parser.return_value = self._parser
        self._stream.catalog_schema = _ANY_SCHEMA
        self._stream.config = _ANY_CONFIG
        self._stream.validation_policy = PropertyMock(validate_schema_before_sync=False)
        self._stream.stream_reader = self._stream_reader

    def test_given_file_extension_does_not_match_when_check_availability_and_parsability_then_stream_is_still_available(
        self,
    ) -> None:
        """
        Before, we had a validation on the file extension but it turns out that in production, users sometimes have mismatch there. The
        example we've seen was for JSONL parser but the file extension was just `.json`. Note that there we more than one record extracted
        from this stream so it's not just that the file is one JSON object
        """
        self._stream.get_files.return_value = [_FILE_WITH_UNKNOWN_EXTENSION]
        self._parser.parse_records.return_value = [{"a record": 1}]

        is_available, reason = self._strategy.check_availability_and_parsability(
            self._stream, Mock(), Mock()
        )

        assert is_available

    def test_not_available_given_no_files(self) -> None:
        """
        If no files are returned, then the stream is not available, and the
        reason is the actionable `EMPTY_STREAM` message rather than a Python
        traceback.
        """
        self._stream.get_files.return_value = []

        is_available, reason = self._strategy.check_availability_and_parsability(
            self._stream, Mock(), Mock()
        )

        assert not is_available
        assert "No files were identified in the stream" in reason
        assert "Traceback" not in reason
        assert "raise CheckAvailabilityError" not in reason

    def test_check_availability_returns_actionable_reason_when_no_files(self) -> None:
        """
        `check_availability` (used by file-transfer / permissions-transfer modes)
        must also return the actionable reason rather than a traceback string.
        """
        self._stream.get_files.return_value = []

        is_available, reason = self._strategy.check_availability(self._stream, Mock(), Mock())

        assert not is_available
        assert "No files were identified in the stream" in reason
        assert "Traceback" not in reason
        assert "raise CheckAvailabilityError" not in reason

    def test_airbyte_traced_exception_from_stream_reader_propagates(self) -> None:
        """
        When the underlying stream reader raises an `AirbyteTracedException`
        (e.g. invalid credentials), the actionable message must propagate
        unchanged instead of being wrapped in a generic `ERROR_LISTING_FILES`
        reason.
        """
        self._stream.get_files.side_effect = AirbyteTracedException(
            internal_message="raw provider error",
            message="Could not authenticate with Google drive. Please check your credentials.",
            failure_type=FailureType.config_error,
        )

        with pytest.raises(AirbyteTracedException) as exc_info:
            self._strategy.check_availability_and_parsability(self._stream, Mock(), Mock())

        assert (
            exc_info.value.message
            == "Could not authenticate with Google drive. Please check your credentials."
        )
        assert exc_info.value.failure_type == FailureType.config_error

    def test_airbyte_traced_exception_propagates_in_check_availability(self) -> None:
        """
        Same propagation guarantee as above for `check_availability`.
        """
        self._stream.get_files.side_effect = AirbyteTracedException(
            internal_message="raw provider error",
            message="Could not authenticate with Google drive. Please check your credentials.",
            failure_type=FailureType.config_error,
        )

        with pytest.raises(AirbyteTracedException) as exc_info:
            self._strategy.check_availability(self._stream, Mock(), Mock())

        assert (
            exc_info.value.message
            == "Could not authenticate with Google drive. Please check your credentials."
        )

    def test_parse_records_is_not_called_with_parser_max_n_files_for_parsability_set(self) -> None:
        """
        If the stream parser sets parser_max_n_files_for_parsability to 0, then we should not call parse_records on it
        """
        self._parser.parser_max_n_files_for_parsability = 0
        self._stream.get_files.return_value = [_FILE_WITH_UNKNOWN_EXTENSION]

        is_available, reason = self._strategy.check_availability_and_parsability(
            self._stream, Mock(), Mock()
        )

        assert is_available
        assert not self._parser.parse_records.called
        assert self._stream_reader.open_file.called

    def test_passing_config_check(self) -> None:
        """
        Test if the DefaultFileBasedAvailabilityStrategy correctly handles the check_config method defined on the parser.
        """
        self._parser.check_config.return_value = (False, "Ran into error")
        is_available, error_message = self._strategy.check_availability_and_parsability(
            self._stream, Mock(), Mock()
        )
        assert not is_available
        assert "Ran into error" in error_message

    def test_catching_and_raising_custom_file_based_exception(self) -> None:
        """
        Test if the DefaultFileBasedAvailabilityStrategy correctly handles the CustomFileBasedException
        by raising a CheckAvailabilityError when the get_files method is called.
        """
        # Mock the get_files method to raise CustomFileBasedException when called
        self._stream.get_files.side_effect = CustomFileBasedException(
            "Custom exception for testing."
        )

        # Invoke the check_availability_and_parsability method and check if it correctly handles the exception
        is_available, error_message = self._strategy.check_availability_and_parsability(
            self._stream, Mock(), Mock()
        )
        assert not is_available
        assert "Custom exception for testing." in error_message
