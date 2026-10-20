#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from airbyte_cdk.models import AirbyteStream, FailureType, SyncMode
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    StreamSlicerPartitionGenerator,
)
from airbyte_cdk.sources.message import InMemoryMessageRepository
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor, CursorField, FinalStateCursor
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.partition_generator import PartitionGenerator
from airbyte_cdk.sources.types import Record
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


class ThreadBasedConcurrentStreamTest(unittest.TestCase):
    def setUp(self):
        self._partition_generator = Mock(spec=PartitionGenerator)
        self._partition = Mock(spec=Partition)
        self._name = "name"
        self._json_schema = {}
        self._primary_key = []
        self._cursor_field = None
        self._logger = Mock()
        self._cursor = Mock(spec=Cursor)
        self._message_repository = InMemoryMessageRepository()
        self._stream = DefaultStream(
            self._partition_generator,
            self._name,
            self._json_schema,
            self._primary_key,
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

    def test_get_json_schema(self):
        json_schema = self._stream.get_json_schema()
        assert json_schema == self._json_schema

    def test_json_schema_is_callable(self):
        expected = {"schema": "is callable"}
        json_schema_callable = lambda: expected
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema_callable,
            self._primary_key,
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        result = stream.get_json_schema()

        assert result == expected

    def test_check_for_error_raises_an_exception_if_any_of_the_futures_are_not_done(self):
        futures = [Mock() for _ in range(3)]
        for f in futures:
            f.exception.return_value = None
        futures[0].done.return_value = False

        with self.assertRaises(Exception):
            self._stream._check_for_errors(futures)

    def test_check_for_error_raises_an_exception_if_any_of_the_futures_raised_an_exception(self):
        futures = [Mock() for _ in range(3)]
        for f in futures:
            f.exception.return_value = None
        futures[0].exception.return_value = Exception("error")

        with self.assertRaises(Exception):
            self._stream._check_for_errors(futures)

    def test_as_airbyte_stream(self):
        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=self._json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=None,
            namespace=None,
            is_resumable=False,
            is_file_based=False,
        )
        actual_airbyte_stream = self._stream.as_airbyte_stream()

        assert actual_airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_primary_key(self):
        json_schema = {
            "type": "object",
            "properties": {
                "id_a": {"type": ["null", "string"]},
                "id_b": {"type": ["null", "string"]},
            },
        }
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema,
            ["composite_key_1", "composite_key_2"],
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=[["composite_key_1"], ["composite_key_2"]],
            namespace=None,
            is_resumable=False,
            is_file_based=False,
        )

        airbyte_stream = stream.as_airbyte_stream()
        assert airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_composite_primary_key(self):
        json_schema = {
            "type": "object",
            "properties": {
                "id_a": {"type": ["null", "string"]},
                "id_b": {"type": ["null", "string"]},
            },
        }
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema,
            ["id_a", "id_b"],
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=[["id_a"], ["id_b"]],
            namespace=None,
            is_resumable=False,
            is_file_based=False,
        )

        airbyte_stream = stream.as_airbyte_stream()
        assert airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_a_cursor(self):
        json_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "date": {"type": ["null", "string"]},
            },
        }
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema,
            self._primary_key,
            CursorField(cursor_field_key="date"),
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            source_defined_cursor=True,
            default_cursor_field=["date"],
            source_defined_primary_key=None,
            namespace=None,
            is_resumable=True,
            is_file_based=False,
        )

        airbyte_stream = stream.as_airbyte_stream()
        assert airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_namespace(self):
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            self._json_schema,
            self._primary_key,
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
            namespace="test",
        )
        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=self._json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=None,
            namespace="test",
            is_resumable=False,
            is_file_based=False,
        )
        actual_airbyte_stream = stream.as_airbyte_stream()

        assert actual_airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_file_transfer_support(self):
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            self._json_schema,
            self._primary_key,
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
            namespace="test",
            supports_file_transfer=True,
        )
        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=self._json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=None,
            namespace="test",
            is_resumable=False,
            is_file_based=True,
        )
        actual_airbyte_stream = stream.as_airbyte_stream()

        assert actual_airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_a_catalog_defined_cursor(self):
        json_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "date": {"type": ["null", "string"]},
            },
        }
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema,
            self._primary_key,
            CursorField(cursor_field_key="date", supports_catalog_defined_cursor_field=True),
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            source_defined_cursor=False,
            default_cursor_field=["date"],
            source_defined_primary_key=None,
            namespace=None,
            is_resumable=True,
            is_file_based=False,
        )

        airbyte_stream = stream.as_airbyte_stream()
        assert airbyte_stream == expected_airbyte_stream

    def test_given_no_partitions_when_get_availability_then_unavailable(self) -> None:
        self._partition_generator.generate.return_value = []

        availability = self._stream.check_availability()

        assert availability.is_available == False
        assert "no stream slices were found" in availability.reason

    def test_given_AirbyteTracedException_when_generating_partitions_when_get_availability_then_unavailable(
        self,
    ) -> None:
        error_message = "error while generating partitions"
        self._partition_generator.generate.side_effect = AirbyteTracedException(
            message=error_message
        )

        availability = self._stream.check_availability()

        assert availability.is_available == False
        assert error_message in availability.reason

    def test_given_unknown_error_when_generating_partitions_when_get_availability_then_raise(
        self,
    ) -> None:
        """
        I'm not sure why we handle AirbyteTracedException but not other exceptions but this is to keep feature compatibility with HttpAvailabilityStrategy
        """
        self._partition_generator.generate.side_effect = ValueError()
        with pytest.raises(ValueError):
            self._stream.check_availability()

    def test_given_no_records_when_get_availability_then_available(self) -> None:
        self._partition_generator.generate.return_value = [self._partition]
        self._partition.read.return_value = []

        availability = self._stream.check_availability()

        assert availability.is_available == True

    def test_given_records_when_get_availability_then_available(self) -> None:
        self._partition_generator.generate.return_value = [self._partition]
        self._partition.read.return_value = [Mock(spec=Record)]

        availability = self._stream.check_availability()

        assert availability.is_available == True

    def test_given_AirbyteTracedException_when_reading_records_when_get_availability_then_unavailable(
        self,
    ) -> None:
        self._partition_generator.generate.return_value = [self._partition]
        error_message = "error while reading records"
        self._partition.read.side_effect = AirbyteTracedException(message=error_message)

        availability = self._stream.check_availability()

        assert availability.is_available == False

    def _stream_with_retriever(self, retriever):
        partition_generator = Mock(spec=StreamSlicerPartitionGenerator)
        partition_generator._partition_factory = SimpleNamespace(_retriever=retriever)
        partition_generator.generate.return_value = [self._partition]
        return DefaultStream(
            partition_generator,
            self._name,
            self._json_schema,
            self._primary_key,
            self._cursor_field,
            self._logger,
            self._cursor,
        ), partition_generator

    def test_check_availability_temporarily_enables_exit_on_rate_limit(self):
        for use_requester in [False, True]:
            for previous_value in [False, True]:
                with self.subTest(use_requester=use_requester, previous_value=previous_value):
                    rate_limited_component = SimpleNamespace(exit_on_rate_limit=previous_value)
                    retriever = (
                        SimpleNamespace(requester=rate_limited_component)
                        if use_requester
                        else rate_limited_component
                    )
                    stream, partition_generator = self._stream_with_retriever(retriever)

                    def generate():
                        assert rate_limited_component.exit_on_rate_limit is True
                        return [self._partition]

                    def read():
                        assert rate_limited_component.exit_on_rate_limit is True
                        return [Mock(spec=Record)]

                    partition_generator.generate.side_effect = generate
                    self._partition.read.side_effect = read

                    availability = stream.check_availability()

                    assert availability.is_available is True
                    assert rate_limited_component.exit_on_rate_limit is previous_value

    def test_check_availability_propagates_transient_errors_and_restores_flag(self):
        for failure_type, should_raise in [
            (FailureType.transient_error, True),
            (FailureType.config_error, False),
        ]:
            for raise_during_generation in [False, True]:
                for previous_value in [False, True]:
                    with self.subTest(
                        failure_type=failure_type,
                        raise_during_generation=raise_during_generation,
                        previous_value=previous_value,
                    ):
                        retriever = SimpleNamespace(exit_on_rate_limit=previous_value)
                        stream, partition_generator = self._stream_with_retriever(retriever)
                        error = AirbyteTracedException(
                            message="request failed", failure_type=failure_type
                        )
                        if raise_during_generation:
                            partition_generator.generate.side_effect = error
                        else:
                            self._partition.read.side_effect = error

                        if should_raise:
                            with pytest.raises(AirbyteTracedException) as raised_error:
                                stream.check_availability()
                            assert raised_error.value is error
                        else:
                            availability = stream.check_availability()
                            assert availability.is_available is False
                            assert availability.reason == "request failed"

                        assert retriever.exit_on_rate_limit is previous_value

    def test_given_unknown_error_when_reading_record_when_get_availability_then_raise(self) -> None:
        """
        I'm not sure why we handle AirbyteTracedException but not other exceptions but this is to keep feature compatibility with HttpAvailabilityStrategy
        """
        self._partition_generator.generate.return_value = [self._partition]
        self._partition.read.side_effect = ValueError()
        with pytest.raises(ValueError):
            self._stream.check_availability()
