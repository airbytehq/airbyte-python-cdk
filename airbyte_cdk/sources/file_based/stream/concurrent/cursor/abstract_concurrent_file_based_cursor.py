#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airbyte_cdk.sources.file_based.stream.cursor import AbstractFileBasedCursor
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor


if TYPE_CHECKING:
    import logging
    from collections.abc import Iterable, MutableMapping
    from datetime import datetime

    from airbyte_cdk.sources.file_based.remote_file import RemoteFile
    from airbyte_cdk.sources.file_based.stream.concurrent.adapters import FileBasedStreamPartition
    from airbyte_cdk.sources.file_based.types import StreamState
    from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
    from airbyte_cdk.sources.streams.concurrent.partitions.record import Record


class AbstractConcurrentFileBasedCursor(Cursor, AbstractFileBasedCursor, ABC):
    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401  (any-type)
        pass

    @property
    @abstractmethod
    def state(self) -> MutableMapping[str, Any]: ...

    @abstractmethod
    def observe(self, record: Record) -> None: ...

    @abstractmethod
    def close_partition(self, partition: Partition) -> None: ...

    @abstractmethod
    def set_pending_partitions(self, partitions: list[FileBasedStreamPartition]) -> None: ...

    @abstractmethod
    def add_file(self, file: RemoteFile) -> None: ...

    @abstractmethod
    def get_files_to_sync(
        self, all_files: Iterable[RemoteFile], logger: logging.Logger
    ) -> Iterable[RemoteFile]: ...

    @abstractmethod
    def get_state(self) -> MutableMapping[str, Any]: ...

    @abstractmethod
    def set_initial_state(self, value: StreamState) -> None: ...

    @abstractmethod
    def get_start_time(self) -> datetime: ...

    @abstractmethod
    def emit_state_message(self) -> None: ...

    @abstractmethod
    def ensure_at_least_one_state_emitted(self) -> None: ...
