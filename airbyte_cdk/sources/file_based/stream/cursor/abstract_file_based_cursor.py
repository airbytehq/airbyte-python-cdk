#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    import logging
    from collections.abc import Iterable, MutableMapping
    from datetime import datetime

    from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
    from airbyte_cdk.sources.file_based.remote_file import RemoteFile
    from airbyte_cdk.sources.file_based.types import StreamState


class AbstractFileBasedCursor(ABC):
    """Abstract base class for cursors used by file-based streams."""

    @abstractmethod
    def __init__(self, stream_config: FileBasedStreamConfig, **kwargs: Any) -> None:  # noqa: ANN401  (any-type)
        """Common interface for all cursors."""
        ...

    @abstractmethod
    def add_file(self, file: RemoteFile) -> None:
        """Add a file to the cursor. This method is called when a file is processed by the stream.
        :param file: The file to add
        """
        ...

    @abstractmethod
    def set_initial_state(self, value: StreamState) -> None:
        """Set the initial state of the cursor. The cursor cannot be initialized at construction time because the stream doesn't know its state yet.
        :param value: The stream state
        """

    @abstractmethod
    def get_state(self) -> MutableMapping[str, Any]:
        """Get the state of the cursor."""
        ...

    @abstractmethod
    def get_start_time(self) -> datetime:
        """Returns the start time of the current sync."""
        ...

    @abstractmethod
    def get_files_to_sync(
        self, all_files: Iterable[RemoteFile], logger: logging.Logger
    ) -> Iterable[RemoteFile]:
        """Given the list of files in the source, return the files that should be synced.
        :param all_files: All files in the source
        :param logger:
        :return: The files that should be synced
        """
        ...
