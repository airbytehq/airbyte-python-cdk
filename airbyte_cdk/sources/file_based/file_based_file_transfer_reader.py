from abc import ABC, abstractmethod

from airbyte_cdk.sources.file_based.remote_file import RemoteFile


class AbstractFileBasedFileTransferReader(ABC):
    FILE_SIZE_LIMIT = 1_500_000_000

    def __init__(self, remote_file: RemoteFile) -> None:
        self.remote_file = remote_file

    @property
    @abstractmethod
    def file_id(self) -> str:
        """
        Unique identifier for the file being transferred.
        """
        ...

    @property
    @abstractmethod
    def file_created_at(self) -> str:
        """
        Date time when the file was created.
        """
        ...

    @property
    @abstractmethod
    def file_updated_at(self) -> str:
        """
        Date time when the file was last updated.
        """
        ...

    @property
    @abstractmethod
    def file_size(self) -> int:
        """
        Returns the file size in bytes.
        """
        ...

    @abstractmethod
    def download_to_local_directory(self, local_file_path: str) -> None:
        """
        Download the file from remote source to local storage.
        """
        ...

    @property
    @abstractmethod
    def source_file_relative_path(self) -> str:
        """
        Returns the relative path of the source file.
        """
        ...

    @property
    def file_uri_for_logging(self) -> str:
        """
        Returns the URI for the file being logged.
        """
        return self.remote_file.uri
