#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from io import IOBase
from os import makedirs, path
from typing import Any, Dict, Iterable, List, Optional, Set

from wcmatch.glob import GLOBSTAR, globmatch

from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from airbyte_cdk.sources.file_based.config.validate_config_transfer_modes import (
    include_identities_stream,
    preserve_directory_structure,
    use_file_transfer,
)
from airbyte_cdk.sources.file_based.remote_file import RemoteFile


class FileReadMode(Enum):
    READ = "r"
    READ_BINARY = "rb"


class AbstractFileBasedStreamReader(ABC):
    DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(self) -> None:
        self._config = None

    @property
    def config(self) -> Optional[AbstractFileBasedSpec]:
        return self._config

    @config.setter
    @abstractmethod
    def config(self, value: AbstractFileBasedSpec) -> None:
        """
        FileBasedSource reads the config from disk and parses it, and once parsed, the source sets the config on its StreamReader.

        Note: FileBasedSource only requires the keys defined in the abstract config, whereas concrete implementations of StreamReader
        will require keys that (for example) allow it to authenticate with the 3rd party.

        Therefore, concrete implementations of AbstractFileBasedStreamReader's config setter should assert that `value` is of the correct
        config type for that type of StreamReader.
        """
        ...

    @abstractmethod
    def open_file(
        self, file: RemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger
    ) -> IOBase:
        """
        Return a file handle for reading.

        Many sources will be able to use smart_open to implement this method,
        for example:

        client = boto3.Session(...)
        return smart_open.open(remote_file.uri, transport_params={"client": client})
        """
        ...

    @abstractmethod
    def get_matching_files(
        self,
        globs: List[str],
        prefix: Optional[str],
        logger: logging.Logger,
    ) -> Iterable[RemoteFile]:
        """
        Return all files that match any of the globs.

        Example:

        The source has files "a.json", "foo/a.json", "foo/bar/a.json"

        If globs = ["*.json"] then this method returns ["a.json"].

        If globs = ["foo/*.json"] then this method returns ["foo/a.json"].

        Utility method `self.filter_files_by_globs` and `self.get_prefixes_from_globs`
        are available, which may be helpful when implementing this method.
        """
        ...

    def filter_files_by_globs_and_start_date(
        self, files: List[RemoteFile], globs: List[str]
    ) -> Iterable[RemoteFile]:
        """
        Utility method for filtering files based on globs.
        """
        start_date = (
            datetime.strptime(self.config.start_date, self.DATE_TIME_FORMAT)
            if self.config and self.config.start_date
            else None
        )
        seen = set()

        for file in files:
            if self.file_matches_globs(file, globs):
                if file.uri not in seen and (not start_date or file.last_modified >= start_date):
                    seen.add(file.uri)
                    yield file

    @abstractmethod
    def file_size(self, file: RemoteFile) -> int:
        """Utility method to get size of the remote file.

        This is required for connectors that will support writing to
        files. If the connector does not support writing files, then the
        subclass can simply `return 0`.
        """
        ...

    @staticmethod
    def file_matches_globs(file: RemoteFile, globs: List[str]) -> bool:
        # Use the GLOBSTAR flag to enable recursive ** matching
        # (https://facelessuser.github.io/wcmatch/wcmatch/#globstar)
        return any(globmatch(file.uri, g, flags=GLOBSTAR) for g in globs)

    @staticmethod
    def get_prefixes_from_globs(globs: List[str]) -> Set[str]:
        """
        Utility method for extracting prefixes from the globs.
        """
        prefixes = {glob.split("*")[0] for glob in globs}
        return set(filter(lambda x: bool(x), prefixes))

    def use_file_transfer(self) -> bool:
        if self.config:
            return use_file_transfer(self.config)
        return False

    def preserve_directory_structure(self) -> bool:
        # fall back to preserve subdirectories if config is not present or incomplete
        if self.config:
            return preserve_directory_structure(self.config)
        return True

    def include_identities_stream(self) -> bool:
        if self.config:
            return include_identities_stream(self.config)
        return False

    @abstractmethod
    def get_file(
        self, file: RemoteFile, local_directory: str, logger: logging.Logger
    ) -> Dict[str, Any]:
        """
        This is required for connectors that will support writing to
        files. It will handle the logic to download,get,read,acquire or
        whatever is more efficient to get a file from the source.

        Args:
               file (RemoteFile): The remote file object containing URI and metadata.
               local_directory (str): The local directory path where the file will be downloaded.
               logger (logging.Logger): Logger for logging information and errors.

           Returns:
               dict: A dictionary containing the following:
                   - "file_url" (str): The absolute path of the downloaded file.
                   - "bytes" (int): The file size in bytes.
                   - "file_relative_path" (str): The relative path of the file for local storage. Is relative to local_directory as
                   this a mounted volume in the pod container.

        """
        ...

    def _get_file_transfer_paths(self, file: RemoteFile, local_directory: str) -> List[str]:
        preserve_directory_structure = self.preserve_directory_structure()
        if preserve_directory_structure:
            # Remove left slashes from source path format to make relative path for writing locally
            file_relative_path = file.uri.lstrip("/")
        else:
            file_relative_path = path.basename(file.uri)
        local_file_path = path.join(local_directory, file_relative_path)

        # Ensure the local directory exists
        makedirs(path.dirname(local_file_path), exist_ok=True)
        absolute_file_path = path.abspath(local_file_path)
        return [file_relative_path, local_file_path, absolute_file_path]

    @abstractmethod
    def get_file_acl_permissions(self, file: RemoteFile, logger: logging.Logger) -> Dict[str, Any]:
        """
        This function should return the allow list for a given file, i.e. the list of all identities and their permission levels associated with it

        e.g.
        def get_file_acl_permissions(self, file: RemoteFile, logger: logging.Logger):
            api_conn = some_api.conn(credentials=SOME_CREDENTIALS)
            result = api_conn.get_file_permissions_info(file.id)
            return MyPermissionsModel(
                id=result["id"],
                access_control_list = result["access_control_list"],
                is_public = result["is_public"],
                ).dict()
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement get_file_acl_permissions(). To support ACL permissions, implement this method and update file_permissions_schema."
        )

    @abstractmethod
    def load_identity_groups(self, logger: logging.Logger) -> Iterable[Dict[str, Any]]:
        """
        This function should return the Identities in a determined "space" or "domain" where the file metadata (ACLs) are fetched and ACLs items (Identities) exists.

        e.g.
        def load_identity_groups(self, logger: logging.Logger) -> Dict[str, Any]:
            api_conn = some_api.conn(credentials=SOME_CREDENTIALS)
            users_api = api_conn.users()
            groups_api = api_conn.groups()
            members_api = self.google_directory_service.members()
            for user in users_api.list():
                yield my_identity_model(id=user.id, name=user.name, email_address=user.email, type="user").dict()
            for group in groups_api.list():
                group_obj = my_identity_model(id=group.id, name=groups.name, email_address=user.email, type="group").dict()
                for member in members_api.list(group=group):
                    group_obj.member_email_addresses = group_obj.member_email_addresses or []
                    group_obj.member_email_addresses.append(member.email)
                yield group_obj.dict()
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement load_identity_groups(). To support identities, implement this method and update identities_schema."
        )

    @property
    @abstractmethod
    def file_permissions_schema(self) -> Dict[str, Any]:
        """
        This function should return the permissions schema for file permissions stream.

        e.g.
        def file_permissions_schema(self) -> Dict[str, Any]:
            # you can also follow the patter we have for python connectors and have a json file and read from there e.g. schemas/identities.json
            return {
                  "type": "object",
                  "properties": {
                    "id": { "type": "string" },
                    "file_path": { "type": "string" },
                    "access_control_list": {
                      "type": "array",
                      "items": { "type": "string" }
                    },
                    "publicly_accessible": { "type": "boolean" }
                  }
                }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement file_permissions_schema, please return json schema for your permissions streams."
        )

    @property
    @abstractmethod
    def identities_schema(self) -> Dict[str, Any]:
        """
        This function should return the identities schema for file identity stream.

        e.g.
        def identities_schema(self) -> Dict[str, Any]:
            # you can also follow the patter we have for python connectors and have a json file and read from there e.g. schemas/identities.json
            return {
              "type": "object",
              "properties": {
                "id": { "type": "string" },
                "remote_id": { "type": "string" },
                "name": { "type": ["null", "string"] },
                "email_address": { "type": ["null", "string"] },
                "member_email_addresses": { "type": ["null", "array"] },
                "type": { "type": "string" },
              }
            }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement identities_schema, please return json schema for your identities stream."
        )
