#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable

from airbyte_cdk.sources.file_based.remote_file import RemoteFile


class AbstractFileBasedStreamPermissionsReader(ABC):
    """
    This class is responsible for reading file permissions and Identities from a source.
    """

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
