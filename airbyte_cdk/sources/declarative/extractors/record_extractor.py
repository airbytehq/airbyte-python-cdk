#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import requests

# Convention:
# - The record extractors may leave service fields bound in the extracted records (mappings).
# - The names (keys) of the service fields have the value of SERVICE_KEY_PREFIX as their prefix.
# - The service fields are kept only during the record's filtering and transformation.
SERVICE_KEY_PREFIX = "$"


def exclude_service_keys(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
    return {k: v for k, v in mapping.items() if not is_service_key(k)}


def remove_service_keys(mapping: dict[str, Any]):  # type: ignore[no-untyped-def]
    """
    Modify the parameter by removing the service keys from it.
    """
    for key in list(mapping.keys()):
        if is_service_key(key):
            mapping.pop(key)


def is_service_key(key: str) -> bool:
    return key.find(SERVICE_KEY_PREFIX) == 0


def assert_service_keys_exist(mapping: Mapping[str, Any]):  # type: ignore[no-untyped-def]
    assert mapping != exclude_service_keys(mapping), "The mapping should contain service keys"


@dataclass
class RecordExtractor:
    """
    Responsible for translating an HTTP response into a list of records by extracting records from the response.
    """

    @abstractmethod
    def extract_records(
        self,
        response: requests.Response,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Selects records from the response
        :param response: The response to extract the records from
        :return: List of Records extracted from the response
        """
        pass
