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


def add_service_key(mapping: Mapping[str, Any], key: str, value: Any) -> dict[str, Any]:
    """
    :param mapping: non-null mapping
    :param key: the name of the key, not including any specific prefixes
    :param value: the value to bind
    :return: a non-null copy of the mappibg including a new key-value pair, where the key is prefixed as service field.
    """
    result = dict(mapping)
    result[SERVICE_KEY_PREFIX + key] = value
    return result


def exclude_service_keys(struct: Any) -> Any:
    """
    :param struct: any object/JSON structure
    :return: a copy of struct without any service fields at any level of nesting
    """
    if isinstance(struct, dict):
        return {k: exclude_service_keys(v) for k, v in struct.items() if not is_service_key(k)}
    elif isinstance(struct, list):
        return [exclude_service_keys(v) for v in struct]
    else:
        return struct


def is_service_key(key: str) -> bool:
    return key.startswith(SERVICE_KEY_PREFIX)


def remove_service_keys(records: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
    for record in records:
        yield exclude_service_keys(record)


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
