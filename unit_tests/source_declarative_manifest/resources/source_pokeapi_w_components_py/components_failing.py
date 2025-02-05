#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
"""A sample implementation of custom components that does nothing but will cause syncs to fail if missing."""

from collections.abc import Iterable, MutableMapping
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

import requests

from airbyte_cdk.sources.declarative.extractors import DpathExtractor


class IntentionalException(Exception):
    """This exception is raised intentionally in order to test error handling."""


class MyCustomExtractor(DpathExtractor):
    def extract_records(
        self,
        response: requests.Response,
    ) -> Iterable[MutableMapping[Any, Any]]:
        raise IntentionalException
