"""A sample implementation of custom components that does nothing but will cause syncs to fail if missing."""

from collections.abc import Iterable, MutableMapping
from typing import Any

import requests

from airbyte_cdk.sources.declarative.extractors import DpathExtractor


class IntentionalException(Exception):
    """This exception is raised intentionally in order to test error handling."""


class MyCustomExtractor(DpathExtractor):
    """Dummy class, directly implements DPatchExtractor.

    Used to prove that SDM can find the custom class by name.
    """

    pass


class MyCustomFailingExtractor(DpathExtractor):
    """Dummy class, intentionally raises an exception when extract_records is called."""

    def extract_records(
        self,
        response: requests.Response,
    ) -> Iterable[MutableMapping[Any, Any]]:
        """Raise an exception when called."""
        raise IntentionalException("This is an intentional failure for testing purposes.")
