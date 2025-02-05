"""A sample implementation of custom components that does nothing but will cause syncs to fail if missing."""

from typing import Any, Mapping

import requests

from airbyte_cdk.sources.declarative.extractors import DpathExtractor


class IntentionalException(Exception):
    """This exception is raised intentionally in order to test error handling."""


class MyCustomExtractor(DpathExtractor):
    """Dummy class, directly implements DPatchExtractor.

    Used to prove that SDM can find the custom class by name.
    """

    pass
