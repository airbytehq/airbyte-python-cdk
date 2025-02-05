"""A sample implementation of custom components that does nothing but will cause syncs to fail if missing."""

from dataclasses import dataclass

from airbyte_cdk.sources.declarative.requesters.paginators import PaginationStrategy
from airbyte_cdk.sources.declarative.retrievers import (
    AsyncRetriever,
    SimpleRetriever,
    SimpleRetrieverTestReadDecorator,
)
from airbyte_cdk.sources.declarative.schema import InlineSchemaLoader


class IntentionalException(Exception):
    """This exception is raised intentionally in order to test error handling."""


@dataclass
class CustomPageIncrement(PaginationStrategy):
    """No op."""


class CustomPaginationStrategy(SimpleRetriever):
    pass


class MyCustomInlineSchemaLoader(InlineSchemaLoader):
    def __init__(self, *args, **kwargs):
        raise IntentionalException
