#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass, field
from typing import ClassVar, List, Optional

from airbyte_cdk.sources.declarative.extractors import DpathExtractor
from airbyte_cdk.sources.declarative.partition_routers import SubstreamPartitionRouter
from airbyte_cdk.sources.declarative.requesters import RequestOption
from airbyte_cdk.sources.declarative.requesters.error_handlers import DefaultErrorHandler
from airbyte_cdk.sources.declarative.requesters.paginators import (
    DefaultPaginator,
    PaginationStrategy,
)
from airbyte_cdk.sources.declarative.retrievers import SimpleRetriever


@dataclass
class TestingSomeComponent(DefaultErrorHandler):
    """
    A basic test class with various field permutations used to test manifests with custom components
    """

    __test__: ClassVar[bool] = False  # Tell Pytest this is not a Pytest class, despite its name

    subcomponent_field_with_hint: DpathExtractor = field(
        default_factory=lambda: DpathExtractor(field_path=[], config={}, parameters={})
    )
    basic_field: str = ""
    optional_subcomponent_field: Optional[RequestOption] = None
    list_of_subcomponents: List[RequestOption] = None
    without_hint = None
    paginator: DefaultPaginator = None


@dataclass
class TestingCustomSubstreamPartitionRouter(SubstreamPartitionRouter):
    """
    A test class based on a SubstreamPartitionRouter used for testing manifests that use custom components.
    """

    __test__: ClassVar[bool] = False  # Tell Pytest this is not a Pytest class, despite its name

    custom_field: str
    custom_pagination_strategy: PaginationStrategy


@dataclass
class TestingCustomRetriever(SimpleRetriever):
    pass
