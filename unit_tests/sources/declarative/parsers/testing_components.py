#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass, field
from typing import ClassVar, List, Optional, Any, Mapping

from airbyte_cdk.sources.declarative.extractors import DpathExtractor
from airbyte_cdk.sources.declarative.migrations.state_migration import StateMigration
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


class TestingStateMigration(StateMigration):
    def should_migrate(self, stream_state: Mapping[str, Any]) -> bool:
        return True

    def migrate(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        updated_at = stream_state["updated_at"]
        return {
            "states": [
                {
                    "partition": {"type": "type_1"},
                    "cursor": {"updated_at": updated_at},
                },
                {
                    "partition": {"type": "type_2"},
                    "cursor": {"updated_at": updated_at},
                },
            ]
        }


class TestingStateMigrationWithParentState(StateMigration):
    def should_migrate(self, stream_state: Mapping[str, Any]) -> bool:
        return True

    def migrate(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        stream_state["lookback_window"] = 20
        stream_state["parent_state"]["parent_stream"] = {"updated_at": "2024-02-01T00:00:00.000000+00:00"}
        return stream_state
