# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from airbyte_cdk.sources.declarative.requesters.query_properties.strategies.group_by_key import (
    GroupByKey,
)
from airbyte_cdk.sources.declarative.requesters.query_properties.strategies.merge_strategy import (
    RecordMergeStrategy,
)

__all__ = ["GroupByKey", "RecordMergeStrategy"]
