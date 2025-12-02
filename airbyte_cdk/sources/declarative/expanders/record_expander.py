#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class RecordExpander:
    """Expands a single record into multiple records.
    
    Implementations of this class can take one input record and expand it into multiple output records.
    This is useful for extracting items from nested arrays and emitting each item as a separate record.
    
    Example use case: An API returns an invoice with nested line items. The expander can extract each
    line item and emit it as a separate record, optionally preserving context from the parent invoice.
    """

    @abstractmethod
    def expand(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> List[Dict[str, Any]]:
        """Expand a single record into multiple records.
        
        Returns:
            List of expanded records. If expansion is not applicable, should return a list containing
            the original record.
        """

    def __eq__(self, other: object) -> bool:
        return other.__dict__ == self.__dict__
