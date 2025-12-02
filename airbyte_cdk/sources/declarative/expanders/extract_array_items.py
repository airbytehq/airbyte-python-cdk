#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional, Union

import dpath

from airbyte_cdk.sources.declarative.expanders.record_expander import RecordExpander
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class ExtractArrayItems(RecordExpander):
    """Extracts items from a nested array field and emits each as a separate record.
    
    This expander takes a single record containing a nested array and expands it into multiple records,
    one for each item in the array. Optionally, fields from the parent record can be preserved in each
    expanded record.
    
    Example:
        Input record:
        {
            "id": "in_123",
            "created": 1234567890,
            "lines": {
                "data": [
                    {"id": "il_1", "amount": 100},
                    {"id": "il_2", "amount": 200}
                ]
            }
        }
        
        With configuration:
            array_path: ["lines", "data"]
            preserve_parent_fields: ["id", "created"]
            parent_field_prefix: "invoice_"
        
        Output records:
        [
            {"id": "il_1", "amount": 100, "invoice_id": "in_123", "invoice_created": 1234567890},
            {"id": "il_2", "amount": 200, "invoice_id": "in_123", "invoice_created": 1234567890}
        ]
    
    Attributes:
        array_path: Path to the array field to extract items from (e.g., ["lines", "data"])
        preserve_parent_fields: List of field names from the parent record to include in each expanded record
        parent_field_prefix: Optional prefix to add to preserved parent fields (e.g., "invoice_" -> "invoice_id")
    """

    config: Config
    array_path: List[Union[InterpolatedString, str]]
    parameters: InitVar[Mapping[str, Any]]
    preserve_parent_fields: Optional[List[str]] = None
    parent_field_prefix: Optional[str] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters
        self._array_path = []
        
        for path_element in self.array_path:
            if isinstance(path_element, str):
                self._array_path.append(
                    InterpolatedString.create(path_element, parameters=self._parameters)
                )
            else:
                self._array_path.append(path_element)
        
        if self.parent_field_prefix is not None:
            self._parent_field_prefix = InterpolatedString.create(
                self.parent_field_prefix, parameters=self._parameters
            ).eval(self.config)
        else:
            self._parent_field_prefix = None

    def expand(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> List[Dict[str, Any]]:
        """Expand a single record into multiple records by extracting array items.
        
        Returns:
            List of expanded records, one for each item in the array. If the array is not found
            or is empty, returns a list containing the original record.
        """
        path = [path_element.eval(self.config) for path_element in self._array_path]
        
        try:
            array_items = dpath.get(record, path, default=None)
        except (KeyError, TypeError):
            return [record]
        
        if array_items is None or not isinstance(array_items, list):
            return [record]
        
        if len(array_items) == 0:
            return [record]
        
        expanded_records = []
        for item in array_items:
            if not isinstance(item, dict):
                continue
            
            expanded_record = item.copy()
            
            if self.preserve_parent_fields:
                for field_name in self.preserve_parent_fields:
                    if field_name in record:
                        if self._parent_field_prefix:
                            target_field_name = f"{self._parent_field_prefix}{field_name}"
                        else:
                            target_field_name = field_name
                        
                        if target_field_name not in expanded_record:
                            expanded_record[target_field_name] = record[field_name]
            
            expanded_records.append(expanded_record)
        
        if len(expanded_records) == 0:
            return [record]
        
        return expanded_records

    def __eq__(self, other: Any) -> bool:
        return bool(self.__dict__ == other.__dict__)
