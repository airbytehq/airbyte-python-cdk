#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional

import dateparser
import dpath

from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, FieldPointer, StreamSlice, StreamState

logger = logging.getLogger("airbyte")


@dataclass
class DateTimeNormalizer(RecordTransformation):
    """
    A transformation which transform specified datetime fields into RFC3339 format.

    Usage syntax:

    ```yaml
        my_stream:
            <other parameters..>
            transformations:
                - type: DateTimeNormalizer
                  field_pointers:
                    - ["path", "to", "field1"]
                    - ["path2"]
    ```

    Attributes:
        field_pointers (List[FieldPointer]): pointers to the fields that should be removed
    """

    field_pointers: List[FieldPointer]
    parameters: InitVar[Mapping[str, Any]]
    datetime_format: Optional[str] = None

    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> None:
        """
        :param record: The record to be transformed
        :return: the input record with the requested fields removed
        """
        for pointer in self.field_pointers:
            try:
                current_date_time_value = dpath.get(record, pointer)  # type: ignore [arg-type]
                parsed_datetime = self.parse_datetime_to_rfc3339(current_date_time_value)  # type: ignore [arg-type]
                dpath.set(record, pointer, parsed_datetime)
            except dpath.exceptions.PathNotFound:
                # if the (potentially nested) property does not exist, silently skip
                pass

    def parse_datetime_to_rfc3339(self, datetime_value: str) -> str:
        value = dateparser.parse(
            datetime_value,
            # date_format will be used as the main source of format; will remove
            date_formats=[self.datetime_format] if self.datetime_format else None,
            settings={"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True},
        )
        if value:
            return value.isoformat()
        else:
            logger.warning("Could not parse datetime value %s", datetime_value)
            return datetime_value
