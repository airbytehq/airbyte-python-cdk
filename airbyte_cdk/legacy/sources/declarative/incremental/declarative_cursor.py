# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import datetime
from abc import ABC
from typing import Any, Mapping, Optional

from airbyte_cdk.sources.declarative.stream_slicers.stream_slicer import StreamSlicer
from airbyte_cdk.sources.streams.checkpoint.cursor import Cursor


class DeclarativeCursor(Cursor, StreamSlicer, ABC):
    """
    DeclarativeCursors are components that allow for checkpointing syncs. In addition to managing the fetching and updating of
    state, declarative cursors also manage stream slicing and injecting slice values into outbound requests.
    """

    def get_cursor_datetime_from_state(
        self, stream_state: Mapping[str, Any]
    ) -> Optional[datetime.datetime]:
        """Extract and parse the cursor datetime from the given stream state.

        This method is used by StateDelegatingStream to validate cursor age against
        an API's data retention period. Subclasses should implement this method to
        extract the cursor value from their specific state structure and parse it
        into a datetime object.

        Returns None if the cursor cannot be extracted or parsed, which will cause
        StateDelegatingStream to fall back to full refresh (safe default).

        Raises NotImplementedError by default - subclasses must implement this method
        if they want to support cursor age validation with api_retention_period.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement get_cursor_datetime_from_state. "
            f"Cursor age validation with api_retention_period is not supported for this cursor type."
        )
