#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass, field
from typing import (
    Any,
    Mapping,
)

from typing_extensions import deprecated

from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.source import ExperimentalClassWarning


@deprecated(
    "This class is experimental. Use at your own risk.",
    category=ExperimentalClassWarning,
)
@dataclass
class StateDelegatingRetriever:
    full_data_retriever: Retriever
    incremental_data_retriever: Retriever
    cursor: DeclarativeCursor
    started_with_state: bool = field(init=True, repr=False, default=False)

    def __getattr__(self, name: str) -> Any:
        # Avoid delegation for these internal names.
        if name in {
            "full_data_retriever",
            "incremental_data_retriever",
            "cursor",
            "retriever",
            "state",
        }:
            return object.__getattribute__(self, name)
        # Delegate everything else to the active retriever.
        return getattr(self.retriever, name)

    def __setattr__(self, name: str, value: Any) -> None:
        # For the internal attributes, set them directly on self.
        if name in {
            "full_data_retriever",
            "incremental_data_retriever",
            "cursor",
            "state",
            "started_with_state",
        }:
            super().__setattr__(name, value)
        else:
            # Delegate setting attributes to the underlying retriever.
            setattr(self.retriever, name, value)

    @property
    def retriever(self) -> Retriever:
        return (
            self.incremental_data_retriever if self.started_with_state else self.full_data_retriever
        )

    @property
    def state(self) -> Mapping[str, Any]:
        return self.cursor.get_stream_state() if self.cursor else {}

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        """State setter, accept state serialized by state getter."""
        if self.cursor:
            self.cursor.set_initial_state(value)
            self.started_with_state = bool(value)
