#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, List, Mapping, Optional

from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.types import StreamSlice, StreamState


@dataclass
class UnionPartitionRouter(PartitionRouter):
    """
    A partition router that yields the deduplicated union of its child partition routers' slices.

    Each emitted slice's partition is normalized to exactly `{partition_field: value}`. Any other
    partition keys coming from a child router (e.g. a SubstreamPartitionRouter's `parent_slice`)
    are moved into `extra_fields` so that per-partition state keys stay stable across children.

    Given two child routers producing:
    A: [{"repository": "org/a"}, {"repository": "org/b"}]
    B: [{"repository": "org/b", "parent_slice": {...}}, {"repository": "org/c", "parent_slice": {...}}]
    the union yields:
    [{"repository": "org/a"}, {"repository": "org/b"}, {"repository": "org/c"}]

    Attributes:
        partition_routers (List[PartitionRouter]): The child partition routers to union.
        partition_field (str): The single partition key all child slices are normalized to.
    """

    partition_routers: List[PartitionRouter]
    partition_field: str
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    def stream_slices(self) -> Iterable[StreamSlice]:
        """
        Iterate over the child partition routers in order, yielding each partition value once.

        The first occurrence of a partition value wins; later duplicates from any child are skipped.
        """
        seen: set[Any] = set()
        for router in self.partition_routers:
            for stream_slice in router.stream_slices():
                if self.partition_field not in stream_slice.partition:
                    raise ValueError(
                        f"UnionPartitionRouter expects all child partition routers to emit the "
                        f"partition field '{self.partition_field}'. Got {stream_slice.partition}"
                    )
                value = stream_slice.partition[self.partition_field]
                if value in seen:
                    continue
                seen.add(value)
                carried_over_fields = {
                    key: field_value
                    for key, field_value in stream_slice.partition.items()
                    if key != self.partition_field
                }
                yield StreamSlice(
                    partition={self.partition_field: value},
                    cursor_slice={},
                    extra_fields={
                        **(dict(stream_slice.extra_fields) if stream_slice.extra_fields else {}),
                        **carried_over_fields,
                    },
                )

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_stream_state(self) -> Optional[Mapping[str, StreamState]]:
        """
        Merge the parent stream states of all child partition routers.

        States are merged with a shallow dict update, which assumes no two child routers
        reference a parent stream with the same name. If they do, the last child's state
        for that parent wins.
        """
        merged_state: dict[str, StreamState] = {}
        for router in self.partition_routers:
            child_state = router.get_stream_state()
            if child_state:
                merged_state.update(child_state)
        return merged_state or None
