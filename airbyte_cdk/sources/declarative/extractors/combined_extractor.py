#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from enum import Enum
from itertools import chain
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Union

import requests

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor


class CombineMode(Enum):
    """
    How a `CombinedExtractor` combines the output of its sub-extractors.
    """

    union = "union"
    first_match = "first_match"
    zip_merge = "zip_merge"


class _NoRecord:
    """Sentinel telling "this extractor yielded nothing" apart from a falsy or `None` record."""


_NO_RECORD = _NoRecord()


@dataclass
class CombinedExtractor(RecordExtractor):
    """Combines the output of several record extractors into a single stream of records.

    A single `DpathExtractor` can only describe one path into the response. Several connectors need
    more than that and reach for a custom `components.py` today, which keeps them out of the
    Connector Builder. This component covers the three shapes those connectors implement:

    - `union` (default) — yield every record of every sub-extractor, in sub-extractor order. Use it
      when one response carries records under several paths, for example a GraphQL document that
      returns `issues.nodes` and `pullRequests.nodes` side by side.
    - `first_match` — yield the records of the first sub-extractor that produces at least one
      record, and skip the remaining sub-extractors. If no sub-extractor produces a record, nothing
      is yielded. Use it when an API answers with one of several alternative shapes. The winning
      sub-extractor is only peeked at, never restarted: the peeked record is chained back in front
      of the remaining ones, so large responses are still streamed lazily and no sub-extractor is
      ever materialized into a list.
    - `zip_merge` — merge the i-th record of every sub-extractor into a single dictionary, with
      later sub-extractors overwriting keys set by earlier ones. Iteration stops at the SHORTEST
      sub-extractor (plain `zip` semantics, not `zip_longest`): records past the end of the
      shortest sequence are dropped rather than padded with empty values. This matches the
      hand-rolled `CombinedExtractor` of `source-google-analytics-data-api`, which this component
      is meant to replace.

    Examples of instantiating this component:
    ```
      extractor:
        type: CombinedExtractor
        mode: union
        extractors:
          - type: DpathExtractor
            field_path: ["data", "repository", "issues", "nodes"]
          - type: DpathExtractor
            field_path: ["data", "repository", "pullRequests", "nodes"]
    ```

    ```
      extractor:
        type: CombinedExtractor
        mode: first_match
        extractors:
          - type: DpathExtractor
            field_path: ["data", "boards", "*", "items_page", "items"]
          - type: DpathExtractor
            field_path: ["data", "next_items_page", "items"]
    ```

    Known limitation - the sub-extractors share one `requests.Response`:
        Every sub-extractor is handed the same response object, so the response body must be
        readable more than once. That holds for the decoders that buffer the whole body, which is
        the common case: `JsonDecoder` and any `CompositeRawDecoder` built with
        `stream_response=False` read `response.content`, `XmlDecoder` reads `response.text` and
        `ZipfileDecoder` reads `response.content` - `requests` caches all of those, so every
        sub-extractor sees the full body.

        It does NOT hold for streaming decoders, i.e. a `CompositeRawDecoder` with
        `stream_response=True`, which is what `CsvDecoder`, `JsonlDecoder`, `JsonItemsDecoder` and
        `GzipDecoder` resolve to outside the Connector Builder, nor for `IterableDecoder`. Those
        read `response.raw` and close it afterwards, so the first sub-extractor drains the stream
        and the later ones see an exhausted (and closed) stream. Note that the Connector Builder
        forces those same decoders to `stream_response=False`, so a manifest that combines
        extractors over a streaming decoder can look fine in the Builder and still lose records in
        production. Do not use `CombinedExtractor` with a streaming decoder.

    Attributes:
        extractors (List[RecordExtractor]): The sub-extractors to combine. At least one is required.
        mode (CombineMode): How the sub-extractor outputs are combined. Defaults to `union`.
    """

    extractors: List[RecordExtractor]
    parameters: InitVar[Mapping[str, Any]]
    mode: CombineMode = CombineMode.union

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.extractors:
            raise ValueError(
                "CombinedExtractor requires at least one extractor in its `extractors` field."
            )
        if not isinstance(self.mode, CombineMode):
            self.mode = CombineMode(self.mode)

    def extract_records(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        if self.mode == CombineMode.first_match:
            yield from self._extract_first_match(response)
        elif self.mode == CombineMode.zip_merge:
            yield from self._extract_zip_merge(response)
        else:
            yield from self._extract_union(response)

    def _extract_union(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        for extractor in self.extractors:
            yield from extractor.extract_records(response)

    def _extract_first_match(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        for extractor in self.extractors:
            records: Iterator[Mapping[str, Any]] = iter(extractor.extract_records(response))
            first_record: Union[Mapping[str, Any], _NoRecord] = next(records, _NO_RECORD)
            if isinstance(first_record, _NoRecord):
                continue
            # Chain the peeked record back instead of restarting the extractor: the rest of the
            # records stay lazy and the response is never read twice.
            yield from chain([first_record], records)
            return

    def _extract_zip_merge(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        # `zip` stops at the shortest sub-extractor, matching the behavior this component replaces.
        for records in zip(*(extractor.extract_records(response) for extractor in self.extractors)):
            merged: Dict[str, Any] = {}
            for record in records:
                merged.update(record)
            yield merged
