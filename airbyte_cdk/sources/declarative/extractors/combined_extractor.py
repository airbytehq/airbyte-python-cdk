#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
from dataclasses import InitVar, dataclass
from enum import Enum
from itertools import chain
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Sequence, Union

import requests

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor

logger = logging.getLogger("airbyte")


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
      shortest sequence are dropped rather than padded with empty values, and a warning is logged
      when that happens. This matches the hand-rolled `CombinedExtractor` of
      `source-google-analytics-data-api`, which this component is meant to replace.

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

    ## Streaming decoders are rejected

    Every sub-extractor is handed the same `requests.Response`, so the response body must be
    readable more than once. That holds for the decoders that buffer the whole body, which is the
    common case: `JsonDecoder` and any `CompositeRawDecoder` built with `stream_response=False`
    read `response.content`, `XmlDecoder` reads `response.text` and `ZipfileDecoder` reads
    `response.content` — `requests` caches all of those, so every sub-extractor sees the full body.

    It does NOT hold for streaming decoders, i.e. a `CompositeRawDecoder` with
    `stream_response=True`, which is what `CsvDecoder`, `JsonlDecoder`, `JsonItemsDecoder` and
    `GzipDecoder` resolve to outside the Connector Builder, nor for `IterableDecoder`. Those read
    `response.raw` and close it afterwards, so the first sub-extractor drains the stream and the
    later ones read a dead body. That loss is SILENT rather than loud: `requests` puts a
    `urllib3.HTTPResponse` in `response.raw`, and reading a closed `urllib3.HTTPResponse` returns
    an empty body instead of raising, so `union` emits only the first sub-extractor's records and
    `first_match` emits nothing at all when the first path misses.

    Because of that, `ModelToComponentFactory.create_combined_extractor` refuses to build this
    component over a streaming decoder and raises a configuration error naming the decoder. The
    Connector Builder forces those same decoders to `stream_response=False`, so the rejection is
    applied there too rather than letting a manifest test-read correctly and lose records once
    published.

    ## Cost

    Each sub-extractor decodes the response independently — `CompositeRawDecoder` re-parses the
    cached body on every `decode()` call — so a response is parsed once per sub-extractor. Under
    `zip_merge` all sub-extractor outputs are also alive at the same time. Combining three
    extractors over a large response costs roughly three times the decode time of a single one.

    Note that an `OffsetIncrement` or `PageIncrement` paginator builds its own copy of the
    extractor to count the records of a page, which doubles that cost, and the count it obtains is
    the combined count: the winner's record count under `first_match` and the shortest
    sub-extractor's count under `zip_merge`, neither of which is the API's page size.

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
        # Iteration stops at the shortest sub-extractor, matching the behavior this component
        # replaces. Unlike plain `zip`, the round in which a sub-extractor runs out is completed so
        # that an uneven number of records can be reported instead of silently truncated.
        iterators = [iter(extractor.extract_records(response)) for extractor in self.extractors]
        emitted = 0
        while True:
            round_records = [next(iterator, _NO_RECORD) for iterator in iterators]
            exhausted = [
                index for index, record in enumerate(round_records) if isinstance(record, _NoRecord)
            ]
            if exhausted:
                if len(exhausted) != len(iterators):
                    logger.warning(
                        "CombinedExtractor in `zip_merge` mode discarded the records of the "
                        "sub-extractors that outlasted the shortest one: sub-extractors "
                        "%s ran out after %s record(s) while the others still had records. "
                        "`zip_merge` stops at the shortest sub-extractor.",
                        exhausted,
                        emitted,
                    )
                return
            yield self._merge(round_records)
            emitted += 1

    def _merge(self, records: Sequence[Any]) -> Mapping[str, Any]:
        merged: Dict[str, Any] = {}
        for index, record in enumerate(records):
            if not isinstance(record, Mapping):
                raise ValueError(
                    f"CombinedExtractor in `zip_merge` mode can only merge records that are "
                    f"objects, but sub-extractor {index} "
                    f"({type(self.extractors[index]).__name__}) yielded a "
                    f"{type(record).__name__}. Point that sub-extractor at a path that holds "
                    f"objects, or use the `union` mode."
                )
            merged.update(record)
        return merged
