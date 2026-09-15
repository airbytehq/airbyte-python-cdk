#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
import io
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Union

import pytest
import requests
import urllib3

from airbyte_cdk.sources.declarative.decoders import CompositeRawDecoder, JsonDecoder
from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import JsonLineParser
from airbyte_cdk.sources.declarative.extractors import (
    CombinedExtractor,
    CombineMode,
    DpathExtractor,
)
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor

config = {"field": "issues"}
parameters = {"parameters_field": "issues"}

decoder_json = JsonDecoder(parameters={})

# A GraphQL document that carries records under two sibling paths, which is the shape that
# source-github and source-monday work around with a custom components.py today.
GRAPHQL_BODY = {
    "data": {
        "repository": {
            "issues": {"nodes": [{"id": "I_1", "title": "first issue"}]},
            "pullRequests": {
                "nodes": [{"id": "PR_1", "title": "first pr"}, {"id": "PR_2", "title": "second pr"}]
            },
            "discussions": {"nodes": []},
        }
    }
}

# The GA4 report shape: dimensions and metrics come back as two parallel lists that have to be
# merged position by position.
REPORT_BODY = {
    "rows": [
        {"dimensions": {"date": "20240101"}, "metrics": {"activeUsers": 1}},
        {"dimensions": {"date": "20240102"}, "metrics": {"activeUsers": 2}},
    ]
}


def create_response(body: Union[Dict, List, bytes]) -> requests.Response:
    """A response whose `raw` is a real `urllib3.HTTPResponse`, which is what `requests` builds.

    This matters for the streaming-decoder tests: `io.BytesIO` raises on a read after close, while
    `urllib3.HTTPResponse` returns an empty body, so only the latter reproduces production.
    """
    raw = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    response = requests.Response()
    response.status_code = 200
    response.raw = urllib3.HTTPResponse(body=io.BytesIO(raw), preload_content=False, status=200)
    return response


def dpath(*field_path: str, decoder=decoder_json) -> DpathExtractor:
    return DpathExtractor(
        field_path=list(field_path), config=config, decoder=decoder, parameters=parameters
    )


@dataclass
class _CountingExtractor(RecordExtractor):
    """Records how many times it is asked for records, so laziness can be asserted."""

    records: List[Mapping[str, Any]]
    calls: List[str] = field(default_factory=list)

    def extract_records(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        self.calls.append("extract_records")
        yield from self.records


def test_union_yields_every_record_in_sub_extractor_order():
    extractor = CombinedExtractor(
        extractors=[
            dpath("data", "repository", "pullRequests", "nodes"),
            dpath("data", "repository", "issues", "nodes"),
        ],
        mode=CombineMode.union,
        parameters=parameters,
    )

    records = list(extractor.extract_records(create_response(GRAPHQL_BODY)))

    assert records == [
        {"id": "PR_1", "title": "first pr"},
        {"id": "PR_2", "title": "second pr"},
        {"id": "I_1", "title": "first issue"},
    ]


def test_union_is_the_default_mode():
    extractor = CombinedExtractor(
        extractors=[
            dpath("data", "repository", "issues", "nodes"),
            dpath("data", "repository", "pullRequests", "nodes"),
        ],
        parameters=parameters,
    )

    assert extractor.mode == CombineMode.union
    assert list(extractor.extract_records(create_response(GRAPHQL_BODY))) == [
        {"id": "I_1", "title": "first issue"},
        {"id": "PR_1", "title": "first pr"},
        {"id": "PR_2", "title": "second pr"},
    ]


def test_union_skips_sub_extractors_that_yield_nothing():
    extractor = CombinedExtractor(
        extractors=[
            dpath("data", "repository", "discussions", "nodes"),
            dpath("data", "repository", "issues", "nodes"),
            dpath("data", "repository", "does_not_exist"),
        ],
        mode=CombineMode.union,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(GRAPHQL_BODY))) == [
        {"id": "I_1", "title": "first issue"}
    ]


def test_first_match_falls_through_an_empty_extractor():
    extractor = CombinedExtractor(
        extractors=[
            dpath("data", "repository", "discussions", "nodes"),
            dpath("data", "repository", "pullRequests", "nodes"),
            dpath("data", "repository", "issues", "nodes"),
        ],
        mode=CombineMode.first_match,
        parameters=parameters,
    )

    records = list(extractor.extract_records(create_response(GRAPHQL_BODY)))

    # The second extractor wins, and the third one is not used at all.
    assert records == [
        {"id": "PR_1", "title": "first pr"},
        {"id": "PR_2", "title": "second pr"},
    ]


def test_first_match_does_not_drop_the_peeked_record():
    """The winner is peeked to find out whether it produced anything; that record must still be emitted."""
    winner = _CountingExtractor(records=[{"id": 1}, {"id": 2}, {"id": 3}])
    extractor = CombinedExtractor(
        extractors=[_CountingExtractor(records=[]), winner],
        mode=CombineMode.first_match,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(GRAPHQL_BODY))) == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
    assert winner.calls == ["extract_records"]


def test_first_match_does_not_touch_extractors_after_the_winner():
    loser = _CountingExtractor(records=[{"id": 99}])
    extractor = CombinedExtractor(
        extractors=[_CountingExtractor(records=[{"id": 1}]), loser],
        mode=CombineMode.first_match,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(GRAPHQL_BODY))) == [{"id": 1}]
    assert loser.calls == []


def test_first_match_yields_nothing_when_every_extractor_is_empty():
    extractor = CombinedExtractor(
        extractors=[
            dpath("data", "repository", "discussions", "nodes"),
            dpath("data", "repository", "does_not_exist"),
        ],
        mode=CombineMode.first_match,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(GRAPHQL_BODY))) == []


def test_zip_merge_merges_the_ith_record_of_every_extractor():
    extractor = CombinedExtractor(
        extractors=[dpath("rows", "*", "dimensions"), dpath("rows", "*", "metrics")],
        mode=CombineMode.zip_merge,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(REPORT_BODY))) == [
        {"date": "20240101", "activeUsers": 1},
        {"date": "20240102", "activeUsers": 2},
    ]


def test_zip_merge_later_extractors_win_on_key_collision():
    extractor = CombinedExtractor(
        extractors=[
            _CountingExtractor(records=[{"id": 1, "source": "first", "only_first": True}]),
            _CountingExtractor(records=[{"id": 1, "source": "second"}]),
        ],
        mode=CombineMode.zip_merge,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(REPORT_BODY))) == [
        {"id": 1, "source": "second", "only_first": True}
    ]


def test_zip_merge_stops_at_the_shortest_sub_extractor():
    """`zip`, not `zip_longest`: the trailing records of the longer extractor are dropped, not padded."""
    extractor = CombinedExtractor(
        extractors=[
            _CountingExtractor(records=[{"a": 1}, {"a": 2}, {"a": 3}]),
            _CountingExtractor(records=[{"b": 1}, {"b": 2}]),
        ],
        mode=CombineMode.zip_merge,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(REPORT_BODY))) == [
        {"a": 1, "b": 1},
        {"a": 2, "b": 2},
    ]


def test_zip_merge_yields_nothing_when_one_extractor_is_empty():
    extractor = CombinedExtractor(
        extractors=[dpath("rows", "*", "dimensions"), dpath("rows", "*", "does_not_exist")],
        mode=CombineMode.zip_merge,
        parameters=parameters,
    )

    assert list(extractor.extract_records(create_response(REPORT_BODY))) == []


@pytest.mark.parametrize(
    "mode",
    [CombineMode.union, CombineMode.first_match, CombineMode.zip_merge],
)
def test_a_single_sub_extractor_behaves_like_that_extractor_alone(mode: CombineMode):
    field_path = ["data", "repository", "pullRequests", "nodes"]
    direct = DpathExtractor(
        field_path=field_path, config=config, decoder=decoder_json, parameters=parameters
    )
    combined = CombinedExtractor(
        extractors=[
            DpathExtractor(
                field_path=field_path, config=config, decoder=decoder_json, parameters=parameters
            )
        ],
        mode=mode,
        parameters=parameters,
    )

    expected = list(direct.extract_records(create_response(GRAPHQL_BODY)))
    assert list(combined.extract_records(create_response(GRAPHQL_BODY))) == expected
    assert expected == [
        {"id": "PR_1", "title": "first pr"},
        {"id": "PR_2", "title": "second pr"},
    ]


def test_an_empty_extractors_list_is_a_configuration_error():
    with pytest.raises(ValueError) as exc_info:
        CombinedExtractor(extractors=[], parameters=parameters)

    assert "extractors" in str(exc_info.value)


def test_mode_can_be_given_as_a_string():
    extractor = CombinedExtractor(
        extractors=[dpath("data", "repository", "issues", "nodes")],
        mode="first_match",
        parameters=parameters,
    )

    assert extractor.mode == CombineMode.first_match


def test_an_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        CombinedExtractor(
            extractors=[dpath("data", "repository", "issues", "nodes")],
            mode="concatenate",
            parameters=parameters,
        )


def test_buffered_decoders_can_be_read_by_every_sub_extractor():
    """`JsonDecoder` reads `response.content`, which `requests` caches, so sharing the response is safe."""
    extractor = CombinedExtractor(
        extractors=[
            dpath("data", "repository", "issues", "nodes"),
            dpath("data", "repository", "pullRequests", "nodes"),
            dpath("data", "repository", "issues", "nodes"),
        ],
        mode=CombineMode.union,
        parameters=parameters,
    )

    assert len(list(extractor.extract_records(create_response(GRAPHQL_BODY)))) == 4


def _streaming_decoder() -> CompositeRawDecoder:
    """What `CsvDecoder`, `JsonlDecoder`, `JsonItemsDecoder` and `GzipDecoder` resolve to in production."""
    return CompositeRawDecoder(parser=JsonLineParser(), stream_response=True)


def test_union_over_a_streaming_decoder_silently_drops_the_later_sub_extractors():
    """Pins the real production behavior, which is silent data loss rather than an error.

    A streaming decoder consumes and closes `response.raw`. Reading a closed
    `urllib3.HTTPResponse` - what `requests` puts there - returns an empty body instead of raising,
    so the second sub-extractor yields nothing and no exception reaches the sync. This is why
    `ModelToComponentFactory.create_combined_extractor` rejects the combination at parse time; see
    `test_combined_extractor_over_a_streaming_decoder_is_rejected` in the factory tests.
    """
    streaming_decoder = _streaming_decoder()
    extractor = CombinedExtractor(
        extractors=[
            dpath("data", decoder=streaming_decoder),
            dpath("data", decoder=streaming_decoder),
        ],
        mode=CombineMode.union,
        parameters=parameters,
    )

    records = list(extractor.extract_records(create_response(b'{"data": [{"id": 1}, {"id": 2}]}')))

    # No exception, and half of the expected four records are gone.
    assert records == [{"id": 1}, {"id": 2}]


def test_first_match_over_a_streaming_decoder_silently_returns_the_wrong_answer():
    """The worst case: `first_match` is built for a first path that misses, and that path drains the body."""
    streaming_decoder = _streaming_decoder()
    body = b'{"data": [{"id": 1}, {"id": 2}]}'

    streaming = CombinedExtractor(
        extractors=[
            dpath("missing", decoder=streaming_decoder),
            dpath("data", decoder=streaming_decoder),
        ],
        mode=CombineMode.first_match,
        parameters=parameters,
    )
    buffered = CombinedExtractor(
        extractors=[dpath("missing"), dpath("data")],
        mode=CombineMode.first_match,
        parameters=parameters,
    )

    assert list(streaming.extract_records(create_response(body))) == []
    assert list(buffered.extract_records(create_response(body))) == [{"id": 1}, {"id": 2}]


def test_zip_merge_warns_when_a_sub_extractor_runs_out_early(caplog):
    extractor = CombinedExtractor(
        extractors=[
            _CountingExtractor(records=[{"a": 1}, {"a": 2}, {"a": 3}]),
            _CountingExtractor(records=[{"b": 1}, {"b": 2}]),
        ],
        mode=CombineMode.zip_merge,
        parameters=parameters,
    )

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        assert list(extractor.extract_records(create_response(REPORT_BODY))) == [
            {"a": 1, "b": 1},
            {"a": 2, "b": 2},
        ]

    assert any(
        "zip_merge" in record.message and record.levelno == logging.WARNING
        for record in caplog.records
    )


def test_zip_merge_does_not_warn_when_every_sub_extractor_has_the_same_length(caplog):
    extractor = CombinedExtractor(
        extractors=[dpath("rows", "*", "dimensions"), dpath("rows", "*", "metrics")],
        mode=CombineMode.zip_merge,
        parameters=parameters,
    )

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        assert len(list(extractor.extract_records(create_response(REPORT_BODY)))) == 2

    assert [record.message for record in caplog.records] == []


def test_zip_merge_names_the_sub_extractor_that_yielded_a_non_object():
    extractor = CombinedExtractor(
        extractors=[
            _CountingExtractor(records=[{"a": 1}]),
            _CountingExtractor(records=["not-an-object"]),
        ],
        mode=CombineMode.zip_merge,
        parameters=parameters,
    )

    with pytest.raises(ValueError) as exc_info:
        list(extractor.extract_records(create_response(REPORT_BODY)))

    message = str(exc_info.value)
    assert "CombinedExtractor" in message
    assert "zip_merge" in message
    assert "sub-extractor 1" in message
    assert "_CountingExtractor" in message
