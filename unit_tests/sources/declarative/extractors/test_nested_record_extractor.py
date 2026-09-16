#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
import io
import json
import logging
from types import MappingProxyType
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Union

import pytest
import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk.sources.declarative.extractors.nested_record_extractor import (
    NestedRecordExtractor,
    ParentFieldPath,
)
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

config = {"child_connection": "reviews"}


def create_response(body: Union[Dict, List, bytes]) -> requests.Response:
    response = requests.Response()
    response.raw = io.BytesIO(body if isinstance(body, bytes) else json.dumps(body).encode("utf-8"))
    return response


def dpath(*field_path: str, parameters: Mapping[str, Any] = {}) -> DpathExtractor:
    return DpathExtractor(field_path=list(field_path), config=config, parameters=parameters)


def nested(
    parent_extractor: RecordExtractor,
    child_field_path: List[str],
    parent_fields: Union[List[Dict[str, List[str]]], None] = None,
    parameters: Mapping[str, Any] = {},
) -> NestedRecordExtractor:
    return NestedRecordExtractor(
        parent_extractor=parent_extractor,
        child_field_path=child_field_path,
        parent_fields=[
            ParentFieldPath(
                parent_path=parent_field["parent_path"],
                record_path=parent_field["record_path"],
                parameters=parameters,
            )
            for parent_field in parent_fields or []
        ],
        config=config,
        parameters=parameters,
    )


class CountingExtractor(RecordExtractor):
    """Parent extractor that records how many parents have been pulled so far."""

    def __init__(self, parents: List[Mapping[str, Any]]) -> None:
        self._parents = parents
        self.yielded = 0

    def extract_records(self, response: requests.Response) -> Iterable[MutableMapping[Any, Any]]:
        for parent in self._parents:
            self.yielded += 1
            yield parent  # type: ignore[misc]


def test_parent_as_list():
    response = create_response({"data": [{"id": "p1", "children": [{"id": "c1"}, {"id": "c2"}]}]})
    extractor = nested(dpath("data"), ["children"])

    assert list(extractor.extract_records(response)) == [{"id": "c1"}, {"id": "c2"}]


def test_parent_as_single_object():
    # The shape the GraphQL drill-down document returns: one parent, not a connection.
    response = create_response({"data": {"id": "p1", "children": [{"id": "c1"}]}})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["id"], "record_path": ["pid"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "pid": "p1"}]


def test_child_as_single_object():
    response = create_response({"data": {"id": "p1", "child": {"id": "c1"}}})
    extractor = nested(dpath("data"), ["child"], [{"parent_path": ["id"], "record_path": ["pid"]}])

    assert list(extractor.extract_records(response)) == [{"id": "c1", "pid": "p1"}]


@pytest.mark.parametrize(
    "parent, child_field_path",
    [
        pytest.param({"id": "p1"}, ["children"], id="child_field_path_absent"),
        pytest.param({"id": "p1", "children": None}, ["children"], id="child_collection_null"),
        pytest.param({"id": "p1", "children": []}, ["children"], id="child_collection_empty"),
        pytest.param(
            {"id": "p1", "children": {}}, ["children"], id="child_collection_empty_object"
        ),
        pytest.param(
            {"id": "p1", "children": {"nodes": None}},
            ["children", "nodes"],
            id="nested_collection_null",
        ),
        pytest.param(
            {"id": "p1", "children": {"nodes": []}},
            ["children", "nodes"],
            id="nested_collection_empty",
        ),
        pytest.param({"id": "p1", "children": "not-a-collection"}, ["children"], id="scalar"),
        pytest.param({"id": "p1"}, ["children", "nodes"], id="intermediate_node_absent"),
    ],
)
def test_unresolvable_or_empty_child_field_path_yields_nothing(parent, child_field_path):
    response = create_response({"data": [parent]})
    extractor = nested(dpath("data"), child_field_path)

    assert list(extractor.extract_records(response)) == []


def test_nested_child_field_path():
    response = create_response(
        {"data": [{"id": "p1", "children": {"nodes": [{"id": "c1"}, {"id": "c2"}]}}]}
    )
    extractor = nested(dpath("data"), ["children", "nodes"])

    assert list(extractor.extract_records(response)) == [{"id": "c1"}, {"id": "c2"}]


def test_non_object_children_are_skipped():
    # A scalar cannot carry the parent fields, so emitting it would produce a record that silently
    # lacks them.
    response = create_response({"data": [{"id": "p1", "children": ["scalar", {"id": "c1"}, 7]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["id"], "record_path": ["pid"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "pid": "p1"}]


def test_missing_parent_path_copies_none():
    # Documented choice: copy None, matching `parent.get(source)` in the source-github extractor, so
    # every record of the stream carries the field and the record shape stays stable.
    response = create_response({"data": [{"id": "p1", "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["url"], "record_path": ["parent_url"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "parent_url": None}]


def test_missing_nested_parent_path_copies_none():
    response = create_response({"data": [{"id": "p1", "author": {}, "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"),
        ["children"],
        [{"parent_path": ["author", "login"], "record_path": ["parent_login"]}],
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "parent_login": None}]


def test_existing_key_on_child_is_overwritten():
    # Documented choice: the parent is authoritative for the fields the manifest names. Keeping the
    # child's value would hide a misconfigured `record_path`.
    response = create_response(
        {"data": [{"id": "parent", "children": [{"id": "child", "url": "child-url"}]}]}
    )
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["id"], "record_path": ["url"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "child", "url": "parent"}]


def test_multiple_parent_fields():
    response = create_response(
        {
            "data": [
                {
                    "id": "p1",
                    "url": "https://example.com/p1",
                    "author": {"login": "octocat"},
                    "children": [{"id": "c1"}],
                }
            ]
        }
    )
    extractor = nested(
        dpath("data"),
        ["children"],
        [
            {"parent_path": ["id"], "record_path": ["parent_id"]},
            {"parent_path": ["url"], "record_path": ["parent_url"]},
            {"parent_path": ["author", "login"], "record_path": ["parent", "login"]},
        ],
    )

    assert list(extractor.extract_records(response)) == [
        {
            "id": "c1",
            "parent_id": "p1",
            "parent_url": "https://example.com/p1",
            "parent": {"login": "octocat"},
        }
    ]


def test_record_path_creates_intermediate_objects():
    response = create_response({"data": [{"id": "p1", "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["id"], "record_path": ["a", "b", "c"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "a": {"b": {"c": "p1"}}}]


def test_paths_are_interpolated():
    response = create_response({"data": [{"id": "p1", "reviews": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"),
        ["{{ config['child_connection'] }}"],
        [{"parent_path": ["{{ parameters['parent_field'] }}"], "record_path": ["parent_id"]}],
        parameters={"parent_field": "id"},
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "parent_id": "p1"}]


def test_children_are_mutated_in_place():
    # Deliberate, and documented on the component: the child dicts come out of the body this
    # extractor just decoded, so copying them would only cost memory. Pinned so nobody "fixes" it
    # into a copy without noticing they changed the contract.
    child = {"id": "c1"}
    parent_extractor = CountingExtractor([{"id": "p1", "children": [child]}])
    extractor = nested(
        parent_extractor, ["children"], [{"parent_path": ["id"], "record_path": ["pid"]}]
    )

    records = list(extractor.extract_records(create_response({})))

    assert records[0] is child
    assert child == {"id": "c1", "pid": "p1"}


def test_is_lazy_and_does_not_materialise_children():
    parents = [{"id": f"p{index}", "children": [{"id": f"c{index}"}]} for index in range(5)]
    parent_extractor = CountingExtractor(parents)
    extractor = nested(parent_extractor, ["children"])

    records = extractor.extract_records(create_response({}))

    assert parent_extractor.yielded == 0
    assert next(iter(records)) == {"id": "c0"}
    # Exactly one parent was pulled to produce the first record. A parent whose children were
    # materialised, or an implementation that built the full result before returning, would have
    # drained all five.
    assert parent_extractor.yielded == 1


def test_no_mutable_state_on_the_component():
    # One instance is shared by every partition of a stream and the partitions are read
    # concurrently, so two interleaved reads must not see each other.
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["id"], "record_path": ["pid"]}]
    )
    first = extractor.extract_records(
        create_response({"data": [{"id": "p1", "children": [{"id": "c1"}, {"id": "c2"}]}]})
    )
    second = extractor.extract_records(
        create_response({"data": [{"id": "p2", "children": [{"id": "c3"}]}]})
    )

    interleaved = [next(first), next(second), next(first)]

    assert interleaved == [
        {"id": "c1", "pid": "p1"},
        {"id": "c3", "pid": "p2"},
        {"id": "c2", "pid": "p1"},
    ]


def test_response_is_read_once():
    class SingleUseExtractor(RecordExtractor):
        def __init__(self) -> None:
            self.reads = 0

        def extract_records(
            self, response: requests.Response
        ) -> Iterable[MutableMapping[Any, Any]]:
            self.reads += 1
            yield from response.json()["data"]

    parent_extractor = SingleUseExtractor()
    extractor = nested(parent_extractor, ["children"])

    assert list(
        extractor.extract_records(create_response({"data": [{"children": [{"id": "c1"}]}]}))
    ) == [{"id": "c1"}]
    assert parent_extractor.reads == 1


def test_empty_child_field_path_is_rejected():
    with pytest.raises(ValueError, match="non-empty `child_field_path`"):
        nested(dpath("data"), [])


@pytest.mark.parametrize(
    "parent_path, record_path, expected_message",
    [
        pytest.param([], ["x"], "non-empty `parent_path`", id="empty_parent_path"),
        pytest.param(["x"], [], "non-empty `record_path`", id="empty_record_path"),
    ],
)
def test_empty_parent_field_paths_are_rejected(parent_path, record_path, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        ParentFieldPath(parent_path=parent_path, record_path=record_path, parameters={})


def test_nested_parent_extractor():
    # Two levels without a third component type: the inner extractor's records are the outer
    # extractor's parents. The outer level copies the grandparent field off the stamp the inner
    # level left on the review, which is how fields from two ancestors reach the same record.
    response = create_response(
        {
            "data": [
                {
                    "id": "p1",
                    "reviews": {
                        "nodes": [
                            {"id": "r1", "comments": {"nodes": [{"id": "cm1"}, {"id": "cm2"}]}},
                            {"id": "r2", "comments": {"nodes": []}},
                        ]
                    },
                }
            ]
        }
    )
    extractor = nested(
        nested(
            dpath("data"),
            ["reviews", "nodes"],
            [{"parent_path": ["id"], "record_path": ["pull_request_id"]}],
        ),
        ["comments", "nodes"],
        [
            {"parent_path": ["id"], "record_path": ["review_id"]},
            {"parent_path": ["pull_request_id"], "record_path": ["pull_request_id"]},
        ],
    )

    assert list(extractor.extract_records(response)) == [
        {"id": "cm1", "review_id": "r1", "pull_request_id": "p1"},
        {"id": "cm2", "review_id": "r1", "pull_request_id": "p1"},
    ]


# ---------------------------------------------------------------------------------------------
# A realistic GraphQL document, end to end, and the deepest nesting the component supports.
# ---------------------------------------------------------------------------------------------


def _reaction(identifier: str) -> Dict[str, Any]:
    return {"id": identifier, "content": "THUMBS_UP", "user": {"login": "octocat"}}


def _comment(identifier: str, *reaction_ids: str) -> Dict[str, Any]:
    return {
        "id": identifier,
        "body": "a comment",
        "reactions": {
            "pageInfo": {"hasNextPage": False},
            "nodes": [_reaction(reaction_id) for reaction_id in reaction_ids],
        },
    }


def _review(identifier: str, *comments: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": identifier,
        "comments": {"pageInfo": {"hasNextPage": False}, "nodes": list(comments)},
    }


def _pull_request(number: int, *reviews: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "number": number,
        "url": f"https://github.com/airbytehq/airbyte/pull/{number}",
        "reviews": {"pageInfo": {"hasNextPage": False}, "nodes": list(reviews)},
    }


def test_graphql_connection_document():
    # A connection: `nodes` is a list, and the sibling `pageInfo` is not a record.
    response = create_response(
        {
            "data": {
                "repository": {
                    "pullRequests": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "Y3Vy"},
                        "nodes": [
                            {
                                "number": 1,
                                "url": "https://github.com/airbytehq/airbyte/pull/1",
                                "reviews": {
                                    "pageInfo": {"hasNextPage": False},
                                    "nodes": [
                                        {"id": "PRR_1", "state": "APPROVED"},
                                        {"id": "PRR_2", "state": "COMMENTED"},
                                    ],
                                },
                            },
                            {
                                "number": 2,
                                "url": "https://github.com/airbytehq/airbyte/pull/2",
                                "reviews": {"pageInfo": {"hasNextPage": False}, "nodes": []},
                            },
                        ],
                    }
                }
            }
        }
    )
    extractor = nested(
        dpath("data", "repository", "pullRequests", "nodes"),
        ["reviews", "nodes"],
        [{"parent_path": ["url"], "record_path": ["pull_request_url"]}],
    )

    assert list(extractor.extract_records(response)) == [
        {
            "id": "PRR_1",
            "state": "APPROVED",
            "pull_request_url": "https://github.com/airbytehq/airbyte/pull/1",
        },
        {
            "id": "PRR_2",
            "state": "COMMENTED",
            "pull_request_url": "https://github.com/airbytehq/airbyte/pull/1",
        },
    ]


def test_three_levels_of_nesting():
    # `...pullRequests.nodes[*].reviews.nodes[*].comments.nodes[*].reactions.nodes[*]`. Three
    # `NestedRecordExtractor`s reach the comment, and only the last hop keeps a parent in scope.
    response = create_response(
        {
            "data": {
                "repository": {
                    "pullRequests": {
                        "pageInfo": {"hasNextPage": False},
                        "nodes": [
                            _pull_request(
                                1,
                                _review("PRR_1", _comment("PRRC_1", "RE_1", "RE_2")),
                                _review("PRR_2", _comment("PRRC_2")),
                            ),
                            _pull_request(2),
                        ],
                    }
                }
            }
        }
    )
    extractor = nested(
        nested(
            nested(dpath("data", "repository", "pullRequests", "nodes"), ["reviews", "nodes"]),
            ["comments", "nodes"],
        ),
        ["reactions", "nodes"],
        [{"parent_path": ["id"], "record_path": ["comment_id"]}],
    )

    assert [
        (record["id"], record["comment_id"]) for record in extractor.extract_records(response)
    ] == [
        ("RE_1", "PRRC_1"),
        ("RE_2", "PRRC_1"),
    ]


# ---------------------------------------------------------------------------------------------
# Copied values are never shared between records.
# ---------------------------------------------------------------------------------------------


def test_copied_object_is_not_shared_between_records():
    # Sharing one object across the collection means a later transformation writing into it rewrites
    # records that were already emitted — they are queued unserialized downstream.
    response = create_response(
        {
            "data": [
                {
                    "author": {"login": "octocat"},
                    "children": [{"id": "c1"}, {"id": "c2"}, {"id": "c3"}],
                }
            ]
        }
    )
    extractor = nested(
        dpath("data"),
        ["children"],
        [{"parent_path": ["author"], "record_path": ["author"]}],
    )

    records = list(extractor.extract_records(response))
    assert records[0]["author"] is not records[1]["author"]

    # What a downstream `AddFields` writing `["author", "child_id"]` would do.
    for record in records:
        record["author"]["child_id"] = record["id"]
    assert [record["author"]["child_id"] for record in records] == ["c1", "c2", "c3"]


def test_copied_list_is_not_shared_between_records():
    response = create_response(
        {"data": [{"labels": ["bug"], "children": [{"id": "c1"}, {"id": "c2"}]}]}
    )
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["labels"], "record_path": ["labels"]}]
    )

    records = list(extractor.extract_records(response))
    assert records[0]["labels"] is not records[1]["labels"]

    records[0]["labels"].append("added")
    assert records[1]["labels"] == ["bug"]


def test_copied_scalar_is_not_copied():
    # Scalars are immutable, so every child shares the parent's object. Nothing can observe the
    # difference, and copying per child would be the extractor's per-record cost for nothing.
    response = create_response({"data": [{"url": "u", "children": [{"id": "c1"}, {"id": "c2"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["url"], "record_path": ["url"]}]
    )

    records = list(extractor.extract_records(response))
    assert records[0]["url"] is records[1]["url"]


def test_copied_object_is_not_shared_with_the_parent():
    parent = {"author": {"login": "octocat"}, "children": [{"id": "c1"}]}
    extractor = nested(
        CountingExtractor([parent]),
        ["children"],
        [{"parent_path": ["author"], "record_path": ["author"]}],
    )

    record = next(iter(extractor.extract_records(create_response({}))))
    record["author"]["login"] = "rewritten"
    assert parent["author"] == {"login": "octocat"}


# ---------------------------------------------------------------------------------------------
# The parent is read once per parent, not once per child.
# ---------------------------------------------------------------------------------------------


class RecordingMapping(Dict[str, Any]):
    """A record that counts how many times each of its keys has been read."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.reads: List[str] = []

    def __getitem__(self, key: str) -> Any:
        self.reads.append(key)
        return super().__getitem__(key)


def test_parent_is_read_once_per_parent():
    # Reading the parent per child makes the extractor quadratic in the size of the collection,
    # because each read walks a parent that contains that collection.
    parent = RecordingMapping(
        {"id": "p1", "children": [{"id": f"c{index}"} for index in range(25)]}
    )
    extractor = nested(
        CountingExtractor([parent]),
        ["children"],
        [
            {"parent_path": ["id"], "record_path": ["parent_id"]},
            {"parent_path": ["id"], "record_path": ["also_parent_id"]},
        ],
    )

    records = list(extractor.extract_records(create_response({})))

    assert len(records) == 25
    # One read per `parent_fields` entry, plus the one that resolves `child_field_path`.
    assert parent.reads == ["id", "id", "children"]


# ---------------------------------------------------------------------------------------------
# `record_path` writes, including over a segment that is already taken.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "child,expected",
    [
        pytest.param({"i": 1}, {"i": 1, "parent": {"id": "p1"}}, id="absent"),
        pytest.param(
            {"parent": {"other": 1}},
            {"parent": {"other": 1, "id": "p1"}},
            id="existing_object_is_merged_into",
        ),
        pytest.param({"parent": 7}, {"parent": {"id": "p1"}}, id="scalar_is_replaced"),
        pytest.param({"parent": [{"a": 1}]}, {"parent": {"id": "p1"}}, id="list_is_replaced"),
        pytest.param({"parent": None}, {"parent": {"id": "p1"}}, id="null_is_replaced"),
    ],
)
def test_nested_record_path_over_an_existing_key(child, expected):
    # The documented "a key already present on the child is overwritten" holds for a nested
    # `record_path` too: an intermediate that is not an object is replaced by one.
    response = create_response({"data": [{"id": "p1", "children": [child]}]})
    extractor = nested(
        dpath("data"),
        ["children"],
        [{"parent_path": ["id"], "record_path": ["parent", "id"]}],
    )

    assert list(extractor.extract_records(response)) == [expected]


def test_numeric_record_path_segment_creates_an_object():
    # Every segment is an object key. A numeric segment creating a list would emit an array where
    # the declared schema says object.
    response = create_response({"data": [{"id": "p1", "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["id"], "record_path": ["hist", "0"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "hist": {"0": "p1"}}]


# ---------------------------------------------------------------------------------------------
# Paths that cannot address a field are rejected rather than read as field names.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "child_field_path,parent_fields",
    [
        pytest.param(["a", "*", "n"], None, id="child_field_path"),
        pytest.param(
            ["children"], [{"parent_path": ["a", "*"], "record_path": ["v"]}], id="parent_path"
        ),
        pytest.param(
            ["children"], [{"parent_path": ["a"], "record_path": ["*"]}], id="record_path"
        ),
    ],
)
def test_wildcard_in_a_path_is_rejected(child_field_path, parent_fields):
    # The wildcard fails on the data rather than the manifest: a body with one match under `a`
    # would work, and the next page with two would not. It is rejected outright instead.
    response = create_response(
        {"data": [{"a": {"x": {"n": 1}}, "children": [{"id": "c1"}]}]},
    )
    extractor = nested(dpath("data"), child_field_path, parent_fields)

    with pytest.raises(AirbyteTracedException) as exception_info:
        list(extractor.extract_records(response))

    assert exception_info.value.failure_type == FailureType.config_error
    assert "'*' wildcard" in str(exception_info.value.internal_message)


def test_other_glob_characters_are_literal_field_names():
    # Unlike the `dpath`-based extractors, this component walks paths itself, so `?` and `[...]`
    # address the fields that spell them rather than matching several.
    response = create_response({"data": [{"a?b": "v1", "c[1]": "v2", "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"),
        ["children"],
        [
            {"parent_path": ["a?b"], "record_path": ["q"]},
            {"parent_path": ["c[1]"], "record_path": ["b"]},
        ],
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "q": "v1", "b": "v2"}]


@pytest.mark.parametrize(
    "child_field_path,parent_fields,expected_field",
    [
        pytest.param(["{{ parameters.missing }}"], None, "child_field_path", id="child_field_path"),
        pytest.param(
            ["children"],
            [{"parent_path": ["{{ parameters.missing }}"], "record_path": ["v"]}],
            "parent_path",
            id="parent_path",
        ),
        pytest.param(
            ["children"],
            [{"parent_path": ["id"], "record_path": ["{{ parameters.missing }}"]}],
            "record_path",
            id="record_path",
        ),
    ],
)
def test_segment_that_resolves_to_nothing_is_rejected(
    child_field_path, parent_fields, expected_field
):
    # An unbound parameter evaluates to "", and writing a field literally named "" is rejected by
    # most destinations with an error that names the destination rather than the manifest.
    response = create_response({"data": [{"id": "p1", "children": [{"id": "c1"}]}]})
    extractor = nested(dpath("data"), child_field_path, parent_fields)

    with pytest.raises(AirbyteTracedException) as exception_info:
        list(extractor.extract_records(response))

    assert exception_info.value.failure_type == FailureType.config_error
    assert expected_field in str(exception_info.value.internal_message)


def test_parent_path_addressing_the_child_collection_is_rejected():
    # The child records are not copied out of the parent, so such a path would give every record a
    # snapshot of its whole sibling collection.
    response = create_response({"data": [{"children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["children"], "record_path": ["ctx"]}]
    )

    with pytest.raises(AirbyteTracedException) as exception_info:
        list(extractor.extract_records(response))

    assert exception_info.value.failure_type == FailureType.config_error


def test_parent_path_addressing_an_ancestor_of_the_child_collection_is_rejected():
    response = create_response({"data": [{"a": {"children": [{"id": "c1"}]}}]})
    extractor = nested(
        dpath("data"), ["a", "children"], [{"parent_path": ["a"], "record_path": ["ctx"]}]
    )

    with pytest.raises(AirbyteTracedException) as exception_info:
        list(extractor.extract_records(response))

    assert exception_info.value.failure_type == FailureType.config_error


# ---------------------------------------------------------------------------------------------
# Reading a `parent_path` that does not match the response.
# ---------------------------------------------------------------------------------------------


def test_parent_path_through_a_value_holding_no_fields_raises():
    # Copying `None` here would be indistinguishable from the documented absent-key case, and a
    # stamped field is a legitimate primary key or cursor.
    response = create_response({"data": [{"tags": [{"name": "t1"}], "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["tags", "name"], "record_path": ["tag"]}]
    )

    with pytest.raises(AirbyteTracedException) as exception_info:
        list(extractor.extract_records(response))

    assert exception_info.value.failure_type == FailureType.system_error
    assert "tags" in str(exception_info.value.internal_message)


def test_parent_path_through_a_null_copies_none():
    # A null is data, not a misconfigured path, so it is treated as the absent-key case.
    response = create_response({"data": [{"a": None, "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["a", "b"], "record_path": ["v"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "v": None}]


@pytest.mark.parametrize(
    "parent_path,expected",
    [
        pytest.param(["tags", "0", "name"], "t1", id="index"),
        pytest.param(["tags", "-1", "name"], "t2", id="negative_index"),
    ],
)
def test_list_positions_can_be_addressed(parent_path, expected):
    response = create_response(
        {"data": [{"tags": [{"name": "t1"}, {"name": "t2"}], "children": [{"id": "c1"}]}]}
    )
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": parent_path, "record_path": ["tag"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "tag": expected}]


def test_out_of_range_list_position_copies_none():
    response = create_response({"data": [{"tags": [], "children": [{"id": "c1"}]}]})
    extractor = nested(
        dpath("data"), ["children"], [{"parent_path": ["tags", "0"], "record_path": ["tag"]}]
    )

    assert list(extractor.extract_records(response)) == [{"id": "c1", "tag": None}]


# ---------------------------------------------------------------------------------------------
# The `RecordExtractor` contract is `Mapping`, not `MutableMapping`.
# ---------------------------------------------------------------------------------------------


class ReadOnlyExtractor(RecordExtractor):
    """A parent extractor honouring the declared `Iterable[Mapping[str, Any]]` return type."""

    def __init__(self, parents: List[Mapping[str, Any]]) -> None:
        self._parents = parents

    def extract_records(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        yield from self._parents


@pytest.mark.parametrize(
    "parent",
    [
        pytest.param(
            MappingProxyType({"id": "p1", "children": [{"id": "c1"}]}), id="read_only_parent"
        ),
        pytest.param(
            {"id": "p1", "children": [MappingProxyType({"id": "c1"})]}, id="read_only_child"
        ),
        pytest.param(
            MappingProxyType({"id": "p1", "children": [MappingProxyType({"id": "c1"})]}),
            id="read_only_throughout",
        ),
    ],
)
def test_read_only_mappings_are_not_dropped(parent):
    # The ABC declares `Iterable[Mapping[str, Any]]`, and the schema admits a
    # `CustomRecordExtractor` as a `parent_extractor`, so a custom extractor honouring the declared
    # contract must not silently produce an empty stream.
    extractor = nested(
        ReadOnlyExtractor([parent]),
        ["children"],
        [{"parent_path": ["id"], "record_path": ["parent_id"]}],
    )

    records = list(extractor.extract_records(create_response({})))

    assert records == [{"id": "c1", "parent_id": "p1"}]
    # Downstream transformations mutate records in place, so what is yielded has to be mutable.
    assert all(isinstance(record, MutableMapping) for record in records)


def test_read_only_child_is_copied_rather_than_mutated():
    child = MappingProxyType({"id": "c1"})
    extractor = nested(
        ReadOnlyExtractor([{"id": "p1", "children": [child]}]),
        ["children"],
        [{"parent_path": ["id"], "record_path": ["parent_id"]}],
    )

    assert list(extractor.extract_records(create_response({}))) == [{"id": "c1", "parent_id": "p1"}]
    assert dict(child) == {"id": "c1"}


# ---------------------------------------------------------------------------------------------
# Every skip path leaves a trace.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body, child_field_path, expected_log",
    [
        pytest.param(
            {"data": ["scalar", {"id": "p1", "children": [{"id": "c1"}]}]},
            ["children"],
            "skipped a parent record that is not an object (str)",
            id="non_object_parent",
        ),
        pytest.param(
            {"data": [{"id": "p1", "children": [7, {"id": "c1"}]}]},
            ["children"],
            "skipped a child element that is not an object (int)",
            id="non_object_child",
        ),
        pytest.param(
            {"data": [{"id": "p1", "children": "not-a-collection"}]},
            ["children"],
            "to a str, which holds no records",
            id="child_field_path_resolves_to_a_scalar",
        ),
    ],
)
def test_skipped_records_are_logged(body, child_field_path, expected_log, caplog):
    # A connector whose API shape shifts should not lose records with no signal at all.
    extractor = nested(dpath("data"), child_field_path)

    with caplog.at_level(logging.DEBUG, logger="airbyte"):
        list(extractor.extract_records(create_response(body)))

    assert expected_log in caplog.text
