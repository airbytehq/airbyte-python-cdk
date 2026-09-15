#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
import io
import json
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Union

import pytest
import requests

from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk.sources.declarative.extractors.nested_record_extractor import (
    NestedRecordExtractor,
    ParentFieldPath,
)
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor

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
# The two source-github shapes, end to end.
#
# Both come from `NestedGraphQLRecordExtractor` and `DeepNestedGraphQLRecordExtractor` in
# airbyte-integrations/connectors/source-github/source_github/components.py on the unmerged
# branch tolik0/source-github/graphql-streams.
# ---------------------------------------------------------------------------------------------

REVIEWS_LISTING_RESPONSE = {
    "data": {
        "repository": {
            "name": "airbyte",
            "owner": {"login": "airbytehq"},
            "pullRequests": {
                "pageInfo": {"hasNextPage": False, "endCursor": "Y3Vy"},
                "nodes": [
                    {
                        "number": 1,
                        "url": "https://github.com/airbytehq/airbyte/pull/1",
                        "reviews": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
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
            },
        }
    }
}

REVIEWS_DRILLDOWN_RESPONSE = {
    "data": {
        "repository": {
            "name": "airbyte",
            "owner": {"login": "airbytehq"},
            "pullRequest": {
                "number": 1,
                "url": "https://github.com/airbytehq/airbyte/pull/1",
                "reviews": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [{"id": "PRR_3", "state": "APPROVED"}],
                },
            },
        }
    }
}


def reviews_extractor(*parent_field_path: str) -> NestedRecordExtractor:
    return nested(
        dpath(*parent_field_path),
        ["reviews", "nodes"],
        [{"parent_path": ["url"], "record_path": ["pull_request_url"]}],
    )


def test_source_github_reviews_listing_document():
    records = list(
        reviews_extractor("data", "repository", "pullRequests", "nodes").extract_records(
            create_response(REVIEWS_LISTING_RESPONSE)
        )
    )

    assert records == [
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


def test_source_github_reviews_drilldown_document():
    # `pullRequest` is a single object, not a connection. `DpathExtractor` wraps it into a
    # one-element list, so the same `child_field_path` works against both documents.
    records = list(
        reviews_extractor("data", "repository", "pullRequest").extract_records(
            create_response(REVIEWS_DRILLDOWN_RESPONSE)
        )
    )

    assert records == [
        {
            "id": "PRR_3",
            "state": "APPROVED",
            "pull_request_url": "https://github.com/airbytehq/airbyte/pull/1",
        }
    ]


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


def reactions_extractor(parent_extractor: RecordExtractor) -> NestedRecordExtractor:
    return nested(
        parent_extractor,
        ["reactions", "nodes"],
        [{"parent_path": ["id"], "record_path": ["comment_id"]}],
    )


def test_source_github_reactions_from_repository_listing_root():
    # `data.repository.pullRequests.nodes[*].reviews.nodes[*].comments.nodes[*].reactions.nodes[*]`.
    # Three nested levels reach the comment, and only the last hop needs the parent kept in scope.
    response = create_response(
        {
            "data": {
                "repository": {
                    "name": "airbyte",
                    "owner": {"login": "airbytehq"},
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
                    },
                }
            }
        }
    )
    extractor = reactions_extractor(
        nested(
            nested(dpath("data", "repository", "pullRequests", "nodes"), ["reviews", "nodes"]),
            ["comments", "nodes"],
        )
    )

    assert [
        (record["id"], record["comment_id"]) for record in extractor.extract_records(response)
    ] == [
        ("RE_1", "PRRC_1"),
        ("RE_2", "PRRC_1"),
    ]


def test_source_github_reactions_from_pull_request_node_root():
    response = create_response(
        {
            "data": {
                "node": {
                    "__typename": "PullRequest",
                    "repository": {"name": "airbyte", "owner": {"login": "airbytehq"}},
                    **_pull_request(1, _review("PRR_1", _comment("PRRC_1", "RE_1"))),
                }
            }
        }
    )
    extractor = reactions_extractor(
        nested(nested(dpath("data", "node"), ["reviews", "nodes"]), ["comments", "nodes"])
    )

    assert [
        (record["id"], record["comment_id"]) for record in extractor.extract_records(response)
    ] == [("RE_1", "PRRC_1")]


def test_source_github_reactions_from_review_node_root():
    response = create_response(
        {
            "data": {
                "node": {
                    "__typename": "PullRequestReview",
                    "repository": {"name": "airbyte", "owner": {"login": "airbytehq"}},
                    **_review("PRR_1", _comment("PRRC_1", "RE_1"), _comment("PRRC_2", "RE_2")),
                }
            }
        }
    )
    extractor = reactions_extractor(nested(dpath("data", "node"), ["comments", "nodes"]))

    assert [
        (record["id"], record["comment_id"]) for record in extractor.extract_records(response)
    ] == [
        ("RE_1", "PRRC_1"),
        ("RE_2", "PRRC_2"),
    ]


def test_source_github_reactions_from_comment_node_root():
    response = create_response(
        {
            "data": {
                "node": {
                    "__typename": "PullRequestReviewComment",
                    "repository": {"name": "airbyte", "owner": {"login": "airbytehq"}},
                    **_comment("PRRC_1", "RE_1", "RE_2"),
                }
            }
        }
    )
    extractor = reactions_extractor(dpath("data", "node"))

    assert [
        (record["id"], record["comment_id"]) for record in extractor.extract_records(response)
    ] == [
        ("RE_1", "PRRC_1"),
        ("RE_2", "PRRC_1"),
    ]


def test_typename_dispatch_equivalence_is_query_dependent():
    """Pin why the `__typename` dispatch in `DeepNestedGraphQLRecordExtractor` is not redundant.

    Expressing the four reaction roots as four separate `NestedRecordExtractor` chains looks
    equivalent to dispatching on `__typename`, because a `PullRequestReview` has no `reviews` field
    and a `PullRequest` has no `comments` field *in the documents source-github actually sends*.
    `PullRequest.comments` is a real GraphQL connection though, so the equivalence holds only
    because the drill-down documents do not select it.

    A path-based extractor is purely structural: given a node that carries both `reviews` and
    `comments`, the `PullRequest`-rooted chain still only walks `reviews.nodes.comments.nodes`, and
    a chain rooted at `comments.nodes` would walk the top-level `comments` instead. A future reader
    changing the query to select `PullRequest.comments` has to add a root, not rely on this holding.
    """
    node = {
        "__typename": "PullRequest",
        **_pull_request(1, _review("PRR_1", _comment("PRRC_review", "RE_review"))),
        # A real connection on PullRequest that the current source-github documents do not select.
        "comments": {
            "pageInfo": {"hasNextPage": False},
            "nodes": [_comment("PRRC_issue", "RE_issue")],
        },
    }

    via_reviews = reactions_extractor(
        nested(nested(dpath("data", "node"), ["reviews", "nodes"]), ["comments", "nodes"])
    )
    via_top_level_comments = reactions_extractor(
        nested(dpath("data", "node"), ["comments", "nodes"])
    )

    assert [
        (record["id"], record["comment_id"])
        for record in via_reviews.extract_records(create_response({"data": {"node": node}}))
    ] == [("RE_review", "PRRC_review")]
    assert [
        (record["id"], record["comment_id"])
        for record in via_top_level_comments.extract_records(
            create_response({"data": {"node": node}})
        )
    ] == [("RE_issue", "PRRC_issue")]
