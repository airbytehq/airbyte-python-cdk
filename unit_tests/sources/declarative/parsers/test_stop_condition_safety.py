#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.parsers.stop_condition_safety import (
    StopConditionSafety,
    classify_stop_condition,
)


@pytest.mark.parametrize(
    "stop_condition,minimum_page_size,expected",
    [
        # The 11 monorepo occurrences of the emptiness test - source-zendesk-support x6, source-trello x5 -
        # which the previous string-matching gate rejected. No reduction can make a full page empty.
        pytest.param("{{ last_page_size == 0 }}", 1, StopConditionSafety.SAFE, id="emptiness_test"),
        pytest.param(
            "{{ 0 == last_page_size }}", 1, StopConditionSafety.SAFE, id="emptiness_test_reversed"
        ),
        pytest.param(
            "{{ last_page_size != 0 }}", 1, StopConditionSafety.SAFE, id="non_emptiness_test"
        ),
        pytest.param(
            "{{ last_page_size == 0 or not response.next }}",
            1,
            StopConditionSafety.SAFE,
            id="emptiness_test_in_a_larger_expression",
        ),
        # The 3 monorepo occurrences of the literal form - source-discord x3 - plus the equivalents.
        pytest.param(
            "{{ last_page_size < 100 }}", 1, StopConditionSafety.TRUNCATES, id="literal_100"
        ),
        pytest.param(
            "{{ last_page_size < 200 }}", 1, StopConditionSafety.TRUNCATES, id="literal_200"
        ),
        pytest.param(
            "{{ last_page_size < 1000 }}", 1, StopConditionSafety.TRUNCATES, id="literal_1000"
        ),
        # The false negative of the string-matching gate: `\bpage_size\b` matches inside `config['page_size']`
        # because `'` is a non-word character, so the gate read a configured constant as reduction-aware.
        pytest.param(
            "{{ last_page_size < config['page_size'] }}",
            1,
            StopConditionSafety.TRUNCATES,
            id="config_getitem",
        ),
        pytest.param(
            "{{ last_page_size < config.page_size }}",
            1,
            StopConditionSafety.TRUNCATES,
            id="config_getattr",
        ),
        pytest.param(
            "{{ last_page_size < parameters['page_size'] }}",
            1,
            StopConditionSafety.TRUNCATES,
            id="parameters_getitem",
        ),
        # The sanctioned form and the shapes that still follow the reduction.
        pytest.param(
            "{{ last_page_size < page_size }}",
            1,
            StopConditionSafety.SAFE,
            id="requested_page_size",
        ),
        pytest.param(
            "{{ last_page_size < page_size | int }}",
            1,
            StopConditionSafety.SAFE,
            id="requested_page_size_through_a_filter",
        ),
        pytest.param(
            "{{ page_size > last_page_size }}",
            1,
            StopConditionSafety.SAFE,
            id="requested_page_size_reversed",
        ),
        # A threshold at or below the smallest page the connector may request is a rewrite of the emptiness
        # test, so it is safe - and it stops being safe as soon as the minimum rises above it.
        pytest.param("{{ last_page_size < 1 }}", 1, StopConditionSafety.SAFE, id="below_minimum"),
        pytest.param("{{ last_page_size <= 0 }}", 1, StopConditionSafety.SAFE, id="at_most_zero"),
        pytest.param(
            "{{ last_page_size < 10 }}", 10, StopConditionSafety.SAFE, id="at_a_raised_minimum"
        ),
        pytest.param(
            "{{ last_page_size < 11 }}",
            10,
            StopConditionSafety.TRUNCATES,
            id="above_a_raised_minimum",
        ),
        # A reduction only makes a full page smaller, so a lower bound can only stop being satisfied.
        pytest.param("{{ last_page_size > 1000 }}", 1, StopConditionSafety.SAFE, id="lower_bound"),
        pytest.param(
            "{{ last_page_size >= 1000 }}", 1, StopConditionSafety.SAFE, id="inclusive_lower_bound"
        ),
        # A condition that never looks at the page size is unaffected by the reduction.
        pytest.param(
            "{{ not response.next }}", 1, StopConditionSafety.SAFE, id="no_last_page_size"
        ),
        # Shapes the analysis cannot reason about are reported as unknown so the caller can warn rather than
        # reject: this runs at stream construction, where a false rejection also breaks `check` and `discover`.
        pytest.param(
            "{{ last_page_size == config['page_size'] }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="equality_against_a_config_value",
        ),
        pytest.param(
            "{{ last_page_size is lt(100) }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="jinja_test_rather_than_a_comparison",
        ),
        pytest.param(
            "{{ last_page_size }}", 1, StopConditionSafety.UNKNOWN, id="no_comparison_at_all"
        ),
        pytest.param(
            "{{ last_page_size < }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="not_a_valid_jinja_expression",
        ),
        pytest.param(
            "{{ last_page_size > last_page_size }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="compared_against_itself",
        ),
        # A transform makes the equality and the lower bound unreadable, but a shrinking upper bound is still
        # a truncation whatever `last_page_size` was piped through.
        pytest.param(
            "{{ last_page_size | int > 100 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="lower_bound_through_a_filter",
        ),
        pytest.param(
            "{{ last_page_size | int < 100 }}",
            1,
            StopConditionSafety.TRUNCATES,
            id="upper_bound_through_a_filter",
        ),
    ],
)
def test_classify_stop_condition(stop_condition, minimum_page_size, expected):
    verdict, reason = classify_stop_condition(stop_condition, minimum_page_size)

    assert verdict is expected
    assert reason


def test_given_one_truncating_comparison_among_several_then_truncates():
    verdict, _ = classify_stop_condition(
        "{{ last_page_size == 0 or last_page_size < config['page_size'] }}", 1
    )

    assert verdict is StopConditionSafety.TRUNCATES


def test_reason_names_the_value_the_page_size_is_compared_against():
    _, reason = classify_stop_condition("{{ last_page_size < config['page_size'] }}", 1)

    assert "config['page_size']" in reason
