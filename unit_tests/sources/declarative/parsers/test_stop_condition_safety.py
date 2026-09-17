#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.parsers.stop_condition_safety import (
    LAST_PAGE_SIZE_VARIABLE,
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
        # A condition that cannot observe the length of a page is unaffected by the reduction, whatever else
        # it reads from the response.
        pytest.param(
            "{{ not response.next }}", 1, StopConditionSafety.SAFE, id="no_last_page_size"
        ),
        pytest.param(
            "{{ response.next is none }}", 1, StopConditionSafety.SAFE, id="a_test_on_the_response"
        ),
        pytest.param(
            "{{ response.page >= response.total_pages }}",
            1,
            StopConditionSafety.SAFE,
            id="a_lower_bound_on_a_response_value",
        ),
        pytest.param(
            "{{ 100 < response.count }}",
            1,
            StopConditionSafety.SAFE,
            id="a_lower_bound_on_a_response_value_reversed",
        ),
        # `last_page_size` is not the only way to count the records of a page: the response body carries the
        # same number, and these are `{{ last_page_size < 100 }}` counted one layer out. Which response field
        # holds a page length is not knowable here, so an upper bound on any value is warned about rather than
        # assumed to be safe. All four shapes below are live in the fleet today.
        pytest.param(
            "{{ response['values']|length < 1000 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="a_page_length_read_from_the_response",
        ),
        pytest.param(
            '{{ response.get("count", 0) < 1000 }}',
            1,
            StopConditionSafety.UNKNOWN,
            id="a_count_read_from_the_response",
        ),
        pytest.param(
            "{{ response.data | length < config['page_size'] }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="a_page_length_against_a_config_value",
        ),
        pytest.param(
            "{{ not response.result.emailClick or response.result.emailClick|length < 200 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="a_page_length_in_a_larger_expression",
        ),
        pytest.param(
            "{{ 100 > response.data | length }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="a_page_length_read_from_the_response_reversed",
        ),
        pytest.param(
            "{{ not (response.data | length >= 100) }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="a_negated_lower_bound_on_a_response_value",
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
        # A negation flips what the comparison decides, so a lower bound that is safe on its own becomes the
        # dangerous upper bound: `not (last_page_size >= 100)` is `last_page_size < 100`. The comparison alone
        # no longer says what the condition does, so it cannot be classified.
        pytest.param(
            "{{ not (last_page_size >= 100) }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="negated_lower_bound",
        ),
        pytest.param(
            "{{ not last_page_size >= 100 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="negated_lower_bound_without_parentheses",
        ),
        pytest.param(
            "{{ not (last_page_size == 0) }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="negated_emptiness_test",
        ),
        pytest.param(
            "{{ 1 if last_page_size == 0 else 0 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="comparison_inside_a_conditional_expression",
        ),
        # Arithmetic moves the threshold out of the comparison: `last_page_size - 100 < 0` is another way to
        # write `last_page_size < 100`, so neither the literal on the right nor `minimum_page_size` bounds it.
        pytest.param(
            "{{ last_page_size - 100 < 0 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="upper_bound_with_arithmetic",
        ),
        pytest.param(
            "{{ 0 > last_page_size - 100 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="upper_bound_with_arithmetic_reversed",
        ),
        pytest.param(
            "{{ last_page_size * 2 < page_size }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="requested_page_size_with_arithmetic",
        ),
        # A threshold merely built from `page_size` can hold the configured size again - `max(50, 100)` is 100 -
        # so mentioning the variable is not enough for the threshold to follow the reduction.
        pytest.param(
            "{{ last_page_size < [page_size, 100] | max }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="requested_page_size_inside_a_list",
        ),
        pytest.param(
            "{{ last_page_size < page_size + 50 }}",
            1,
            StopConditionSafety.UNKNOWN,
            id="requested_page_size_plus_a_literal",
        ),
        # A `{% if %}` renders truthy text exactly when its test holds, so the test decides the condition.
        pytest.param(
            "{% if last_page_size < 100 %}true{% endif %}",
            1,
            StopConditionSafety.TRUNCATES,
            id="literal_threshold_in_an_if_block",
        ),
        pytest.param(
            "{% if last_page_size == 0 %}true{% endif %}",
            1,
            StopConditionSafety.SAFE,
            id="emptiness_test_in_an_if_block",
        ),
        # ... but only while the guarded branch is the whole story: an `else` branch, or a body the CDK reads
        # as false, breaks the equivalence between the test and what the condition renders.
        pytest.param(
            "{% if last_page_size < 100 %}true{% else %}also true{% endif %}",
            1,
            StopConditionSafety.UNKNOWN,
            id="literal_threshold_in_an_if_block_with_an_else",
        ),
        pytest.param(
            "{% if last_page_size < 100 %}false{% endif %}",
            1,
            StopConditionSafety.UNKNOWN,
            id="if_block_rendering_a_false_value",
        ),
        pytest.param(
            "{% if last_page_size < 100 %}{% endif %}",
            1,
            StopConditionSafety.UNKNOWN,
            id="if_block_rendering_nothing",
        ),
        # Text rendered next to the comparison makes the condition truthy whatever the comparison decided.
        pytest.param(
            "{{ last_page_size < 100 }} records",
            1,
            StopConditionSafety.UNKNOWN,
            id="comparison_rendered_next_to_text",
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


def test_given_negated_comparison_then_reason_points_at_the_shape():
    verdict, reason = classify_stop_condition("{{ not (last_page_size >= 100) }}", 1)

    assert verdict is StopConditionSafety.UNKNOWN
    assert "does not decide the condition on its own" in reason


def test_given_truncating_comparison_next_to_a_negated_one_then_truncates():
    # The negated comparison is only warned about, so it must not mask a sibling that does truncate.
    verdict, _ = classify_stop_condition(
        "{{ not (last_page_size >= 100) or last_page_size < 500 }}", 1
    )

    assert verdict is StopConditionSafety.TRUNCATES


def test_given_page_length_read_from_the_response_then_reason_names_the_expression():
    verdict, reason = classify_stop_condition("{{ response['values']|length < 1000 }}", 1)

    assert verdict is StopConditionSafety.UNKNOWN
    assert LAST_PAGE_SIZE_VARIABLE in reason
