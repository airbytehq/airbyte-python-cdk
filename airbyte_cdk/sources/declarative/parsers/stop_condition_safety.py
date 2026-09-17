#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Config-time analysis of a `CursorPagination` `stop_condition` against a page size reduction.

A condition that infers "this was the last page" from a short page is only correct relative to the page size
that was actually requested. When `page_size_reduction` shrinks that size, a *full* page at the reduced size
reads as a short page against the configured size, the pagination ends, and the rest of the partition is
dropped without anything failing.

Matching the expression as a string cannot tell the reduction-aware `page_size` variable apart from a
config-derived constant - `\\bpage_size\\b` matches inside `config['page_size']` just as well - and it cannot
tell an inequality, which a reduction can invalidate, apart from `last_page_size == 0`, which no reduction can.
So the expression is parsed with Jinja and the comparisons involving `last_page_size` are inspected on the AST,
where a bare `Name` node is distinguishable from a `Getitem` on `config`.

The safety rule is a single question: can the condition be true for a page that is *full at the size that was
requested*? A full page holds exactly `requested_page_size` records, and a reduction only ever lowers that
number, so:

- `last_page_size == 0` and `last_page_size != 0` cannot change verdict: a full page is never empty, because
  `minimum_page_size` is at least 1.
- `last_page_size > x` and `last_page_size >= x` can only stop being true as the page shrinks, so a reduction
  cannot introduce a stop that would not have happened anyway.
- `last_page_size < x` and `last_page_size <= x` are the dangerous shape. They are safe only when `x` is the
  `page_size` variable, which follows the reduction, or when `x` is a literal at or below `minimum_page_size`,
  which makes the comparison a rewrite of "the page is empty". An expression merely built from `page_size`,
  such as `[page_size, 100] | max` or `page_size + 50`, does not count: it can hold the configured size again.

Each verdict above reads the comparison on its own, which is only sound while the comparison decides the
condition in its own polarity. `{{ not (last_page_size >= 100) }}` means `last_page_size < 100`, and
`{{ last_page_size - 100 < 0 }}` means the same again, so the operator and the operands of the comparison no
longer say what the condition does. A comparison is therefore only classified when the truth of the condition follows its own: reached from the
root through `and`/`or`, or as the test of a `{% if %}` that renders truthy text and nothing else, and with
`last_page_size` compared bare rather than transformed first. Anything else is unclassifiable.

A condition that never names `last_page_size` is not safe by default either: the response body carries the
same count, so `{{ response['data'] | length < 100 }}` truncates in exactly the same way. Which of a
connector's own response fields counts the records of a page is not knowable here, so any comparison that
bounds something from above is reported as unclassifiable. A condition with no such comparison, such as
`{{ not response.next }}`, cannot read a page length by comparison and is safe.

Anything else is reported as unclassifiable rather than as a truncation: this analysis rejects a manifest at
stream construction, so a shape it does not understand must not be treated as a defect.
"""

from enum import Enum
from typing import Iterator, List, Optional, Tuple

from jinja2 import nodes
from jinja2.environment import Environment
from jinja2.exceptions import TemplateSyntaxError

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import FALSE_VALUES

LAST_PAGE_SIZE_VARIABLE = "last_page_size"
REQUESTED_PAGE_SIZE_VARIABLE = "page_size"

# Parsing is all this environment is used for: no filter, test or global is resolved, so the plain environment
# parses everything the interpolation environment accepts.
_PARSING_ENVIRONMENT = Environment()

_MIRRORED_OPERATORS = {
    "lt": "gt",
    "lteq": "gteq",
    "gt": "lt",
    "gteq": "lteq",
    "eq": "eq",
    "ne": "ne",
}


class StopConditionSafety(Enum):
    """Whether a page size reduction can change what a `stop_condition` decides."""

    SAFE = "SAFE"
    """No reduction can make the condition stop the pagination earlier than it already would."""

    TRUNCATES = "TRUNCATES"
    """A full page at a reduced size satisfies the condition, so the partition would be cut short."""

    UNKNOWN = "UNKNOWN"
    """The condition uses `last_page_size` in a shape this analysis cannot reason about."""


def classify_stop_condition(
    stop_condition: str, minimum_page_size: int
) -> Tuple[StopConditionSafety, str]:
    """
    :param stop_condition: the raw `stop_condition` template from the manifest
    :param minimum_page_size: the smallest page size the reduction is allowed to request
    :return: the verdict and a human readable reason for it
    """
    try:
        template = _PARSING_ENVIRONMENT.parse(stop_condition)
    except TemplateSyntaxError as exception:
        return (
            StopConditionSafety.UNKNOWN,
            f"it is not a valid Jinja expression ({exception.message})",
        )

    if not _references(template, LAST_PAGE_SIZE_VARIABLE):
        return _classify_without_last_page_size(template)

    unclassifiable: List[str] = []
    understood = 0
    for left, operator, right, decides_condition in _comparisons(template):
        if not _references(left, LAST_PAGE_SIZE_VARIABLE) and not _references(
            right, LAST_PAGE_SIZE_VARIABLE
        ):
            continue
        if not decides_condition:
            # The comparison is negated or is an operand of a larger expression, so its own operator no longer
            # says what the condition does and neither verdict below would be about the right question.
            unclassifiable.append(
                f"it uses `{LAST_PAGE_SIZE_VARIABLE}` in a comparison that does not decide the condition on "
                f"its own, such as one under a `not` or inside a larger expression"
            )
            continue
        verdict, reason = _classify_comparison(left, operator, right, minimum_page_size)
        if verdict is StopConditionSafety.TRUNCATES:
            return verdict, reason
        if verdict is StopConditionSafety.UNKNOWN:
            unclassifiable.append(reason)
        else:
            understood += 1

    if unclassifiable:
        return StopConditionSafety.UNKNOWN, unclassifiable[0]
    if understood:
        return (
            StopConditionSafety.SAFE,
            f"every comparison it makes against `{LAST_PAGE_SIZE_VARIABLE}` holds whatever page size was requested",
        )
    return (
        StopConditionSafety.UNKNOWN,
        f"it uses `{LAST_PAGE_SIZE_VARIABLE}` outside of a comparison",
    )


def _classify_without_last_page_size(
    template: nodes.Template,
) -> Tuple[StopConditionSafety, str]:
    """
    Classify a condition that never names `last_page_size`.

    `last_page_size` is not the only way to observe how many records a page held: the response body carries
    the same number, and `{{ response['data'] | length < 100 }}` is the comparison `{{ last_page_size < 100 }}`
    counted one layer out. The CDK cannot tell which of a connector's own response fields counts the records
    of the page, so a condition that bounds *anything* from above is reported as unclassifiable and warned
    about rather than assumed to be safe. A condition that makes no such comparison - the common
    `{{ not response.next }}` shape - cannot read a page length by comparison at all, so it stays safe.
    """
    for left, operator, right, decides_condition in _comparisons(template):
        bounded = _bounded_from_above(left, operator, right, decides_condition)
        if bounded is not None:
            return (
                StopConditionSafety.UNKNOWN,
                f"it stops when {_describe(bounded)} stays below a threshold. `{LAST_PAGE_SIZE_VARIABLE}` is "
                f"not the only way to count the records of a page - a count read from the response body is "
                f"another - and the connector cannot tell whether this one does, so a full page at a reduced "
                f"size may read as a short page here",
            )
    return (
        StopConditionSafety.SAFE,
        f"it does not use `{LAST_PAGE_SIZE_VARIABLE}` and makes no comparison that could be bounding the "
        f"number of records in a page from above",
    )


def _bounded_from_above(
    left: nodes.Node, operator: str, right: nodes.Node, decides_condition: bool
) -> Optional[nodes.Node]:
    """
    :return: the operand the comparison bounds from above, or None when it bounds nothing from above

    An upper bound is the shape a reduction can invalidate, for the same reason it can invalidate
    `last_page_size < 100`: the reduction lowers what a full page holds, so the bound starts holding for pages
    that are not short at all. A lower bound can only stop being satisfied as pages get smaller. When the
    comparison does not decide the condition in its own polarity, which of the two it is cannot be read off
    the operator, so it counts as an upper bound.
    """
    if operator not in ("lt", "lteq", "gt", "gteq"):
        return None
    if not decides_condition:
        return left if not isinstance(left, nodes.Const) else right
    if operator in ("lt", "lteq"):
        # `x < 100` bounds x from above; `100 < x` is a lower bound on x.
        return None if isinstance(left, nodes.Const) else left
    # `100 > x` is the mirror of `x < 100`; `x > 100` is a lower bound on x.
    return right if isinstance(left, nodes.Const) else None


def _comparisons(
    node: nodes.Node, decides_condition: bool = True
) -> Iterator[Tuple[nodes.Node, str, nodes.Node, bool]]:
    """
    Flatten every comparison, including the chained ones, into (left, operator, right, decides_condition).

    `decides_condition` is true only for a comparison the truth of the whole condition follows directly:
    reached from the root through `and`/`or` alone. Under a `not`, inside a conditional expression, piped
    through a filter or used as an operand of another expression, the comparison's own operator says nothing
    about what the condition decides, so it is flagged and left unclassified.
    """
    if isinstance(node, nodes.Compare):
        left = node.expr
        for operand in node.ops:
            yield left, operand.op, operand.expr, decides_condition
            left = operand.expr
        # A comparison nested inside an operand of this one is an ordinary sub-expression, not a decider.
        for operand_node in [node.expr, *(operand.expr for operand in node.ops)]:
            yield from _comparisons(operand_node, False)
        return

    if isinstance(node, nodes.If):
        # `{% if last_page_size < 100 %}true{% endif %}` renders truthy text exactly when its test holds, so
        # the test decides the condition. That only follows while the branch it guards is the whole story:
        # an `else`, an `elif`, or a body rendering something the CDK reads as false breaks the equivalence.
        test_decides = decides_condition and _if_follows_its_test(node)
        yield from _comparisons(node.test, test_decides)
        for branch in [*node.body, *node.elif_, *node.else_]:
            yield from _comparisons(branch, False)
        return

    children_decide = decides_condition and _passes_truth_through(node)
    for child in node.iter_child_nodes():
        yield from _comparisons(child, children_decide)


def _passes_truth_through(node: nodes.Node) -> bool:
    """Whether the truth of the condition follows the truth of this node's children."""
    if isinstance(node, (nodes.And, nodes.Or)):
        return True
    if isinstance(node, nodes.Template):
        # Anything rendered next to the comparison is text of its own, which makes the rendered condition
        # truthy whatever the comparison decided.
        return len(node.body) == 1
    if isinstance(node, nodes.Output):
        return len(node.nodes) == 1
    return False


def _if_follows_its_test(node: nodes.If) -> bool:
    if node.elif_ or node.else_:
        return False
    rendered = ""
    for statement in node.body:
        if not isinstance(statement, nodes.Output):
            return False
        for output in statement.nodes:
            if isinstance(output, nodes.TemplateData):
                rendered += output.data
            elif isinstance(output, nodes.Const):
                rendered += str(output.value)
            else:
                return False
    return rendered not in FALSE_VALUES


def _classify_comparison(
    left: nodes.Node, operator: str, right: nodes.Node, minimum_page_size: int
) -> Tuple[StopConditionSafety, str]:
    if _references(right, LAST_PAGE_SIZE_VARIABLE) and not _references(
        left, LAST_PAGE_SIZE_VARIABLE
    ):
        left, right, operator = right, left, _MIRRORED_OPERATORS.get(operator, operator)
    elif _references(left, LAST_PAGE_SIZE_VARIABLE) and _references(right, LAST_PAGE_SIZE_VARIABLE):
        return (
            StopConditionSafety.UNKNOWN,
            f"it compares `{LAST_PAGE_SIZE_VARIABLE}` against itself",
        )

    is_bare = isinstance(left, nodes.Name) and left.name == LAST_PAGE_SIZE_VARIABLE

    if operator in ("eq", "ne"):
        if not is_bare:
            return (
                StopConditionSafety.UNKNOWN,
                f"`{LAST_PAGE_SIZE_VARIABLE}` is transformed before being compared",
            )
        if isinstance(right, nodes.Const) and right.value == 0:
            # A page that is full at the requested size holds at least `minimum_page_size` records, which is
            # at least 1, so no reduction can make an emptiness test fire.
            return (
                StopConditionSafety.SAFE,
                f"`{LAST_PAGE_SIZE_VARIABLE}` is only tested for emptiness",
            )
        return (
            StopConditionSafety.UNKNOWN,
            f"it tests `{LAST_PAGE_SIZE_VARIABLE}` for equality against {_describe(right)} rather than against 0",
        )

    if operator in ("gt", "gteq"):
        if is_bare:
            # A reduction only lowers the size of a full page, so a lower bound can only stop being satisfied.
            return (
                StopConditionSafety.SAFE,
                f"a smaller page can only make `{LAST_PAGE_SIZE_VARIABLE} {'>' if operator == 'gt' else '>='} "
                f"{_describe(right)}` less true, never more",
            )
        return (
            StopConditionSafety.UNKNOWN,
            f"`{LAST_PAGE_SIZE_VARIABLE}` is transformed before being compared",
        )

    if operator in ("lt", "lteq"):
        if isinstance(left, nodes.BinExpr):
            # `last_page_size - 100 < 0` is `last_page_size < 100` in disguise: with arithmetic on the left the
            # threshold on the right is no longer the page size the condition really stops at, so neither the
            # `page_size` reading nor the `minimum_page_size` reading below applies.
            return (
                StopConditionSafety.UNKNOWN,
                f"`{LAST_PAGE_SIZE_VARIABLE}` takes part in arithmetic before being compared",
            )
        if _references(right, REQUESTED_PAGE_SIZE_VARIABLE):
            if not _is_requested_page_size(right):
                # `[page_size, 100] | max` and `page_size + 50` both mention the variable while holding a
                # value a reduction does not lower, so the threshold does not follow the reduction after all.
                return (
                    StopConditionSafety.UNKNOWN,
                    f"it compares `{LAST_PAGE_SIZE_VARIABLE}` against an expression built from "
                    f"`{REQUESTED_PAGE_SIZE_VARIABLE}` rather than against `{REQUESTED_PAGE_SIZE_VARIABLE}` "
                    f"itself, so the threshold may not follow the reduction",
                )
            return (
                StopConditionSafety.SAFE,
                f"it compares `{LAST_PAGE_SIZE_VARIABLE}` against `{REQUESTED_PAGE_SIZE_VARIABLE}`, "
                f"which follows the reduction",
            )
        if isinstance(right, nodes.Const) and _is_whole_number(right.value):
            # `last_page_size < k` is false for every full page exactly when k is at or below the smallest page
            # the connector is allowed to request, which makes it another way of writing "the page is empty".
            threshold = right.value if operator == "lt" else right.value + 1
            if threshold <= minimum_page_size:
                return (
                    StopConditionSafety.SAFE,
                    f"no page the connector is allowed to request is smaller than {threshold}",
                )
        return (
            StopConditionSafety.TRUNCATES,
            f"it stops as soon as a page is smaller than {_describe(right)}, which does not follow the "
            f"reduction, so a full page at a reduced size reads as a short page",
        )

    return (
        StopConditionSafety.UNKNOWN,
        f"it uses `{LAST_PAGE_SIZE_VARIABLE}` with the `{operator}` operator",
    )


def _is_requested_page_size(node: nodes.Node) -> bool:
    """Whether the node is the `page_size` variable itself, filters aside."""
    unfiltered: Optional[nodes.Node] = node
    while isinstance(unfiltered, nodes.Filter):
        unfiltered = unfiltered.node
    return isinstance(unfiltered, nodes.Name) and unfiltered.name == REQUESTED_PAGE_SIZE_VARIABLE


def _references(node: nodes.Node, name: str) -> bool:
    if isinstance(node, nodes.Name):
        return bool(node.name == name)
    return any(referenced.name == name for referenced in node.find_all(nodes.Name))


def _is_whole_number(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _describe(node: nodes.Node) -> str:
    """Render the expression a `last_page_size` comparison is made against, for an error message."""
    if isinstance(node, nodes.Const):
        return repr(node.value)
    if isinstance(node, nodes.Name):
        return f"`{node.name}`"
    if isinstance(node, nodes.Getitem) and isinstance(node.arg, nodes.Const):
        return f"`{_describe(node.node).strip('`')}[{node.arg.value!r}]`"
    if isinstance(node, nodes.Getattr):
        return f"`{_describe(node.node).strip('`')}.{node.attr}`"
    return "a value the connector computes itself"
