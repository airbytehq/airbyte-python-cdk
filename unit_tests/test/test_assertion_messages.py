# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Guards against assertion messages that never render as intended.

Two failure modes are covered:

- A string message containing a `{placeholder}` but missing the `f` prefix, which
  prints the literal braces instead of the interpolated value.
- A tuple message, which is always truthy and prints as a tuple repr.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import airbyte_cdk.test.standard_tests as standard_tests

_PLACEHOLDER_PATTERN = re.compile(r"\{[A-Za-z_][A-Za-z0-9_.\[\]()!:'\"\s]*\}")
_STANDARD_TESTS_DIR = Path(standard_tests.__file__).parent


def _uninterpolated_placeholders(node: ast.expr) -> list[str]:
    """Return placeholder-looking substrings that will print literally."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return _PLACEHOLDER_PATTERN.findall(node.value)

    if isinstance(node, ast.JoinedStr):
        # Implicit concatenation of f-string and plain-string fragments collapses
        # into a single JoinedStr; plain fragments survive as Constant values.
        found: list[str] = []
        for value in node.values:
            found.extend(_uninterpolated_placeholders(value))
        return found

    return []


@pytest.mark.parametrize(
    "source_file",
    sorted(_STANDARD_TESTS_DIR.rglob("*.py")),
    ids=lambda path: path.name,
)
def test_assert_messages_are_renderable(source_file: Path) -> None:
    tree = ast.parse(source_file.read_text(), filename=str(source_file))
    problems: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assert) or node.msg is None:
            continue

        if isinstance(node.msg, ast.Tuple):
            problems.append(
                f"{source_file.name}:{node.msg.lineno}: assert message is a tuple, "
                "which is always truthy and prints as a tuple repr"
            )
            continue

        for placeholder in _uninterpolated_placeholders(node.msg):
            problems.append(
                f"{source_file.name}:{node.msg.lineno}: assert message contains "
                f"{placeholder} but the fragment is missing the `f` prefix"
            )

    assert not problems, "\n".join(problems)
