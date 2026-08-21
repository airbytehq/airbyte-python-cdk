#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""The shipped manifest schema has to parse, and nothing else asserted that it does.

`_get_declarative_component_schema()` runs before any low-code source is built, so a malformed
`declarative_component_schema.yaml` fails every declarative connector at startup. It is also easy
to break from a documentation edit alone: the descriptions are plain YAML scalars, so a `": "` in
prose ends the scalar and the file stops being a mapping. That happened in this file's history and
surfaced as 194 unrelated test failures rather than as one obvious error.

Two guards, because the two ways prose breaks a plain scalar look nothing alike. A `": "` is loud:
the file stops parsing, so loading it is enough to catch it. A `" #"` is silent: YAML reads the
rest of the line as a comment, the value keeps parsing as a shorter string, and nothing in the
parsed tree records that a sentence went missing. Only the source text knows, so the second guard
reads that instead of the loaded schema.
"""

import pkgutil
import re

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    _get_declarative_component_schema,
)

# `key: value` with the value on the same line. List items are excluded on purpose: `- key: value`
# is a nested mapping, where a colon is structure rather than a truncated sentence.
_INLINE_VALUE = re.compile(r"^\s*[\w$\"-]+:\s+(\S.*?)\s*$")

# A plain scalar is one not opened with a quote, a block indicator or a flow collection, and it is
# the only style these two characters are dangerous in: inside `'...'` or a `|` block they are
# text like any other.
_QUOTED_BLOCK_OR_FLOW = "'\"|>&*[{"


def test_the_shipped_component_schema_parses():
    schema = _get_declarative_component_schema()

    assert schema["title"] == "DeclarativeSource"
    assert "HttpRequester" in schema["definitions"]


def test_no_plain_scalar_carries_yaml_punctuation():
    """`": "` ends a plain scalar and `" #"` starts a comment, so either one truncates a value
    written as prose -- the first noisily, the second without a trace. Checked against the source
    text rather than the parsed schema, which by then has already lost the evidence.

    Every inline value is checked, not only `description`, because the hazard belongs to the
    scalar style rather than to the field: a `title` or an `error_message` breaks the same way.
    """
    # The same bytes the loader reads, fetched the same way, so this cannot drift from what ships.
    raw_schema = pkgutil.get_data(
        "airbyte_cdk", "sources/declarative/declarative_component_schema.yaml"
    )
    assert raw_schema is not None, "the manifest schema is missing from the package"

    offenders = []

    for number, line in enumerate(raw_schema.decode().splitlines(), start=1):
        match = _INLINE_VALUE.match(line)
        if match is None:
            continue
        value = match.group(1)
        if value[0] in _QUOTED_BLOCK_OR_FLOW:
            continue
        if ": " in value or " #" in value:
            offenders.append(f"line {number}: {value[:80]}")

    assert not offenders, (
        "these are plain YAML scalars containing punctuation that ends them early; "
        "wrap each one in single quotes:\n" + "\n".join(offenders)
    )
