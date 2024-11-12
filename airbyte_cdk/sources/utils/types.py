#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# ruff: noqa: A005  # Shadows built-in 'types' module

from __future__ import annotations

from typing import Union


JsonType = Union[  # noqa: UP007  (deprecated Union type)
    dict[str, "JsonType"],
    list["JsonType"],
    str,
    int,
    float,
    bool,
    None,
]
