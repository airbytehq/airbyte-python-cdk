#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import dpath


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from airbyte_cdk.models import AirbyteStream


def get_first(
    iterable: Iterable[Any],
    predicate: Callable[[Any], bool] = lambda m: True,  # noqa: ARG005  (unused lambda arg)
) -> Any | None:  # noqa: ANN401  (any-type)
    return next(filter(predicate, iterable), None)


def get_defined_id(stream: AirbyteStream, data: dict[str, Any]) -> str | None:
    if not stream.source_defined_primary_key:
        return None
    primary_key = []
    for key in stream.source_defined_primary_key:
        try:
            primary_key.append(str(dpath.get(data, key)))
        except KeyError:
            primary_key.append("__not_found__")
    return "_".join(primary_key)
