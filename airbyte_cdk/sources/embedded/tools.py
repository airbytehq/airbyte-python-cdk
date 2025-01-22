#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections.abc import Callable, Iterable
from typing import Any

import dpath

from airbyte_cdk.models import AirbyteStream


def get_first(
    iterable: Iterable[Any],
    predicate: Callable[[Any], bool] = lambda m: True,  # noqa: ARG005
) -> Any | None:  # noqa: ANN401
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
