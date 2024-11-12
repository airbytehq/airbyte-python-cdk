#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from typing import TYPE_CHECKING, Any


@dataclass
class RequestPath:
    """Describes that a component value should be inserted into the path"""

    parameters: InitVar[Mapping[str, Any]]
