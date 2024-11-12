#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# ruff: noqa: A005  # Shadows built-in 'types' module

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any


StreamSlice = Mapping[str, Any]
StreamState = MutableMapping[str, Any]
