#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations


class ReadException(Exception):
    """Raise when there is an error reading data from an API Source"""
