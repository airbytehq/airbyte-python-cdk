#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import requests

from airbyte_cdk.sources.declarative.requesters.paginators.no_pagination import NoPagination


def test():
    paginator = NoPagination(parameters={})
    next_page_token = paginator.next_page_token(requests.Response(), 0, [])
    assert next_page_token == {}
