# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from unittest import TestCase
from unittest.mock import Mock

import requests
import requests_mock

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    ResponseAction,
    create_fallback_error_resolution,
)
from airbyte_cdk.utils.airbyte_secrets_utils import update_secrets

_A_SECRET = "a-secret"
_A_URL = "https://a-url.com"


class DefaultErrorResolutionTest(TestCase):
    def setUp(self) -> None:
        update_secrets([_A_SECRET])

    def tearDown(self) -> None:
        # to avoid other tests being impacted by added secrets
        update_secrets([])

    def test_given_none_when_create_fallback_error_resolution_then_return_error_resolution(
        self,
    ) -> None:
        any_response_or_exception = Mock()
        assert create_fallback_error_resolution(any_response_or_exception) is None

    def _create_response(self, status_code: int) -> requests.Response:
        with requests_mock.Mocker() as http_mocker:
            http_mocker.get(_A_URL, status_code=status_code)
            return requests.get(_A_URL)
