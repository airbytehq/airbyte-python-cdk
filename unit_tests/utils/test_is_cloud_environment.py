#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import os
from unittest import mock

import pytest

from airbyte_cdk.utils import is_cloud_environment


@pytest.mark.parametrize(
    "env, expected",
    [
        pytest.param({}, False, id="neither_set"),
        pytest.param({"AIRBYTE_EDITION": "cloud"}, True, id="edition_lowercase"),
        pytest.param({"AIRBYTE_EDITION": "CLOUD"}, True, id="edition_uppercase"),
        pytest.param({"AIRBYTE_EDITION": "community"}, False, id="edition_community"),
        pytest.param({"DEPLOYMENT_MODE": "CLOUD"}, True, id="deployment_mode_uppercase"),
        pytest.param({"DEPLOYMENT_MODE": "cloud"}, True, id="deployment_mode_lowercase"),
        pytest.param({"DEPLOYMENT_MODE": "OSS"}, False, id="deployment_mode_oss"),
        pytest.param(
            {"AIRBYTE_EDITION": "CLOUD", "DEPLOYMENT_MODE": "CLOUD"}, True, id="both_cloud"
        ),
        pytest.param(
            {"AIRBYTE_EDITION": "COMMUNITY", "DEPLOYMENT_MODE": "OSS"}, False, id="both_oss"
        ),
    ],
)
def test_is_cloud_environment(env, expected):
    with mock.patch.dict(os.environ, env, clear=True):
        assert is_cloud_environment() is expected
