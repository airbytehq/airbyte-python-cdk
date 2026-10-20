import pytest

from airbyte_cdk.sources.declarative.requesters.request_options.sync_token_request_options_provider import (
    SyncTokenRequestOptionsProvider,
)
from airbyte_cdk.sources.declarative.requesters.request_path import RequestPath
from airbyte_cdk.sources.types import StreamSlice


@pytest.mark.parametrize(
    "method_name",
    (
        "get_request_params",
        "get_request_headers",
        "get_request_body_data",
        "get_request_body_json",
    ),
)
def test_request_path_token_option_does_not_inject_request_options(method_name):
    provider = SyncTokenRequestOptionsProvider(
        config={},
        token_option=RequestPath(parameters={}),
        parameters={},
    )

    method = getattr(provider, method_name)
    assert method(stream_slice=StreamSlice(partition={}, cursor_slice={"token": "stored"})) == {}


@pytest.mark.parametrize(
    ("cursor_slice", "initial_token", "config", "expected"),
    (
        (
            {"token": "https://api.example.com/stored"},
            "{{ config['initial'] }}",
            {"initial": "https://api.example.com/initial"},
            "https://api.example.com/stored",
        ),
        (
            {},
            "{{ config['initial'] }}",
            {"initial": "https://api.example.com/initial"},
            "https://api.example.com/initial",
        ),
        ({}, None, {}, None),
    ),
)
def test_get_request_path_prefers_stored_then_initial_token(
    cursor_slice, initial_token, config, expected
):
    provider = SyncTokenRequestOptionsProvider(
        config=config,
        token_option=RequestPath(parameters={}),
        initial_token=initial_token,
        parameters={},
    )

    assert (
        provider.get_request_path(StreamSlice(partition={}, cursor_slice=cursor_slice)) == expected
    )
