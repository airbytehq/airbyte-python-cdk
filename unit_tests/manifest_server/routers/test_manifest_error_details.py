import logging
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from airbyte_cdk.manifest_server.app import app
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

pytestmark = pytest.mark.usefixtures("inline_manifest_operations")

client = TestClient(app)

INTERNAL_MARKER = "/var/run/secrets/kubernetes.io/serviceaccount/token"
MANIFEST = {"version": "6.0.0", "type": "DeclarativeSource", "streams": []}

ROUTES = [
    pytest.param(
        "/v1/manifest/test_read",
        {"manifest": MANIFEST, "config": {}, "stream_name": "products"},
        "test_read",
        id="test_read",
    ),
    pytest.param(
        "/v1/manifest/check", {"manifest": MANIFEST, "config": {}}, "check_connection", id="check"
    ),
    pytest.param(
        "/v1/manifest/discover", {"manifest": MANIFEST, "config": {}}, "discover", id="discover"
    ),
    pytest.param("/v1/manifest/resolve", {"manifest": MANIFEST}, None, id="resolve"),
    pytest.param(
        "/v1/manifest/full_resolve",
        {"manifest": MANIFEST, "config": {}, "stream_limit": 1},
        None,
        id="full_resolve",
    ),
]


def _post_with_error(route, body, runner_method, error):
    with (
        patch("airbyte_cdk.manifest_server.routers.manifest.build_source") as build_source,
        patch(
            "airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor"
        ) as runner_class,
    ):
        if runner_method is None:
            build_source.side_effect = error
        else:
            runner = Mock()
            getattr(runner, runner_method).side_effect = error
            runner_class.return_value = runner
        return client.post(route, json=body)


@pytest.mark.parametrize("route,body,runner_method", ROUTES)
@pytest.mark.parametrize(
    "error",
    [
        pytest.param(PermissionError(13, "Permission denied", INTERNAL_MARKER), id="os_error"),
        pytest.param(
            RuntimeError(f"internal failure reading {INTERNAL_MARKER}"), id="runtime_error"
        ),
        pytest.param(KeyError(INTERNAL_MARKER), id="key_error"),
    ],
)
def test_internal_exception_text_is_not_returned(route, body, runner_method, error, caplog):
    with caplog.at_level(logging.ERROR, logger="airbyte.manifest-server"):
        response = _post_with_error(route, body, runner_method, error)

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert INTERNAL_MARKER not in detail
    assert "an internal error occurred (error id " in detail
    error_id = detail.split("(error id ")[1].split(")")[0]
    assert any(
        error_id in r.getMessage() and INTERNAL_MARKER in r.getMessage() for r in caplog.records
    )


@pytest.mark.parametrize("route,body,runner_method", ROUTES)
def test_traced_exception_returns_only_its_user_facing_message(route, body, runner_method):
    error = AirbyteTracedException(
        message="The stream 'products' could not be read.",
        internal_message=f"internal details {INTERNAL_MARKER}",
    )

    response = _post_with_error(route, body, runner_method, error)

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "The stream 'products' could not be read." in detail
    assert INTERNAL_MARKER not in detail


@pytest.mark.parametrize("route,body,runner_method", ROUTES)
def test_manifest_value_error_is_still_returned(route, body, runner_method):
    response = _post_with_error(
        route, body, runner_method, ValueError("Unknown field 'pagination_strategy'")
    )

    assert response.status_code == 400
    assert "Unknown field 'pagination_strategy'" in response.json()["detail"]
