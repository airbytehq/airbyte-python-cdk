from fastapi.testclient import TestClient

from airbyte_cdk.manifest_server.app import app

client = TestClient(app, follow_redirects=False)


def test_health_endpoint_without_trailing_slash() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_endpoint_with_trailing_slash() -> None:
    response = client.get("/health/")

    assert response.status_code == 200
