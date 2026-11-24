import pytest


@pytest.fixture(autouse=True)
def allow_unauthenticated_requests(monkeypatch):
    monkeypatch.delenv("AB_JWT_SIGNATURE_SECRET", raising=False)
    monkeypatch.delenv("AB_MANIFEST_SERVER_JWT_SIGNATURE_SECRET", raising=False)
    monkeypatch.setenv("AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED", "true")
