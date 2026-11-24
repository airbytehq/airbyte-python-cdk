import pytest


@pytest.fixture(autouse=True)
def allow_unauthenticated_requests(monkeypatch):
    monkeypatch.delenv("AB_JWT_SIGNATURE_SECRET", raising=False)
    monkeypatch.delenv("AB_MANIFEST_SERVER_JWT_SIGNATURE_SECRET", raising=False)
    monkeypatch.setenv("AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED", "true")


@pytest.fixture
def inline_manifest_operations(monkeypatch):
    """Run handler unit tests in-process so their mocks and log capture are visible."""
    from airbyte_cdk.manifest_server.routers import manifest

    def run_operation(operation, request, response_type):
        handler = manifest.WORKER_OPERATIONS[operation][1]
        return handler.__wrapped__(request)

    monkeypatch.setattr(manifest, "run_operation", run_operation)
