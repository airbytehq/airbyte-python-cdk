import socket
import threading
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from airbyte_cdk.manifest_server.app import app

client = TestClient(app)


@pytest.fixture
def stalled_server_url():
    """A server that accepts connections and never sends a response."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()
    connections = []
    stop = threading.Event()

    def accept_forever():
        server.settimeout(0.1)
        while not stop.is_set():
            try:
                connections.append(server.accept()[0])
            except socket.timeout:
                continue

    thread = threading.Thread(target=accept_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.getsockname()[1]}"
    stop.set()
    thread.join()
    for connection in connections:
        connection.close()
    server.close()


def _manifest(url_base):
    return {
        "version": "6.48.15",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["items"]},
        "spec": {
            "type": "Spec",
            "connection_specification": {"type": "object", "properties": {}},
        },
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "items",
                "primary_key": [],
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": url_base,
                        "path": "/items",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object", "properties": {}},
                },
            }
        ],
    }


@patch(
    "airbyte_cdk.sources.declarative.parsers.model_to_component_factory._CONNECTOR_BUILDER_REQUEST_DEADLINE_SECONDS",
    2.0,
)
def test_test_read_against_stalled_api_ends_at_the_deadline(stalled_server_url):
    started = time.monotonic()
    response = client.post(
        "/v1/manifest/test_read",
        json={"manifest": _manifest(stalled_server_url), "config": {}, "stream_name": "items"},
    )
    elapsed = time.monotonic() - started

    assert response.status_code == 200
    errors = [log["message"] for log in response.json()["logs"] if log["level"] == "ERROR"]
    assert any("Read timed out" in message for message in errors), errors
    assert elapsed < 15


@pytest.mark.parametrize("value", ["abc", "inf", "nan", "0", "-5"])
def test_invalid_deadline_override_is_rejected(monkeypatch, value):
    from airbyte_cdk.sources.declarative.parsers import model_to_component_factory

    monkeypatch.setenv("AIRBYTE_CONNECTOR_BUILDER_REQUEST_DEADLINE_SECONDS", value)

    with pytest.raises(ValueError, match="must be a positive number of seconds"):
        model_to_component_factory._connector_builder_request_deadline_seconds()


def test_deadline_override_is_read(monkeypatch):
    from airbyte_cdk.sources.declarative.parsers import model_to_component_factory

    monkeypatch.setenv("AIRBYTE_CONNECTOR_BUILDER_REQUEST_DEADLINE_SECONDS", "120")

    assert model_to_component_factory._connector_builder_request_deadline_seconds() == 120.0
