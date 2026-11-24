"""A Builder manifest must not read files outside a package through the schema loader."""

import pytest
from fastapi.testclient import TestClient

from airbyte_cdk.manifest_server.app import app

client = TestClient(app)

MARKER = "FILE_OUTSIDE_PACKAGE_MARKER"
TRAVERSAL_PREFIX = "airbyte_cdk/" + "../" * 20
API_URL = "https://api.example.com/items"


@pytest.fixture(autouse=True)
def no_authentication(monkeypatch):
    monkeypatch.delenv("AB_JWT_SIGNATURE_SECRET", raising=False)
    monkeypatch.setenv("AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED", "true")


@pytest.fixture
def api(requests_mock):
    requests_mock.get(API_URL, json=[{"id": 1}])


@pytest.fixture
def outside_files(tmp_path):
    (tmp_path / "secret.json").write_text(f'{{"leaked": "{MARKER}"}}')
    (tmp_path / "invalid.json").write_text("not json")
    return tmp_path


def _traversal(path):
    return TRAVERSAL_PREFIX + str(path).lstrip("/")


def _manifest(stream_name="items", file_path=None):
    stream = {
        "type": "DeclarativeStream",
        "name": stream_name,
        "primary_key": [],
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {"type": "HttpRequester", "url_base": API_URL, "http_method": "GET"},
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
        },
    }
    if file_path is not None:
        stream["schema_loader"] = {"type": "JsonFileSchemaLoader", "file_path": file_path}
    return {
        "version": "6.48.15",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": [stream_name]},
        "spec": {
            "type": "Spec",
            "connection_specification": {"type": "object", "properties": {}},
        },
        "streams": [stream],
    }


def _discover(manifest):
    return client.post("/v1/manifest/discover", json={"manifest": manifest, "config": {}})


def _test_read(manifest, stream_name="items"):
    return client.post(
        "/v1/manifest/test_read",
        json={"manifest": manifest, "config": {}, "stream_name": stream_name, "record_limit": 1},
    )


@pytest.mark.parametrize("call", [_discover, _test_read], ids=["discover", "test_read"])
def test_explicit_schema_loader_does_not_return_an_outside_file(call, outside_files, api):
    response = call(_manifest(file_path=_traversal(outside_files / "secret.json")))

    assert MARKER not in response.text


def test_default_schema_loader_does_not_return_an_outside_file(outside_files, api, monkeypatch):
    """Without a `schema_loader`, the stream name is interpolated into the default schema path."""
    # The manifest server's default path; other tests can import `source_*` modules that change it.
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.schema.json_file_schema_loader._default_file_path",
        lambda: "./{{parameters['name']}}.json",
    )
    stream_name = _traversal(outside_files / "secret")

    for response in (
        _discover(_manifest(stream_name=stream_name)),
        _test_read(_manifest(stream_name=stream_name), stream_name=stream_name),
    ):
        assert MARKER not in response.text


@pytest.mark.parametrize("call", [_discover, _test_read], ids=["discover", "test_read"])
def test_invalid_and_missing_outside_files_are_indistinguishable(call, outside_files, api):
    responses = {}
    for name in ("invalid.json", "missing.json"):
        file_path = _traversal(outside_files / name)
        response = call(_manifest(file_path=file_path))
        responses[name] = (response.status_code, response.text.replace(file_path, "<file_path>"))

    assert responses["invalid.json"] == responses["missing.json"]
