import hashlib
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from airbyte_cdk.connector_builder.models import StreamRead as CDKStreamRead
from airbyte_cdk.manifest_server.app import app

client = TestClient(app)


class TestManifestRouter:
    """Test cases for the manifest router endpoints."""

    @pytest.fixture
    def sample_manifest(self):
        """Sample manifest for testing."""
        return {
            "version": "6.48.15",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["products"]},
            "definitions": {
                "base_requester": {
                    "type": "HttpRequester",
                    "url_base": "https://dummyjson.com",
                }
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "products",
                    "primary_key": ["id"],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://dummyjson.com",
                            "path": "products",
                            "http_method": "GET",
                        },
                    },
                }
            ],
        }

    @pytest.fixture
    def sample_config(self):
        """Sample config for testing."""
        return {}

    @pytest.fixture
    def mock_source(self):
        """Mock source object."""
        mock_source = Mock()
        mock_source.resolved_manifest = {
            "version": "6.48.15",
            "type": "DeclarativeSource",
            "streams": [{"name": "products", "type": "DeclarativeStream"}],
        }
        mock_source.dynamic_streams = []
        return mock_source

    @pytest.fixture
    def mock_stream_read(self):
        """Mock StreamRead result."""
        return CDKStreamRead(
            logs=[],
            slices=[],
            test_read_limit_reached=False,
            auxiliary_requests=[],
            inferred_schema=None,
            inferred_datetime_formats=None,
            latest_config_update=None,
        )

    @patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_catalog")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_source")
    def test_test_read_endpoint_success(
        self,
        mock_build_source,
        mock_build_catalog,
        mock_runner_class,
        sample_manifest,
        sample_config,
        mock_source,
        mock_stream_read,
    ):
        """Test successful test_read endpoint call."""
        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
            "stream_name": "products",
            "state": [],
            "record_limit": 100,
            "page_limit": 5,
            "slice_limit": 5,
        }

        mock_build_source.return_value = mock_source
        mock_build_catalog.return_value = Mock()

        mock_runner = Mock()
        mock_runner.test_read.return_value = mock_stream_read
        mock_runner_class.return_value = mock_runner

        response = client.post("/v1/manifest/test_read", json=request_data)

        assert response.status_code == 200
        # Verify build_source was called with correct arguments
        mock_build_source.assert_called_once_with(
            sample_manifest,
            mock_build_catalog.return_value,
            sample_config,
            [],
            100,  # record_limit
            5,  # page_limit
            5,  # slice_limit
        )
        mock_build_catalog.assert_called_once_with("products")
        mock_runner.test_read.assert_called_once()

    @patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_catalog")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_source")
    def test_test_read_with_custom_components(
        self,
        mock_build_source,
        mock_build_catalog,
        mock_runner_class,
        sample_manifest,
        sample_config,
        mock_source,
        mock_stream_read,
    ):
        """Test test_read endpoint with custom components code."""
        custom_code = "def custom_function(): pass"
        expected_checksum = hashlib.md5(custom_code.encode()).hexdigest()

        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
            "stream_name": "products",
            "state": [],
            "custom_components_code": custom_code,
            "record_limit": 50,
            "page_limit": 3,
            "slice_limit": 2,
        }

        mock_build_source.return_value = mock_source
        mock_build_catalog.return_value = Mock()

        mock_runner = Mock()
        mock_runner.test_read.return_value = mock_stream_read
        mock_runner_class.return_value = mock_runner

        response = client.post("/v1/manifest/test_read", json=request_data)

        assert response.status_code == 200

        # Verify that build_source was called with config containing custom components
        call_args = mock_build_source.call_args
        config_arg = call_args[0][2]  # Third argument is config
        assert "__injected_components_py" in config_arg
        assert config_arg["__injected_components_py"] == custom_code
        assert "__injected_components_py_checksums" in config_arg
        assert config_arg["__injected_components_py_checksums"]["md5"] == expected_checksum

        # Verify other arguments
        mock_build_source.assert_called_once_with(
            sample_manifest,
            mock_build_catalog.return_value,
            config_arg,
            [],
            50,  # record_limit
            3,  # page_limit
            2,  # slice_limit
        )

    @patch("airbyte_cdk.manifest_server.routers.manifest.AirbyteStateMessageSerializer")
    @patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_catalog")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_source")
    def test_test_read_with_state(
        self,
        mock_build_source,
        mock_build_catalog,
        mock_runner_class,
        mock_serializer,
        sample_manifest,
        sample_config,
        mock_source,
        mock_stream_read,
    ):
        """Test test_read endpoint with state."""
        state_data = [{"type": "STREAM", "stream": {"stream_descriptor": {"name": "products"}}}]

        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
            "stream_name": "products",
            "state": state_data,
        }

        mock_build_source.return_value = mock_source
        mock_build_catalog.return_value = Mock()
        mock_serializer.load.return_value = Mock()

        mock_runner = Mock()
        mock_runner.test_read.return_value = mock_stream_read
        mock_runner_class.return_value = mock_runner

        response = client.post("/v1/manifest/test_read", json=request_data)

        assert response.status_code == 200
        assert mock_serializer.load.call_count == len(state_data)

    def test_test_read_invalid_request(self):
        """Test test_read endpoint with invalid request data."""
        invalid_request = {
            "manifest": {},
            "config": {},
            "stream_name": "test",
            "record_limit": -1,  # Invalid - should be >= 1
        }

        response = client.post("/v1/manifest/test_read", json=invalid_request)
        assert response.status_code == 422  # Validation error

    def test_resolve_endpoint_success(self, sample_manifest, mock_source):
        """Test successful resolve endpoint call."""
        request_data = {"manifest": sample_manifest}

        with patch(
            "airbyte_cdk.manifest_server.routers.manifest.build_source"
        ) as mock_build_source:
            mock_build_source.return_value = mock_source

            response = client.post("/v1/manifest/resolve", json=request_data)

            assert response.status_code == 200
            data = response.json()
            assert "manifest" in data
            assert data["manifest"] == mock_source.resolved_manifest
            mock_build_source.assert_called_once_with(
                sample_manifest,
                None,  # catalog
                {},  # config
                None,  # state
                None,  # record_limit
                None,  # page_limit
                None,  # slice_limit
            )

    def test_resolve_invalid_manifest(self):
        """Test resolve endpoint with invalid manifest."""
        request_data = {}  # Missing required 'manifest' field

        response = client.post("/v1/manifest/resolve", json=request_data)
        assert response.status_code == 422  # Validation error

    def test_full_resolve_endpoint_success(self, sample_manifest, sample_config, mock_source):
        """Test successful full_resolve endpoint call."""
        # Setup mock source with dynamic streams
        mock_source.dynamic_streams = [
            {
                "name": "dynamic_stream_1",
                "dynamic_stream_name": "template_stream",
                "type": "DeclarativeStream",
            },
            {
                "name": "dynamic_stream_2",
                "dynamic_stream_name": "template_stream",
                "type": "DeclarativeStream",
            },
        ]

        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
            "stream_limit": 10,
        }

        with patch(
            "airbyte_cdk.manifest_server.routers.manifest.build_source"
        ) as mock_build_source:
            mock_build_source.return_value = mock_source

            response = client.post("/v1/manifest/full_resolve", json=request_data)

            assert response.status_code == 200
            data = response.json()
            assert "manifest" in data

            # Verify that dynamic streams were added
            streams = data["manifest"]["streams"]
            assert len(streams) >= len(mock_source.resolved_manifest["streams"])

            # Check that dynamic_stream_name is set to None for original streams
            original_stream = next(s for s in streams if s["name"] == "products")
            assert original_stream["dynamic_stream_name"] is None

    def test_full_resolve_with_stream_limit(self, sample_manifest, sample_config, mock_source):
        """Test full_resolve endpoint respects stream_limit."""
        # Create more dynamic streams than the limit
        mock_source.dynamic_streams = [
            {
                "name": f"dynamic_stream_{i}",
                "dynamic_stream_name": "template_stream",
                "type": "DeclarativeStream",
            }
            for i in range(5)  # 5 dynamic streams
        ]

        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
            "stream_limit": 2,  # Limit to 2 streams per template
        }

        with patch(
            "airbyte_cdk.manifest_server.routers.manifest.build_source"
        ) as mock_build_source:
            mock_build_source.return_value = mock_source

            response = client.post("/v1/manifest/full_resolve", json=request_data)

            assert response.status_code == 200
            data = response.json()

            # Count dynamic streams added (should be limited to 2)
            dynamic_streams = [
                s for s in data["manifest"]["streams"] if s["name"].startswith("dynamic_stream_")
            ]
            assert len(dynamic_streams) == 2

    def test_full_resolve_multiple_dynamic_stream_templates(
        self, sample_manifest, sample_config, mock_source
    ):
        """Test full_resolve with multiple dynamic stream templates."""
        mock_source.dynamic_streams = [
            {
                "name": "dynamic_stream_1a",
                "dynamic_stream_name": "template_a",
                "type": "DeclarativeStream",
            },
            {
                "name": "dynamic_stream_1b",
                "dynamic_stream_name": "template_a",
                "type": "DeclarativeStream",
            },
            {
                "name": "dynamic_stream_2a",
                "dynamic_stream_name": "template_b",
                "type": "DeclarativeStream",
            },
        ]

        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
            "stream_limit": 1,  # Only 1 stream per template
        }

        with patch(
            "airbyte_cdk.manifest_server.routers.manifest.build_source"
        ) as mock_build_source:
            mock_build_source.return_value = mock_source

            response = client.post("/v1/manifest/full_resolve", json=request_data)

            assert response.status_code == 200
            data = response.json()

            # Should have 2 dynamic streams (1 from each template)
            dynamic_streams = [
                s for s in data["manifest"]["streams"] if s["name"].startswith("dynamic_stream_")
            ]
            assert len(dynamic_streams) == 2

            # Verify we got one from each template
            template_a_streams = [s for s in dynamic_streams if "1a" in s["name"]]
            template_b_streams = [s for s in dynamic_streams if "2a" in s["name"]]
            assert len(template_a_streams) == 1
            assert len(template_b_streams) == 1

    @patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_source")
    def test_check_endpoint_success(
        self, mock_build_source, mock_runner_class, sample_manifest, sample_config, mock_source
    ):
        """Test successful check endpoint call."""
        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
        }

        mock_build_source.return_value = mock_source

        mock_runner = Mock()
        mock_runner.check_connection.return_value = (True, "Connection successful")
        mock_runner_class.return_value = mock_runner

        response = client.post("/v1/manifest/check", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["message"] == "Connection successful"

        mock_build_source.assert_called_once_with(
            sample_manifest,
            None,  # catalog
            sample_config,
            None,  # state
            None,  # record_limit
            None,  # page_limit
            None,  # slice_limit
        )
        mock_runner_class.assert_called_once_with(mock_source)
        mock_runner.check_connection.assert_called_once_with(sample_config)

    @patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_source")
    def test_check_endpoint_failure(
        self, mock_build_source, mock_runner_class, sample_manifest, sample_config, mock_source
    ):
        """Test check endpoint with connection failure."""
        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
        }

        mock_build_source.return_value = mock_source

        mock_runner = Mock()
        mock_runner.check_connection.return_value = (False, "Invalid API key")
        mock_runner_class.return_value = mock_runner

        response = client.post("/v1/manifest/check", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert data["message"] == "Invalid API key"

        mock_build_source.assert_called_once_with(
            sample_manifest,
            None,  # catalog
            sample_config,
            None,  # state
            None,  # record_limit
            None,  # page_limit
            None,  # slice_limit
        )

    @patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_source")
    def test_discover_endpoint_success(
        self, mock_build_source, mock_runner_class, sample_manifest, sample_config, mock_source
    ):
        """Test successful discover endpoint call."""
        from airbyte_protocol_dataclasses.models import AirbyteCatalog, AirbyteStream

        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
        }

        # Create mock catalog
        mock_catalog = AirbyteCatalog(
            streams=[
                AirbyteStream(
                    name="products",
                    json_schema={"type": "object", "properties": {"id": {"type": "integer"}}},
                    supported_sync_modes=["full_refresh"],
                )
            ]
        )

        mock_build_source.return_value = mock_source

        mock_runner = Mock()
        mock_runner.discover.return_value = mock_catalog
        mock_runner_class.return_value = mock_runner

        response = client.post("/v1/manifest/discover", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert "catalog" in data
        assert data["catalog"]["streams"][0]["name"] == "products"

        mock_build_source.assert_called_once_with(
            sample_manifest,
            None,  # catalog
            sample_config,
            None,  # state
            None,  # record_limit
            None,  # page_limit
            None,  # slice_limit
        )
        mock_runner_class.assert_called_once_with(mock_source)
        mock_runner.discover.assert_called_once_with(sample_config)

    @patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor")
    @patch("airbyte_cdk.manifest_server.routers.manifest.build_source")
    def test_discover_endpoint_missing_catalog(
        self, mock_build_source, mock_runner_class, sample_manifest, sample_config, mock_source
    ):
        """Test discover endpoint with no catalog throws 422 error."""
        request_data = {
            "manifest": sample_manifest,
            "config": sample_config,
        }

        mock_build_source.return_value = mock_source

        mock_runner = Mock()
        mock_runner.discover.return_value = None  # No catalog returned
        mock_runner_class.return_value = mock_runner

        response = client.post("/v1/manifest/discover", json=request_data)

        assert response.status_code == 422
        data = response.json()
        assert "Connector did not return a discovered catalog" in data["detail"]
