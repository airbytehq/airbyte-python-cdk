import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import TestCase
from unittest.mock import Mock, patch

from airbyte_cdk.models import AirbyteStateMessage, ConfiguredAirbyteCatalog, Status
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory as OriginalModelToComponentFactory,
)
from airbyte_cdk.sources.declarative.retrievers.file_uploader.noop_file_writer import NoopFileWriter
from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.entrypoint_wrapper import discover as entrypoint_discover
from airbyte_cdk.test.entrypoint_wrapper import read as entrypoint_read
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import find_binary_response, find_template
from airbyte_cdk.test.standard_tests.models.scenario import ExpectedOutcome
from airbyte_cdk.test.state_builder import StateBuilder


class ConfigBuilder:
    def build(self) -> Dict[str, Any]:
        return {
            "subdomain": "d3v-airbyte",
            "start_date": "2023-01-01T00:00:00Z",
            "credentials": {
                "credentials": "api_token",
                "email": "integration-test@airbyte.io",
                "api_token": "fake_token",
            },
        }


def _source(
    catalog: ConfiguredAirbyteCatalog,
    config: Dict[str, Any],
    state: Optional[List[AirbyteStateMessage]] = None,
    yaml_file: Optional[str] = None,
) -> YamlDeclarativeSource:
    if not yaml_file:
        yaml_file = "file_stream_manifest.yaml"
    return YamlDeclarativeSource(
        path_to_yaml=str(Path(__file__).parent / yaml_file),
        catalog=catalog,
        config=config,
        state=state,
    )


def read(
    config_builder: ConfigBuilder,
    catalog: ConfiguredAirbyteCatalog,
    state_builder: Optional[StateBuilder] = None,
    yaml_file: Optional[str] = None,
    *,
    expected_outcome: ExpectedOutcome = ExpectedOutcome.EXPECT_SUCCESS,
) -> EntrypointOutput:
    config = config_builder.build()
    state = state_builder.build() if state_builder else StateBuilder().build()
    return entrypoint_read(
        _source(catalog, config, state, yaml_file),
        config,
        catalog,
        state,
        expected_outcome=expected_outcome,
    )


def discover(
    config_builder: ConfigBuilder,
    expected_outcome: ExpectedOutcome = ExpectedOutcome.EXPECT_SUCCESS,
) -> EntrypointOutput:
    config = config_builder.build()
    return entrypoint_discover(
        _source(CatalogBuilder().build(), config),
        config,
        expected_outcome=expected_outcome,
    )


SERVER_URL = "https://d3v-airbyte.zendesk.com"
STREAM_URL = f"{SERVER_URL}/api/v2/help_center/incremental/articles?start_time=1672531200"
STREAM_ATTACHMENTS_URL = (
    f"{SERVER_URL}/api/v2/help_center/articles/12138789487375/attachments?per_page=100&=1672531200"
)
STREAM_ATTACHMENT_CONTENT_URL = f"{SERVER_URL}/hc/article_attachments/12138758717583"


class FileStreamTest(TestCase):
    def _config(self) -> ConfigBuilder:
        return ConfigBuilder()

    def test_check(self) -> None:
        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url=STREAM_URL),
                HttpResponse(json.dumps(find_template("file_api/articles", __file__)), 200),
            )

            source = _source(
                CatalogBuilder()
                .with_stream(ConfiguredAirbyteStreamBuilder().with_name("articles"))
                .build(),
                self._config().build(),
            )

            check_result = source.check(Mock(), self._config().build())

            assert check_result.status == Status.SUCCEEDED

    def test_get_articles(self) -> None:
        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url=STREAM_URL),
                HttpResponse(json.dumps(find_template("file_api/articles", __file__)), 200),
            )
            output = read(
                self._config(),
                CatalogBuilder()
                .with_stream(ConfiguredAirbyteStreamBuilder().with_name("articles"))
                .build(),
            )

            assert output.records

    def test_get_article_attachments(self) -> None:
        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url=STREAM_URL),
                HttpResponse(json.dumps(find_template("file_api/articles", __file__)), 200),
            )
            http_mocker.get(
                HttpRequest(url=STREAM_ATTACHMENTS_URL),
                HttpResponse(
                    json.dumps(find_template("file_api/article_attachments", __file__)), 200
                ),
            )
            http_mocker.get(
                HttpRequest(url=STREAM_ATTACHMENT_CONTENT_URL),
                HttpResponse(
                    find_binary_response("file_api/article_attachment_content.png", __file__), 200
                ),
            )

            output = read(
                self._config(),
                CatalogBuilder()
                .with_stream(ConfiguredAirbyteStreamBuilder().with_name("article_attachments"))
                .build(),
            )

            assert output.records
            file_reference = output.records[0].record.file_reference
            assert file_reference
            assert file_reference.staging_file_url
            assert re.match(
                r"^.*/article_attachments/[0-9a-fA-F-]{36}$", file_reference.staging_file_url
            )
            assert file_reference.source_file_relative_path
            assert re.match(
                r"^article_attachments/[0-9a-fA-F-]{36}$", file_reference.source_file_relative_path
            )
            assert file_reference.file_size_bytes

    def test_get_article_attachments_with_filename_extractor(self) -> None:
        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url=STREAM_URL),
                HttpResponse(json.dumps(find_template("file_api/articles", __file__)), 200),
            )
            http_mocker.get(
                HttpRequest(url=STREAM_ATTACHMENTS_URL),
                HttpResponse(
                    json.dumps(find_template("file_api/article_attachments", __file__)), 200
                ),
            )
            http_mocker.get(
                HttpRequest(url=STREAM_ATTACHMENT_CONTENT_URL),
                HttpResponse(
                    find_binary_response("file_api/article_attachment_content.png", __file__), 200
                ),
            )

            output = read(
                self._config(),
                CatalogBuilder()
                .with_stream(ConfiguredAirbyteStreamBuilder().with_name("article_attachments"))
                .build(),
                yaml_file="test_file_stream_with_filename_extractor.yaml",
            )

            assert len(output.records) == 1
            file_reference = output.records[0].record.file_reference
            assert file_reference
            assert (
                file_reference.staging_file_url
                == "/tmp/airbyte-file-transfer/article_attachments/12138758717583/some_image_name.png"
            )
            assert file_reference.source_file_relative_path
            assert not re.match(
                r"^article_attachments/[0-9a-fA-F-]{36}$", file_reference.source_file_relative_path
            )
            assert file_reference.file_size_bytes

    def test_get_article_attachments_messages_for_connector_builder(self) -> None:
        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url=STREAM_URL),
                HttpResponse(json.dumps(find_template("file_api/articles", __file__)), 200),
            )
            http_mocker.get(
                HttpRequest(url=STREAM_ATTACHMENTS_URL),
                HttpResponse(
                    json.dumps(find_template("file_api/article_attachments", __file__)), 200
                ),
            )
            http_mocker.get(
                HttpRequest(url=STREAM_ATTACHMENT_CONTENT_URL),
                HttpResponse(
                    find_binary_response("file_api/article_attachment_content.png", __file__), 200
                ),
            )

            # Define a mock factory that forces emit_connector_builder_messages=True
            class MockModelToComponentFactory(OriginalModelToComponentFactory):
                def __init__(self, *args, **kwargs):
                    kwargs["emit_connector_builder_messages"] = True
                    super().__init__(*args, **kwargs)

            # Patch the factory class where ConcurrentDeclarativeSource (parent of YamlDeclarativeSource) imports it
            with patch(
                "airbyte_cdk.sources.declarative.concurrent_declarative_source.ModelToComponentFactory",
                new=MockModelToComponentFactory,
            ):
                output = read(
                    self._config(),
                    CatalogBuilder()
                    .with_stream(ConfiguredAirbyteStreamBuilder().with_name("article_attachments"))
                    .build(),
                    yaml_file="test_file_stream_with_filename_extractor.yaml",
                )

                assert len(output.records) == 1
                file_reference = output.records[0].record.file_reference
                assert file_reference
                assert file_reference.staging_file_url
                assert file_reference.source_file_relative_path
                # because we didn't write the file, the size is NOOP_FILE_SIZE
                assert file_reference.file_size_bytes == NoopFileWriter.NOOP_FILE_SIZE

                # Assert file reference fields are copied to record data
                record_data = output.records[0].record.data
                assert record_data["staging_file_url"] == file_reference.staging_file_url
                assert (
                    record_data["source_file_relative_path"]
                    == file_reference.source_file_relative_path
                )
                assert record_data["file_size_bytes"] == file_reference.file_size_bytes

    def test_discover_article_attachments(self) -> None:
        output = discover(self._config())

        article_attachments_stream = next(
            filter(
                lambda stream: stream.name == "article_attachments", output.catalog.catalog.streams
            )
        )
        assert article_attachments_stream.is_file_based
