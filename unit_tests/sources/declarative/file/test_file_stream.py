import re

from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import TestCase
from unittest.mock import Mock

from airbyte_cdk.models import AirbyteStateMessage, ConfiguredAirbyteCatalog, Status
from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.entrypoint_wrapper import discover as entrypoint_discover
from airbyte_cdk.test.entrypoint_wrapper import read as entrypoint_read
from airbyte_cdk.test.state_builder import StateBuilder


class ConfigBuilder:
    def build(self) -> Dict[str, Any]:
        return {
            "subdomain": "d3v-airbyte",
            "start_date": "2023-01-01T00:00:00Z",
            "credentials": {
                "credentials": "api_token",
                "email": "integration-test@airbyte.io",
                "api_token": "fake token",
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
    expecting_exception: bool = False,
    yaml_file: Optional[str] = None,
) -> EntrypointOutput:
    config = config_builder.build()
    state = state_builder.build() if state_builder else StateBuilder().build()
    return entrypoint_read(
        _source(catalog, config, state, yaml_file), config, catalog, state, expecting_exception
    )


def discover(config_builder: ConfigBuilder, expecting_exception: bool = False) -> EntrypointOutput:
    config = config_builder.build()
    return entrypoint_discover(
        _source(CatalogBuilder().build(), config), config, expecting_exception
    )


class FileStreamTest(TestCase):
    def _config(self) -> ConfigBuilder:
        return ConfigBuilder()

    def test_check(self) -> None:
        source = _source(
            CatalogBuilder()
            .with_stream(ConfiguredAirbyteStreamBuilder().with_name("articles"))
            .build(),
            self._config().build(),
        )

        check_result = source.check(Mock(), self._config().build())

        assert check_result.status == Status.SUCCEEDED

    def test_get_articles(self) -> None:
        output = read(
            self._config(),
            CatalogBuilder()
            .with_stream(ConfiguredAirbyteStreamBuilder().with_name("articles"))
            .build(),
        )

        assert output.records

    def test_get_article_attachments(self) -> None:
        output = read(
            self._config(),
            CatalogBuilder()
            .with_stream(ConfiguredAirbyteStreamBuilder().with_name("article_attachments"))
            .build(),
        )

        assert output.records
        file_reference = output.records[0].record.file_reference
        assert file_reference
        assert file_reference.file_url
        assert re.match(r"^.*/article_attachments/[0-9a-fA-F-]{36}$", file_reference.file_url)
        assert file_reference.file_relative_path
        assert re.match(
            r"^article_attachments/[0-9a-fA-F-]{36}$", file_reference.file_relative_path
        )
        assert file_reference.file_size_bytes

    def test_get_article_attachments_with_filename_extractor(self) -> None:
        output = read(
            self._config(),
            CatalogBuilder()
            .with_stream(ConfiguredAirbyteStreamBuilder().with_name("article_attachments"))
            .build(),
            yaml_file="test_file_stream_with_filename_extractor.yaml",
        )

        assert output.records
        file_reference = output.records[0].record.file_reference
        assert file_reference
        assert file_reference.file_url
        # todo: once we finally mock the response update to check file name
        assert not re.match(r"^.*/article_attachments/[0-9a-fA-F-]{36}$", file_reference.file_url)
        assert file_reference.file_relative_path
        assert not re.match(
            r"^article_attachments/[0-9a-fA-F-]{36}$", file_reference.file_relative_path
        )
        assert file_reference.file_size_bytes

    def test_discover_article_attachments(self) -> None:
        output = discover(self._config())

        article_attachments_stream = next(
            filter(
                lambda stream: stream.name == "article_attachments", output.catalog.catalog.streams
            )
        )
        assert article_attachments_stream.is_file_based
