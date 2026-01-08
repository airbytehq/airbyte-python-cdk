#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
import os
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, List, Mapping
from unittest.mock import Mock, call, mock_open, patch

import pytest
import requests
import yaml
from jsonschema.exceptions import ValidationError

import unit_tests.sources.declarative.external_component  # Needed for dynamic imports to work
from airbyte_cdk.legacy.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)
from airbyte_cdk.models import (
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Level,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from unit_tests.sources.declarative.parsers.test_model_to_component_factory import get_retriever

logger = logging.getLogger("airbyte")

EXTERNAL_CONNECTION_SPECIFICATION = {
    "type": "object",
    "required": ["api_token"],
    "additionalProperties": False,
    "properties": {"api_token": {"type": "string"}},
}


class MockManifestDeclarativeSource(ManifestDeclarativeSource):
    """
    Mock test class that is needed to monkey patch how we read from various files that make up a declarative source because of how our
    tests write configuration files during testing. It is also used to properly namespace where files get written in specific
    cases like when we temporarily write files like spec.yaml to the package unit_tests, which is the directory where it will
    be read in during the tests.
    """


class TestManifestDeclarativeSource:
    @pytest.fixture
    def use_external_yaml_spec(self):
        # Our way of resolving the absolute path to root of the airbyte-cdk unit test directory where spec.yaml files should
        # be written to (i.e. ~/airbyte/airbyte-cdk/python/unit-tests) because that is where they are read from during testing.
        module = sys.modules[__name__]
        module_path = os.path.abspath(module.__file__)
        test_path = os.path.dirname(module_path)
        spec_root = test_path.split("/legacy/sources/declarative")[0]

        spec = {
            "documentationUrl": "https://airbyte.com/#yaml-from-external",
            "connectionSpecification": EXTERNAL_CONNECTION_SPECIFICATION,
        }

        yaml_path = os.path.join(spec_root, "spec.yaml")
        with open(yaml_path, "w") as f:
            f.write(yaml.dump(spec))
        yield
        os.remove(yaml_path)

    @pytest.fixture
    def _base_manifest(self):
        """Base manifest without streams or dynamic streams."""
        return {
            "version": "3.8.2",
            "description": "This is a sample source connector that is very valid.",
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }

    @pytest.fixture
    def _declarative_stream(self):
        def declarative_stream_config(
            name="lists", requester_type="HttpRequester", custom_requester=None
        ):
            """Generates a DeclarativeStream configuration."""
            requester_config = {
                "type": requester_type,
                "path": "/v3/marketing/lists",
                "authenticator": {
                    "type": "BearerAuthenticator",
                    "api_token": "{{ config.apikey }}",
                },
                "request_parameters": {"page_size": "{{ 10 }}"},
            }
            if custom_requester:
                requester_config.update(custom_requester)

            return {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": name,
                    "primary_key": "id",
                    "url_base": "https://api.sendgrid.com",
                },
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": f"./source_sendgrid/schemas/{{{{ parameters.name }}}}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                            "page_size": 10,
                        },
                    },
                    "requester": requester_config,
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            }

        return declarative_stream_config

    @pytest.fixture
    def _dynamic_declarative_stream(self, _declarative_stream):
        """Generates a DynamicDeclarativeStream configuration."""
        return {
            "type": "DynamicDeclarativeStream",
            "stream_template": _declarative_stream(),
            "components_resolver": {
                "type": "HttpComponentsResolver",
                "$parameters": {
                    "name": "lists",
                    "primary_key": "id",
                    "url_base": "https://api.sendgrid.com",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                            "page_size": 10,
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{ components_value['name'] }}",
                    }
                ],
            },
        }

    def test_valid_manifest(self):
        manifest = {
            "version": "3.8.2",
            "definitions": {},
            "description": "This is a sample source connector that is very valid.",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "stream_with_custom_requester",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "type": "CustomRequester",
                            "class_name": "unit_tests.sources.declarative.external_component.SampleCustomComponent",
                            "path": "/v3/marketing/lists",
                            "custom_request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        assert "unit_tests" in sys.modules
        assert "unit_tests.sources" in sys.modules
        assert "unit_tests.sources.declarative" in sys.modules
        assert "unit_tests.sources.declarative.external_component" in sys.modules

        source = ManifestDeclarativeSource(source_config=manifest)

        check_stream = source.connection_checker
        check_stream.check_connection(source, logging.getLogger(""), {})

        streams = source.streams({})
        assert len(streams) == 2
        assert isinstance(streams[0], DefaultStream)
        assert isinstance(streams[1], DefaultStream)
        assert (
            source.resolved_manifest["description"]
            == "This is a sample source connector that is very valid."
        )

    def test_manifest_with_spec(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
            "spec": {
                "type": "Spec",
                "documentation_url": "https://airbyte.com/#yaml-from-manifest",
                "connection_specification": {
                    "title": "Test Spec",
                    "type": "object",
                    "required": ["api_key"],
                    "additionalProperties": False,
                    "properties": {
                        "api_key": {
                            "type": "string",
                            "airbyte_secret": True,
                            "title": "API Key",
                            "description": "Test API Key",
                            "order": 0,
                        }
                    },
                },
            },
        }
        source = ManifestDeclarativeSource(source_config=manifest)
        connector_specification = source.spec(logger)
        assert connector_specification is not None
        assert connector_specification.documentationUrl == "https://airbyte.com/#yaml-from-manifest"
        assert connector_specification.connectionSpecification["title"] == "Test Spec"
        assert connector_specification.connectionSpecification["required"][0] == "api_key"
        assert connector_specification.connectionSpecification["additionalProperties"] is False
        assert connector_specification.connectionSpecification["properties"]["api_key"] == {
            "type": "string",
            "airbyte_secret": True,
            "title": "API Key",
            "description": "Test API Key",
            "order": 0,
        }

    def test_manifest_with_external_spec(self, use_external_yaml_spec):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        source = MockManifestDeclarativeSource(source_config=manifest)

        connector_specification = source.spec(logger)

        assert connector_specification.documentationUrl == "https://airbyte.com/#yaml-from-external"
        assert connector_specification.connectionSpecification == EXTERNAL_CONNECTION_SPECIFICATION

    def test_source_is_not_created_if_toplevel_fields_are_unknown(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": 10},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
            "not_a_valid_field": "error",
        }
        with pytest.raises(ValidationError):
            ManifestDeclarativeSource(source_config=manifest)

    def test_source_missing_checker_fails_validation(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": 10},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
        }
        with pytest.raises(ValidationError):
            ManifestDeclarativeSource(source_config=manifest)

    def test_source_with_missing_streams_and_dynamic_streams_fails(
        self, _base_manifest, _dynamic_declarative_stream, _declarative_stream
    ):
        # test case for manifest without streams or dynamic streams
        manifest_without_streams_and_dynamic_streams = _base_manifest
        with pytest.raises(ValidationError):
            ManifestDeclarativeSource(source_config=manifest_without_streams_and_dynamic_streams)

        # test case for manifest with streams
        manifest_with_streams = {
            **manifest_without_streams_and_dynamic_streams,
            "streams": [
                _declarative_stream(name="lists"),
                _declarative_stream(
                    name="stream_with_custom_requester",
                    requester_type="CustomRequester",
                    custom_requester={
                        "class_name": "unit_tests.sources.declarative.external_component.SampleCustomComponent",
                        "custom_request_parameters": {"page_size": 10},
                    },
                ),
            ],
        }
        ManifestDeclarativeSource(source_config=manifest_with_streams)

        # test case for manifest with dynamic streams
        manifest_with_dynamic_streams = {
            **manifest_without_streams_and_dynamic_streams,
            "dynamic_streams": [_dynamic_declarative_stream],
        }
        ManifestDeclarativeSource(source_config=manifest_with_dynamic_streams)

    def test_source_with_missing_version_fails(self):
        manifest = {
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": 10},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        with pytest.raises(ValidationError):
            ManifestDeclarativeSource(source_config=manifest)

    @pytest.mark.parametrize(
        "cdk_version, manifest_version, expected_error",
        [
            pytest.param(
                "0.35.0", "0.30.0", None, id="manifest_version_less_than_cdk_package_should_run"
            ),
            pytest.param(
                "1.5.0",
                "0.29.0",
                None,
                id="manifest_version_less_than_cdk_major_package_should_run",
            ),
            pytest.param(
                "0.29.0", "0.29.0", None, id="manifest_version_matching_cdk_package_should_run"
            ),
            pytest.param(
                "0.29.0",
                "0.25.0",
                ValidationError,
                id="manifest_version_before_beta_that_uses_the_beta_0.29.0_cdk_package_should_throw_error",
            ),
            pytest.param(
                "1.5.0",
                "0.25.0",
                ValidationError,
                id="manifest_version_before_beta_that_uses_package_later_major_version_than_beta_0.29.0_cdk_package_should_throw_error",
            ),
            pytest.param(
                "0.34.0",
                "0.35.0",
                ValidationError,
                id="manifest_version_greater_than_cdk_package_should_throw_error",
            ),
            pytest.param(
                "0.29.0", "-1.5.0", ValidationError, id="manifest_version_has_invalid_major_format"
            ),
            pytest.param(
                "0.29.0",
                "0.invalid.0",
                ValidationError,
                id="manifest_version_has_invalid_minor_format",
            ),
            pytest.param(
                "0.29.0",
                "0.29.0rc1",
                None,
                id="manifest_version_is_release_candidate",
            ),
            pytest.param(
                "0.29.0rc1",
                "0.29.0",
                None,
                id="cdk_version_is_release_candidate",
            ),
            pytest.param(
                "0.29.0",
                "0.29.0.0.3",  # packaging library does not complain and the parts are ignored during comparisons.
                None,
                id="manifest_version_has_extra_version_parts",
            ),
            pytest.param(
                "0.29.0", "5.0", ValidationError, id="manifest_version_has_too_few_version_parts"
            ),
            pytest.param(
                "0.29.0:dev", "0.29.0", ValidationError, id="manifest_version_has_extra_release"
            ),
        ],
    )
    @patch("importlib.metadata.version")
    def test_manifest_versions(
        self,
        version,
        cdk_version,
        manifest_version,
        expected_error,
    ) -> None:
        # Used to mock the metadata.version() for test scenarios which normally returns the actual version of the airbyte-cdk package
        version.return_value = cdk_version

        manifest = {
            "version": manifest_version,
            "definitions": {},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "stream_with_custom_requester",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "type": "CustomRequester",
                            "class_name": "unit_tests.sources.declarative.external_component.SampleCustomComponent",
                            "path": "/v3/marketing/lists",
                            "custom_request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        if expected_error:
            with pytest.raises(expected_error):
                ManifestDeclarativeSource(source_config=manifest)
        else:
            ManifestDeclarativeSource(source_config=manifest)

    def test_source_with_invalid_stream_config_fails_validation(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                }
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        with pytest.raises(ValidationError):
            ManifestDeclarativeSource(source_config=manifest)

    def test_source_with_no_external_spec_and_no_in_yaml_spec_fails(self):
        manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "{{ 10 }}"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        source = ManifestDeclarativeSource(source_config=manifest)

        # We expect to fail here because we have not created a temporary spec.yaml file
        with pytest.raises(FileNotFoundError):
            source.spec(logger)

    @patch("airbyte_cdk.legacy.sources.declarative.declarative_source.DeclarativeSource.read")
    def test_given_debug_when_read_then_set_log_level(self, declarative_source_read):
        any_valid_manifest = {
            "version": "0.29.3",
            "definitions": {
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "page_size",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ response._metadata.next }}",
                        },
                    },
                    "requester": {
                        "path": "/v3/marketing/lists",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config.apikey }}",
                        },
                        "request_parameters": {"page_size": "10"},
                    },
                    "record_selector": {"extractor": {"field_path": ["result"]}},
                },
            },
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "lists",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v3/marketing/lists",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "stream_with_custom_requester",
                        "primary_key": "id",
                        "url_base": "https://api.sendgrid.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "type": "CustomRequester",
                            "class_name": "unit_tests.sources.declarative.external_component.SampleCustomComponent",
                            "path": "/v3/marketing/lists",
                            "custom_request_parameters": {"page_size": 10},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }
        source = ManifestDeclarativeSource(source_config=any_valid_manifest, debug=True)

        debug_logger = logging.getLogger("logger.debug")
        list(source.read(debug_logger, {}, {}, {}))

        assert debug_logger.isEnabledFor(logging.DEBUG)

    @pytest.mark.parametrize(
        "is_sandbox, expected_stream_count",
        [
            pytest.param(True, 3, id="test_sandbox_config_includes_conditional_streams"),
            pytest.param(False, 1, id="test_non_sandbox_config_skips_conditional_streams"),
        ],
    )
    def test_conditional_streams_manifest(self, is_sandbox, expected_stream_count):
        manifest = {
            "version": "3.8.2",
            "definitions": {},
            "description": "This is a sample source connector that is very valid.",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "students",
                        "primary_key": "id",
                        "url_base": "https://api.yasogamihighschool.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v1/students",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "ConditionalStreams",
                    "condition": "{{ config['is_sandbox'] }}",
                    "streams": [
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "classrooms",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "name": "{{ parameters.stream_name }}",
                                "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/classrooms",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                    "request_parameters": {"page_size": "{{ 10 }}"},
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "clubs",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "name": "{{ parameters.stream_name }}",
                                "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/clubs",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                    "request_parameters": {"page_size": "{{ 10 }}"},
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                    ],
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["students"]},
        }

        assert "unit_tests" in sys.modules
        assert "unit_tests.sources" in sys.modules
        assert "unit_tests.sources.declarative" in sys.modules
        assert "unit_tests.sources.declarative.external_component" in sys.modules

        config = {"is_sandbox": is_sandbox}

        source = ManifestDeclarativeSource(source_config=manifest)

        check_stream = source.connection_checker
        check_stream.check_connection(source, logging.getLogger(""), config=config)

        actual_streams = source.streams(config=config)
        assert len(actual_streams) == expected_stream_count
        assert isinstance(actual_streams[0], DefaultStream)
        assert actual_streams[0].name == "students"

        if is_sandbox:
            assert isinstance(actual_streams[1], DefaultStream)
            assert actual_streams[1].name == "classrooms"
            assert isinstance(actual_streams[2], DefaultStream)
            assert actual_streams[2].name == "clubs"

        assert (
            source.resolved_manifest["description"]
            == "This is a sample source connector that is very valid."
        )

    @pytest.mark.parametrize(
        "field_to_remove,expected_error",
        [
            pytest.param("condition", ValidationError, id="test_no_condition_raises_error"),
            pytest.param("streams", ValidationError, id="test_no_streams_raises_error"),
        ],
    )
    def test_conditional_streams_invalid_manifest(self, field_to_remove, expected_error):
        manifest = {
            "version": "3.8.2",
            "definitions": {},
            "description": "This is a sample source connector that is very valid.",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "students",
                        "primary_key": "id",
                        "url_base": "https://api.yasogamihighschool.com",
                    },
                    "schema_loader": {
                        "name": "{{ parameters.stream_name }}",
                        "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_size": 10,
                            "page_size_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page_size",
                            },
                            "page_token_option": {"type": "RequestPath"},
                            "pagination_strategy": {
                                "type": "CursorPagination",
                                "cursor_value": "{{ response._metadata.next }}",
                                "page_size": 10,
                            },
                        },
                        "requester": {
                            "path": "/v1/students",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "request_parameters": {"page_size": "{{ 10 }}"},
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                },
                {
                    "type": "ConditionalStreams",
                    "condition": "{{ config['is_sandbox'] }}",
                    "streams": [
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "classrooms",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "name": "{{ parameters.stream_name }}",
                                "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/classrooms",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                    "request_parameters": {"page_size": "{{ 10 }}"},
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                        {
                            "type": "DeclarativeStream",
                            "$parameters": {
                                "name": "clubs",
                                "primary_key": "id",
                                "url_base": "https://api.yasogamihighschool.com",
                            },
                            "schema_loader": {
                                "name": "{{ parameters.stream_name }}",
                                "file_path": "./source_yasogami_high_school/schemas/{{ parameters.name }}.yaml",
                            },
                            "retriever": {
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_size": 10,
                                    "page_size_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "page_size",
                                    },
                                    "page_token_option": {"type": "RequestPath"},
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": "{{ response._metadata.next }}",
                                        "page_size": 10,
                                    },
                                },
                                "requester": {
                                    "path": "/v1/clubs",
                                    "authenticator": {
                                        "type": "BearerAuthenticator",
                                        "api_token": "{{ config.apikey }}",
                                    },
                                    "request_parameters": {"page_size": "{{ 10 }}"},
                                },
                                "record_selector": {"extractor": {"field_path": ["result"]}},
                            },
                        },
                    ],
                },
            ],
            "check": {"type": "CheckStream", "stream_names": ["students"]},
        }

        assert "unit_tests" in sys.modules
        assert "unit_tests.sources" in sys.modules
        assert "unit_tests.sources.declarative" in sys.modules
        assert "unit_tests.sources.declarative.external_component" in sys.modules

        del manifest["streams"][1][field_to_remove]

        with pytest.raises(ValidationError):
            ManifestDeclarativeSource(source_config=manifest)


def request_log_message(request: dict) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.LOG,
        log=AirbyteLogMessage(level=Level.INFO, message=f"request:{json.dumps(request)}"),
    )


def response_log_message(response: dict) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.LOG,
        log=AirbyteLogMessage(level=Level.INFO, message=f"response:{json.dumps(response)}"),
    )


def _create_request():
    url = "https://example.com/api"
    headers = {"Content-Type": "application/json"}
    return requests.Request("POST", url, headers=headers, json={"key": "value"}).prepare()


def _create_response(body):
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(json.dumps(body), "utf-8")
    response.headers["Content-Type"] = "application/json"
    return response


def _create_page(response_body):
    response = _create_response(response_body)
    response.request = _create_request()
    return response


def test_only_parent_streams_use_cache():
    applications_stream = {
        "type": "DeclarativeStream",
        "$parameters": {
            "name": "applications",
            "primary_key": "id",
            "url_base": "https://harvest.greenhouse.io/v1/",
        },
        "schema_loader": {
            "name": "{{ parameters.stream_name }}",
            "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
        },
        "retriever": {
            "paginator": {
                "type": "DefaultPaginator",
                "page_size": 10,
                "page_size_option": {
                    "type": "RequestOption",
                    "inject_into": "request_parameter",
                    "field_name": "per_page",
                },
                "page_token_option": {"type": "RequestPath"},
                "pagination_strategy": {
                    "type": "CursorPagination",
                    "cursor_value": "{{ headers['link']['next']['url'] }}",
                    "stop_condition": "{{ 'next' not in headers['link'] }}",
                    "page_size": 100,
                },
            },
            "requester": {
                "path": "applications",
                "authenticator": {
                    "type": "BasicHttpAuthenticator",
                    "username": "{{ config['api_key'] }}",
                },
            },
            "record_selector": {"extractor": {"type": "DpathExtractor", "field_path": []}},
        },
    }

    manifest = {
        "version": "0.29.3",
        "definitions": {},
        "streams": [
            deepcopy(applications_stream),
            {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": "applications_interviews",
                    "primary_key": "id",
                    "url_base": "https://harvest.greenhouse.io/v1/",
                },
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "per_page",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ headers['link']['next']['url'] }}",
                            "stop_condition": "{{ 'next' not in headers['link'] }}",
                            "page_size": 100,
                        },
                    },
                    "requester": {
                        "path": "applications_interviews",
                        "authenticator": {
                            "type": "BasicHttpAuthenticator",
                            "username": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {"extractor": {"type": "DpathExtractor", "field_path": []}},
                    "partition_router": {
                        "parent_stream_configs": [
                            {
                                "parent_key": "id",
                                "partition_field": "parent_id",
                                "stream": deepcopy(applications_stream),
                            }
                        ],
                        "type": "SubstreamPartitionRouter",
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": "jobs",
                    "primary_key": "id",
                    "url_base": "https://harvest.greenhouse.io/v1/",
                },
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_sendgrid/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {
                        "type": "DefaultPaginator",
                        "page_size": 10,
                        "page_size_option": {
                            "type": "RequestOption",
                            "inject_into": "request_parameter",
                            "field_name": "per_page",
                        },
                        "page_token_option": {"type": "RequestPath"},
                        "pagination_strategy": {
                            "type": "CursorPagination",
                            "cursor_value": "{{ headers['link']['next']['url'] }}",
                            "stop_condition": "{{ 'next' not in headers['link'] }}",
                            "page_size": 100,
                        },
                    },
                    "requester": {
                        "path": "jobs",
                        "authenticator": {
                            "type": "BasicHttpAuthenticator",
                            "username": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {"extractor": {"type": "DpathExtractor", "field_path": []}},
                },
            },
        ],
        "check": {"type": "CheckStream", "stream_names": ["applications"]},
    }
    source = ManifestDeclarativeSource(source_config=manifest)

    streams = source.streams({})
    assert len(streams) == 3

    # Main stream with caching (parent for substream `applications_interviews`)
    assert streams[0].name == "applications"
    assert get_retriever(streams[0]).requester.use_cache

    # Substream
    assert streams[1].name == "applications_interviews"

    stream_1_retriever = get_retriever(streams[1])
    assert not stream_1_retriever.requester.use_cache

    # Parent stream created for substream
    stream_slicer = streams[1]._stream_partition_generator._stream_slicer
    assert stream_slicer.parent_stream_configs[0].stream.name == "applications"
    assert stream_slicer.parent_stream_configs[
        0
    ].stream._stream_partition_generator._partition_factory._retriever.requester.use_cache

    # Main stream without caching
    assert streams[2].name == "jobs"
    assert not get_retriever(streams[2]).requester.use_cache


def test_parent_stream_respects_explicit_use_cache_false():
    """Test that explicit use_cache: false is respected for parent streams.

    This is important for APIs that use scroll-based pagination (like Intercom's /companies/scroll
    endpoint), where caching must be disabled because the same scroll_param is returned in
    pagination responses, causing duplicate records and infinite pagination loops.
    """
    # Parent stream with explicit use_cache: false
    companies_stream = {
        "type": "DeclarativeStream",
        "$parameters": {
            "name": "companies",
            "primary_key": "id",
            "url_base": "https://api.intercom.io/",
        },
        "schema_loader": {
            "name": "{{ parameters.stream_name }}",
            "file_path": "./source_intercom/schemas/{{ parameters.name }}.yaml",
        },
        "retriever": {
            "paginator": {
                "type": "DefaultPaginator",
                "page_token_option": {"type": "RequestPath"},
                "pagination_strategy": {
                    "type": "CursorPagination",
                    "cursor_value": "{{ response.get('scroll_param') }}",
                    "page_size": 100,
                },
            },
            "requester": {
                "path": "companies/scroll",
                "use_cache": False,  # Explicitly disabled for scroll-based pagination
                "authenticator": {
                    "type": "BearerAuthenticator",
                    "api_token": "{{ config['api_key'] }}",
                },
            },
            "record_selector": {"extractor": {"type": "DpathExtractor", "field_path": ["data"]}},
        },
    }

    manifest = {
        "version": "0.29.3",
        "definitions": {},
        "streams": [
            deepcopy(companies_stream),
            {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": "company_segments",
                    "primary_key": "id",
                    "url_base": "https://api.intercom.io/",
                },
                "schema_loader": {
                    "name": "{{ parameters.stream_name }}",
                    "file_path": "./source_intercom/schemas/{{ parameters.name }}.yaml",
                },
                "retriever": {
                    "paginator": {"type": "NoPagination"},
                    "requester": {
                        "path": "companies/{{ stream_partition.parent_id }}/segments",
                        "authenticator": {
                            "type": "BearerAuthenticator",
                            "api_token": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {
                        "extractor": {"type": "DpathExtractor", "field_path": ["data"]}
                    },
                    "partition_router": {
                        "parent_stream_configs": [
                            {
                                "parent_key": "id",
                                "partition_field": "parent_id",
                                "stream": deepcopy(companies_stream),
                            }
                        ],
                        "type": "SubstreamPartitionRouter",
                    },
                },
            },
        ],
        "check": {"type": "CheckStream", "stream_names": ["companies"]},
    }
    source = ManifestDeclarativeSource(source_config=manifest)

    streams = source.streams({})
    assert len(streams) == 2

    # Main stream with explicit use_cache: false should remain false (parent for substream)
    assert streams[0].name == "companies"
    # use_cache should remain False because it was explicitly set to False
    assert not get_retriever(streams[0]).requester.use_cache

    # Substream
    assert streams[1].name == "company_segments"

    # Parent stream created for substream should also respect use_cache: false
    stream_slicer = streams[1]._stream_partition_generator._stream_slicer
    assert stream_slicer.parent_stream_configs[0].stream.name == "companies"
    # The parent stream in the substream config should also have use_cache: false
    assert not stream_slicer.parent_stream_configs[
        0
    ].stream._stream_partition_generator._partition_factory._retriever.requester.use_cache


def _run_read(manifest: Mapping[str, Any], stream_name: str) -> List[AirbyteMessage]:
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=stream_name, json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            )
        ]
    )
    config = {}
    state = {}
    source = ConcurrentDeclarativeSource(
        catalog=catalog,
        config=config,
        source_config=manifest,
        state=state,
    )
    return list(source.read(logger, {}, catalog, state))


def test_declarative_component_schema_valid_ref_links():
    def load_yaml(file_path) -> Mapping[str, Any]:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)

    def extract_refs(data, base_path="#") -> List[str]:
        refs = []
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "$ref" and isinstance(value, str) and value.startswith("#"):
                    ref_path = value
                    refs.append(ref_path)
                else:
                    refs.extend(extract_refs(value, base_path))
        elif isinstance(data, list):
            for item in data:
                refs.extend(extract_refs(item, base_path))
        return refs

    def resolve_pointer(data: Mapping[str, Any], pointer: str) -> bool:
        parts = pointer.split("/")[1:]  # Skip the first empty part due to leading '#/'
        current = data
        try:
            for part in parts:
                part = part.replace("~1", "/").replace("~0", "~")  # Unescape JSON Pointer
                current = current[part]
            return True
        except (KeyError, TypeError):
            return False

    def validate_refs(yaml_file: str) -> List[str]:
        data = load_yaml(yaml_file)
        refs = extract_refs(data)
        invalid_refs = [ref for ref in refs if not resolve_pointer(data, ref.replace("#", ""))]
        return invalid_refs

    yaml_file_path = (
        Path(__file__).resolve().parent.parent.parent.parent.parent
        / "airbyte_cdk/sources/declarative/declarative_component_schema.yaml"
    )
    assert not validate_refs(yaml_file_path)


@pytest.mark.parametrize(
    "test_name, manifest, pages, expected_states_qty",
    [
        (
            "test_with_pagination_and_partition_router",
            {
                "version": "0.34.2",
                "type": "DeclarativeSource",
                "check": {"type": "CheckStream", "stream_names": ["Rates"]},
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "name": "Rates",
                        "primary_key": [],
                        "schema_loader": {
                            "type": "InlineSchemaLoader",
                            "schema": {
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ABC": {"type": "number"},
                                    "AED": {"type": "number"},
                                    "partition": {"type": "number"},
                                },
                                "type": "object",
                            },
                        },
                        "retriever": {
                            "type": "SimpleRetriever",
                            "requester": {
                                "type": "HttpRequester",
                                "url_base": "https://api.apilayer.com",
                                "path": "/exchangerates_data/latest",
                                "http_method": "GET",
                                "request_parameters": {},
                                "request_headers": {},
                                "request_body_json": {},
                                "authenticator": {
                                    "type": "ApiKeyAuthenticator",
                                    "header": "apikey",
                                    "api_token": "{{ config['api_key'] }}",
                                },
                            },
                            "partition_router": {
                                "type": "ListPartitionRouter",
                                "values": ["0", "1"],
                                "cursor_field": "partition",
                            },
                            "record_selector": {
                                "type": "RecordSelector",
                                "extractor": {"type": "DpathExtractor", "field_path": ["rates"]},
                            },
                            "paginator": {
                                "type": "DefaultPaginator",
                                "page_size": 2,
                                "page_size_option": {
                                    "inject_into": "request_parameter",
                                    "field_name": "page_size",
                                },
                                "page_token_option": {"inject_into": "path", "type": "RequestPath"},
                                "pagination_strategy": {
                                    "type": "CursorPagination",
                                    "cursor_value": "{{ response._metadata.next }}",
                                    "page_size": 2,
                                },
                            },
                        },
                        "incremental_sync": {
                            "type": "DatetimeBasedCursor",
                            "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%S.%fZ"],
                            "datetime_format": "%Y-%m-%dT%H:%M:%S.%fZ",
                            "cursor_field": "updated_at",
                            "start_datetime": {
                                "datetime": "{{ config.get('start_date', '2020-10-16T00:00:00.000Z') }}"
                            },
                        },
                    }
                ],
                "spec": {
                    "connection_specification": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "required": ["api_key"],
                        "properties": {
                            "api_key": {
                                "type": "string",
                                "title": "API Key",
                                "airbyte_secret": True,
                            },
                            "start_date": {
                                "title": "Start Date",
                                "description": "UTC date and time in the format YYYY-MM-DDTHH:MM:SS.000Z. During incremental sync, any data generated before this date will not be replicated. If left blank, the start date will be set to 2 years before the present date.",
                                "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                                "pattern_descriptor": "YYYY-MM-DDTHH:MM:SS.000Z",
                                "examples": ["2020-11-16T00:00:00.000Z"],
                                "type": "string",
                                "format": "date-time",
                            },
                        },
                        "additionalProperties": True,
                    },
                    "documentation_url": "https://example.org",
                    "type": "Spec",
                },
            },
            (
                _create_page(
                    {
                        "rates": [
                            {"ABC": 0, "partition": 0, "updated_at": "2020-11-16T00:00:00.000Z"},
                            {"AED": 1, "partition": 0, "updated_at": "2020-11-16T00:00:00.000Z"},
                        ],
                        "_metadata": {"next": "next"},
                    }
                ),
                _create_page(
                    {
                        "rates": [
                            {"USD": 3, "partition": 0, "updated_at": "2020-11-16T00:00:00.000Z"}
                        ],
                        "_metadata": {},
                    }
                ),
                _create_page(
                    {
                        "rates": [
                            {"ABC": 2, "partition": 1, "updated_at": "2020-11-16T00:00:00.000Z"}
                        ],
                        "_metadata": {},
                    }
                ),
            ),
            2,
        ),
    ],
)
def test_slice_checkpoint(test_name, manifest, pages, expected_states_qty):
    _stream_name = "Rates"
    with patch.object(SimpleRetriever, "_fetch_next_page", side_effect=pages):
        states = [message.state for message in _run_read(manifest, _stream_name) if message.state]
        assert len(states) == expected_states_qty


@pytest.fixture
def migration_mocks(monkeypatch):
    mock_message_repository = Mock()
    mock_message_repository.consume_queue.return_value = [Mock()]

    _mock_open = mock_open()
    mock_json_dump = Mock()
    mock_print = Mock()
    mock_serializer_dump = Mock()

    mock_decoded_bytes = Mock()
    mock_decoded_bytes.decode.return_value = "decoded_message"
    mock_orjson_dumps = Mock(return_value=mock_decoded_bytes)

    monkeypatch.setattr("builtins.open", _mock_open)
    monkeypatch.setattr("json.dump", mock_json_dump)
    monkeypatch.setattr("builtins.print", mock_print)
    monkeypatch.setattr(
        "airbyte_cdk.models.airbyte_protocol_serializers.AirbyteMessageSerializer.dump",
        mock_serializer_dump,
    )
    monkeypatch.setattr(
        "airbyte_cdk.legacy.sources.declarative.manifest_declarative_source.orjson.dumps",
        mock_orjson_dumps,
    )

    return {
        "message_repository": mock_message_repository,
        "open": _mock_open,
        "json_dump": mock_json_dump,
        "print": mock_print,
        "serializer_dump": mock_serializer_dump,
        "orjson_dumps": mock_orjson_dumps,
        "decoded_bytes": mock_decoded_bytes,
    }


def test_given_unmigrated_config_when_migrating_then_config_is_migrated(migration_mocks) -> None:
    input_config = {"planet": "CRSC"}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "config_migrations": [
                    {
                        "type": "ConfigMigration",
                        "description": "Test migration",
                        "transformations": [
                            {
                                "type": "ConfigRemapField",
                                "map": {"CRSC": "Coruscant"},
                                "field_path": ["planet"],
                            }
                        ],
                    }
                ],
            },
        },
    }

    ManifestDeclarativeSource(
        source_config=manifest,
        config=input_config,
        config_path="/fake/config/path",
        component_factory=ModelToComponentFactory(
            message_repository=migration_mocks["message_repository"],
        ),
    )

    migration_mocks["message_repository"].emit_message.assert_called_once()
    migration_mocks["open"].assert_called_once_with("/fake/config/path", "w")
    migration_mocks["json_dump"].assert_called_once()
    migration_mocks["print"].assert_called()
    migration_mocks["serializer_dump"].assert_called()
    migration_mocks["orjson_dumps"].assert_called()
    migration_mocks["decoded_bytes"].decode.assert_called()


def test_given_already_migrated_config_no_control_message_is_emitted(migration_mocks) -> None:
    input_config = {"planet": "Coruscant"}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "config_migrations": [
                    {
                        "type": "ConfigMigration",
                        "description": "Test migration",
                        "transformations": [
                            {
                                "type": "ConfigRemapField",
                                "map": {"CRSC": "Coruscant"},
                                "field_path": ["planet"],
                            }
                        ],
                    }
                ],
            },
        },
    }

    ManifestDeclarativeSource(
        source_config=manifest,
        config=input_config,
        config_path="/fake/config/path",
        component_factory=ModelToComponentFactory(
            message_repository=migration_mocks["message_repository"],
        ),
    )

    migration_mocks["message_repository"].emit_message.assert_not_called()
    migration_mocks["open"].assert_not_called()
    migration_mocks["json_dump"].assert_not_called()
    migration_mocks["print"].assert_not_called()
    migration_mocks["serializer_dump"].assert_not_called()
    migration_mocks["orjson_dumps"].assert_not_called()
    migration_mocks["decoded_bytes"].decode.assert_not_called()


def test_given_transformations_config_is_transformed():
    input_config = {"planet": "CRSC"}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "transformations": [
                    {
                        "type": "ConfigAddFields",
                        "fields": [
                            {
                                "type": "AddedFieldDefinition",
                                "path": ["population"],
                                "value": "{{ config['planet'] }}",
                            }
                        ],
                    },
                    {
                        "type": "ConfigRemapField",
                        "map": {"CRSC": "Coruscant"},
                        "field_path": ["planet"],
                    },
                    {
                        "type": "ConfigRemapField",
                        "map": {"CRSC": 3_000_000_000_000},
                        "field_path": ["population"],
                    },
                ],
            },
        },
    }

    source = ManifestDeclarativeSource(
        source_config=manifest,
        config=input_config,
    )

    source.write_config = Mock(return_value=None)

    config = source.configure(input_config, "/fake/temp/dir")

    assert config != input_config
    assert config == {"planet": "Coruscant", "population": 3_000_000_000_000}


def test_given_valid_config_streams_validates_config_and_does_not_raise():
    input_config = {"schema_to_validate": {"planet": "Coruscant"}}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "parameters": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "validations": [
                    {
                        "type": "DpathValidator",
                        "field_path": ["schema_to_validate"],
                        "validation_strategy": {
                            "type": "ValidateAdheresToSchema",
                            "base_schema": {
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "title": "Test Spec",
                                "type": "object",
                                "properties": {"planet": {"type": "string"}},
                                "required": ["planet"],
                                "additionalProperties": False,
                            },
                        },
                    }
                ],
            },
        },
    }

    source = ManifestDeclarativeSource(
        source_config=manifest,
    )

    source.streams(input_config)


def test_given_invalid_config_streams_validates_config_and_raises():
    input_config = {"schema_to_validate": {"will_fail": "Coruscant"}}

    manifest = {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["Test"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "Test",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"type": "object"},
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.org",
                        "path": "/test",
                        "authenticator": {"type": "NoAuth"},
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": "https://example.org",
            "connection_specification": {},
            "parameters": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "validations": [
                    {
                        "type": "DpathValidator",
                        "field_path": ["schema_to_validate"],
                        "validation_strategy": {
                            "type": "ValidateAdheresToSchema",
                            "base_schema": {
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "title": "Test Spec",
                                "type": "object",
                                "properties": {"planet": {"type": "string"}},
                                "required": ["planet"],
                                "additionalProperties": False,
                            },
                        },
                    }
                ],
            },
        },
    }
    source = ManifestDeclarativeSource(
        source_config=manifest,
    )

    with pytest.raises(ValueError):
        source.streams(input_config)
