#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import copy
import json

import freezegun

from airbyte_cdk.connector_builder.connector_builder_handler import (
    TestLimits,
)
from airbyte_cdk.connector_builder.main import (
    handle_connector_builder_request,
)
from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStreamState,
    ConfiguredAirbyteCatalogSerializer,
    Level,
    StreamDescriptor,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import find_template

BASE_URL = "https://api.apilayer.com/exchangerates_data/"
FREEZE_DATE = "2025-05-23"

PROPERTY_KEY = "test"
PROPERTY_LIST = ["one", "two", "three", "four"]

MANIFEST = {
    "version": "6.48.15",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "Rates",
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "path": "/exchangerates_data/{{stream_interval.start_time}}",
                    "url_base": "https://api.apilayer.com",
                    "http_method": "GET",
                    "authenticator": {
                        "type": "ApiKeyAuthenticator",
                        "api_token": "{{ config['api_key'] }}",
                        "inject_into": {
                            "type": "RequestOption",
                            "field_name": "apikey",
                            "inject_into": "header",
                        },
                    },
                    "request_parameters": {
                        "base": "{{ config['base'] }}",
                        PROPERTY_KEY: {
                            "type": "QueryProperties",
                            "property_list": PROPERTY_LIST,
                            "property_chunking": {
                                "type": "PropertyChunking",
                                "property_limit_type": "property_count",
                                "property_limit": 2,
                            },
                        },
                    },
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
            },
            "primary_key": [],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "type": "object",
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "base": {"type": "string"},
                        "date": {"type": "string"},
                        "rates": {
                            "type": "object",
                            "properties": {"fake_currency": {"type": "number"}},
                        },
                        "success": {"type": "boolean"},
                        "timestamp": {"type": "number"},
                        "historical": {"type": "boolean"},
                    },
                },
            },
            "transformations": [],
            "incremental_sync": {
                "type": "DatetimeBasedCursor",
                "step": "P1D",
                "cursor_field": "date",
                "end_datetime": {
                    "type": "MinMaxDatetime",
                    "datetime": "{{ now_utc().strftime('%Y-%m-%dT%H:%M:%SZ') }}",
                    "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                },
                "start_datetime": {
                    "type": "MinMaxDatetime",
                    "datetime": "{{ config['start_date'] }}",
                    "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                },
                "datetime_format": "%Y-%m-%d",
                "cursor_granularity": "P1D",
                "cursor_datetime_formats": ["%Y-%m-%d"],
            },
            "state_migrations": [],
        }
    ],
    "spec": {
        "type": "Spec",
        "documentation_url": "https://example.org",
        "connection_specification": {
            "type": "object",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "required": ["start_date", "api_key", "base"],
            "properties": {
                "base": {"type": "string", "order": 2, "title": "Base"},
                "api_key": {
                    "type": "string",
                    "order": 1,
                    "title": "API Key",
                    "airbyte_secret": True,
                },
                "start_date": {
                    "type": "string",
                    "order": 0,
                    "title": "Start date",
                    "format": "date-time",
                    "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                },
            },
            "additionalProperties": True,
        },
    },
    "metadata": {
        "testedStreams": {
            "Rates": {
                "hasRecords": False,
                "streamHash": "4dce031d602258dd3bcc478731d6862a5cdeb70f",
                "hasResponse": False,
                "primaryKeysAreUnique": False,
                "primaryKeysArePresent": False,
                "responsesAreSuccessful": False,
            }
        },
        "autoImportSchema": {"Rates": True},
    },
    "dynamic_streams": [],
}

_stream_name = "Rates"

_A_STATE = [
    AirbyteStateMessage(
        type="STREAM",
        stream=AirbyteStreamState(
            stream_descriptor=StreamDescriptor(name=_stream_name),
            stream_state=AirbyteStateBlob({"key": "value"}),
        ),
    )
]

TEST_READ_CONFIG = {
    "__injected_declarative_manifest": MANIFEST,
    "__command": "test_read",
    "__test_read_config": {"max_pages_per_slice": 2, "max_slices": 5, "max_records": 10},
}

CONFIGURED_CATALOG = {
    "streams": [
        {
            "stream": {
                "name": _stream_name,
                "json_schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {},
                },
                "supported_sync_modes": ["full_refresh"],
                "source_defined_cursor": False,
            },
            "sync_mode": "full_refresh",
            "destination_sync_mode": "overwrite",
        }
    ]
}


@freezegun.freeze_time(f"{FREEZE_DATE}T00:00:00Z")
def test_read():
    conversion_base = "USD"
    config = copy.deepcopy(TEST_READ_CONFIG)
    config["start_date"] = f"{FREEZE_DATE}T00:00:00Z"
    config["base"] = conversion_base
    config["api_key"] = "test_api_key"

    stream_url = f"{BASE_URL}{FREEZE_DATE}?base={conversion_base}&{PROPERTY_KEY}="

    with HttpMocker() as http_mocker:
        source = ManifestDeclarativeSource(
            source_config=MANIFEST, emit_connector_builder_messages=True
        )
        limits = TestLimits()

        http_mocker.get(
            HttpRequest(url=f"{stream_url}{PROPERTY_LIST[0]}%2C{PROPERTY_LIST[1]}"),
            HttpResponse(
                json.dumps(find_template("declarative/property_chunking/rates_one_two", __file__)),
                200,
            ),
        )
        http_mocker.get(
            HttpRequest(url=f"{stream_url}{PROPERTY_LIST[2]}%2C{PROPERTY_LIST[3]}"),
            HttpResponse(
                json.dumps(
                    find_template("declarative/property_chunking/rates_three_four", __file__)
                ),
                200,
            ),
        )
        output_record = handle_connector_builder_request(
            source,
            "test_read",
            config,
            ConfiguredAirbyteCatalogSerializer.load(CONFIGURED_CATALOG),
            _A_STATE,
            limits,
        )
        # for connector build we get each record in a single page
        assert len(output_record.record.data["slices"][0]["pages"]) == 2
        for current_log in output_record.record.data["logs"]:
            assert not "Something went wrong in the connector" in current_log["message"]
            assert not current_log["internal_message"]
            assert not current_log["level"] == Level.ERROR
            assert not current_log["stacktrace"]
