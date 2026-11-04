#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from pathlib import Path

from airbyte_cdk.models import AirbyteAnalyticsTraceMessage, SyncMode
from airbyte_cdk.sources.file_based.config.csv_format import CsvFormat
from airbyte_cdk.sources.file_based.exceptions import ConfigValidationError, FileBasedSourceError
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.sources.file_based.helpers import (
    EmptySchemaParser,
    LowInferenceLimitDiscoveryPolicy,
)
from unit_tests.sources.file_based.in_memory_files_source import InMemoryFilesSource
from unit_tests.sources.file_based.scenarios.file_based_source_builder import FileBasedSourceBuilder
from unit_tests.sources.file_based.scenarios.scenario_builder import (
    TestScenario,
    TestScenarioBuilder,
)

single_csv_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("single_csv_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11", "val12"),
                        ("val21", "val22"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_spec(json.loads((Path(__file__).parent / "file_based_spec.json").read_text()))
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11",
                    "col2": "val12",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21",
                    "col2": "val22",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_analytics_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_analytics")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["a.csv"],
                    "validation_policy": "Emit Record",
                },
                {
                    "name": "stream2",
                    "format": {"filetype": "csv"},
                    "globs": ["b.csv"],
                    "validation_policy": "Emit Record",
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col1", "col2", "col3"),
                        ("val11b", "val12b", "val13b"),
                        ("val21b", "val22b", "val23b"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                },
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                },
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11a",
                    "col2": "val12a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21a",
                    "col2": "val22a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val11b",
                    "col2": "val12b",
                    "col3": "val13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
            {
                "data": {
                    "col1": "val21b",
                    "col2": "val22b",
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
        ]
    )
    .set_expected_analytics(
        [
            AirbyteAnalyticsTraceMessage(type="file-cdk-csv-stream-count", value="2"),
        ]
    )
).build()

multi_csv_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("multi_csv_stream")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col1", "col2", "col3"),
                        ("val11b", "val12b", "val13b"),
                        ("val21b", "val22b", "val23b"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11a",
                    "col2": "val12a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21a",
                    "col2": "val22a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val11b",
                    "col2": "val12b",
                    "col3": "val13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21b",
                    "col2": "val22b",
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

multi_csv_stream_n_file_exceeds_limit_for_inference = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("multi_csv_stream_n_file_exceeds_limit_for_inference")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col1", "col2", "col3"),
                        ("val11b", "val12b", "val13b"),
                        ("val21b", "val22b", "val23b"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
        .set_discovery_policy(LowInferenceLimitDiscoveryPolicy())
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11a",
                    "col2": "val12a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21a",
                    "col2": "val22a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val11b",
                    "col2": "val12b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21b",
                    "col2": "val22b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

multi_csv_stream_n_file_exceeds_config_limit_for_inference = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("multi_csv_stream_n_file_exceeds_config_limit_for_inference")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "recent_n_files_to_read_for_schema_discovery": 3,
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col1", "col2", "col3"),
                        ("val11b", "val12b", "val13b"),
                        ("val21b", "val22b", "val23b"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "c.csv": {
                    "contents": [
                        ("col1", "col2", "col3", "col4"),
                        ("val11c", "val12c", "val13c", "val14c"),
                        ("val21c", "val22c", "val23c", "val24c"),
                    ],
                    "last_modified": "2023-06-06T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "col3": {"type": ["null", "string"]},
                            "col4": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11a",
                    "col2": "val12a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21a",
                    "col2": "val22a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val11b",
                    "col2": "val12b",
                    "col3": "val13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21b",
                    "col2": "val22b",
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val11c",
                    "col2": "val12c",
                    "col3": "val13c",
                    "col4": "val14c",
                    "_ab_source_file_last_modified": "2023-06-06T03:54:07.000000Z",
                    "_ab_source_file_url": "c.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21c",
                    "col2": "val22c",
                    "col3": "val23c",
                    "col4": "val24c",
                    "_ab_source_file_last_modified": "2023-06-06T03:54:07.000000Z",
                    "_ab_source_file_url": "c.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

invalid_csv_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("invalid_csv_scenario")  # too many values for the number of headers
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1",),
                        ("val11", "val12"),
                        ("val21", "val22"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                }
            ]
        }
    )
    .set_expected_records([])
    .set_expected_discover_error(
        AirbyteTracedException, FileBasedSourceError.SCHEMA_INFERENCE_ERROR.value
    )
    .set_expected_logs(
        {
            "read": [
                {
                    "level": "ERROR",
                    "message": f"{FileBasedSourceError.INVALID_SCHEMA_ERROR.value} stream=stream1 file=a.csv line_no=1 n_skipped=0",
                },
            ]
        }
    )
    .set_expected_read_error(
        AirbyteTracedException,
        "Please check the logged errors for more information.",
    )
).build()

invalid_csv_multi_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("invalid_csv_multi_scenario")  # too many values for the number of headers
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                },
                {
                    "name": "stream2",
                    "format": {"filetype": "csv"},
                    "globs": ["b.csv"],
                    "validation_policy": "Emit Record",
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1",),
                        ("val11", "val12"),
                        ("val21", "val22"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col3",),
                        ("val13b", "val14b"),
                        ("val23b", "val24b"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                },
            ]
        }
    )
    .set_expected_records([])
    .set_expected_discover_error(
        AirbyteTracedException, FileBasedSourceError.SCHEMA_INFERENCE_ERROR.value
    )
    .set_expected_logs(
        {
            "read": [
                {
                    "level": "ERROR",
                    "message": f"{FileBasedSourceError.ERROR_PARSING_RECORD.value} stream=stream1 file=a.csv line_no=1 n_skipped=0",
                },
                {
                    "level": "ERROR",
                    "message": f"{FileBasedSourceError.ERROR_PARSING_RECORD.value} stream=stream2 file=b.csv line_no=1 n_skipped=0",
                },
            ]
        }
    )
    .set_expected_read_error(
        AirbyteTracedException, "Please check the logged errors for more information."
    )
).build()

csv_single_stream_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_single_stream_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*.csv"],
                    "validation_policy": "Emit Record",
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.jsonl": {
                    "contents": [
                        {"col1": "val11b", "col2": "val12b", "col3": "val13b"},
                        {"col1": "val12b", "col2": "val22b", "col3": "val23b"},
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11a",
                    "col2": "val12a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21a",
                    "col2": "val22a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_multi_stream_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_multi_stream")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*.csv"],
                    "validation_policy": "Emit Record",
                },
                {
                    "name": "stream2",
                    "format": {"filetype": "csv"},
                    "globs": ["b.csv"],
                    "validation_policy": "Emit Record",
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col3",),
                        ("val13b",),
                        ("val23b",),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                },
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11a",
                    "col2": "val12a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21a",
                    "col2": "val22a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col3": "val13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col3": "val13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
            {
                "data": {
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
        ]
    )
).build()

csv_custom_format_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_custom_format")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "delimiter": "#",
                        "quote_char": "|",
                        "escape_char": "!",
                        "double_quote": True,
                    },
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2", "col3"),
                        ("val11", "val12", "val |13|"),
                        ("val21", "val22", "val23"),
                        ("val,31", "val |,32|", "val, !!!! 33"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
        .set_file_write_options(
            {
                "delimiter": "#",
                "quotechar": "|",
            }
        )
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": ["null", "string"],
                            },
                            "col2": {
                                "type": ["null", "string"],
                            },
                            "col3": {
                                "type": ["null", "string"],
                            },
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11",
                    "col2": "val12",
                    "col3": "val |13|",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val21",
                    "col2": "val22",
                    "col3": "val23",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val,31",
                    "col2": "val |,32|",
                    "col3": "val, !! 33",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

multi_stream_custom_format = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("multi_stream_custom_format_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*.csv"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "delimiter": "#",
                        "escape_char": "!",
                        "double_quote": True,
                        "newlines_in_values": False,
                    },
                },
                {
                    "name": "stream2",
                    "globs": ["b.csv"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "delimiter": "#",
                        "escape_char": "@",
                        "double_quote": True,
                        "newlines_in_values": False,
                    },
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val !! 12a"),
                        ("val !! 21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col3",),
                        ("val @@@@ 13b",),
                        ("val23b",),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
        .set_file_write_options(
            {
                "delimiter": "#",
            }
        )
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": ["null", "string"],
                            },
                            "col2": {
                                "type": ["null", "string"],
                            },
                            "col3": {
                                "type": ["null", "string"],
                            },
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {
                                "type": ["null", "string"],
                            },
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                },
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11a",
                    "col2": "val ! 12a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col1": "val ! 21a",
                    "col2": "val22a",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col3": "val @@@@ 13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col3": "val @@ 13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
            {
                "data": {
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
        ]
    )
).build()

empty_schema_inference_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("empty_schema_inference_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11", "val12"),
                        ("val21", "val22"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
        .set_parsers({CsvFormat: EmptySchemaParser()})
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_discover_error(
        AirbyteTracedException, FileBasedSourceError.SCHEMA_INFERENCE_ERROR.value
    )
).build()

schemaless_csv_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("schemaless_csv_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Skip Record",
                    "schemaless": True,
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col1", "col2", "col3"),
                        ("val11b", "val12b", "val13b"),
                        ("val21b", "val22b", "val23b"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "object"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "data": {"col1": "val11a", "col2": "val12a"},
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "data": {"col1": "val21a", "col2": "val22a"},
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "data": {"col1": "val11b", "col2": "val12b", "col3": "val13b"},
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "data": {"col1": "val21b", "col2": "val22b", "col3": "val23b"},
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

schemaless_csv_multi_stream_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("schemaless_csv_multi_stream_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["a.csv"],
                    "validation_policy": "Skip Record",
                    "schemaless": True,
                },
                {
                    "name": "stream2",
                    "format": {"filetype": "csv"},
                    "globs": ["b.csv"],
                    "validation_policy": "Skip Record",
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col3",),
                        ("val13b",),
                        ("val23b",),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "object"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                },
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "data": {"col1": "val11a", "col2": "val12a"},
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "data": {"col1": "val21a", "col2": "val22a"},
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
            {
                "data": {
                    "col3": "val13b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
            {
                "data": {
                    "col3": "val23b",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "b.csv",
                },
                "stream": "stream2",
            },
        ]
    )
).build()

schemaless_with_user_input_schema_fails_connection_check_scenario: TestScenario[
    InMemoryFilesSource
] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("schemaless_with_user_input_schema_fails_connection_check_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Skip Record",
                    "input_schema": '{"col1": "string", "col2": "string", "col3": "string"}',
                    "schemaless": True,
                }
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col1", "col2", "col3"),
                        ("val11b", "val12b", "val13b"),
                        ("val21b", "val22b", "val23b"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_catalog(CatalogBuilder().with_stream("stream1", SyncMode.full_refresh).build())
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "object"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                }
            ]
        }
    )
    .set_expected_check_status("FAILED")
    .set_expected_check_error(
        None, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_discover_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_read_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
).build()

schemaless_with_user_input_schema_fails_connection_check_multi_stream_scenario: TestScenario[
    InMemoryFilesSource
] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("schemaless_with_user_input_schema_fails_connection_check_multi_stream_scenario")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["a.csv"],
                    "validation_policy": "Skip Record",
                    "schemaless": True,
                    "input_schema": '{"col1": "string", "col2": "string", "col3": "string"}',
                },
                {
                    "name": "stream2",
                    "format": {"filetype": "csv"},
                    "globs": ["b.csv"],
                    "validation_policy": "Skip Record",
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col3",),
                        ("val13b",),
                        ("val23b",),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_catalog(
        CatalogBuilder()
        .with_stream("stream1", SyncMode.full_refresh)
        .with_stream("stream2", SyncMode.full_refresh)
        .build()
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "object"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                },
            ]
        }
    )
    .set_expected_check_status("FAILED")
    .set_expected_check_error(
        None, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_discover_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_read_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
).build()

recent_n_files_to_read_for_schema_discovery_with_use_first_found_file_for_schema_discovery_fails_connection_check_multi_stream_scenario: TestScenario[
    InMemoryFilesSource
] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name(
        "recent_n_files_to_read_for_schema_discovery_with_use_first_found_file_for_schema_discovery_fails_connection_check_multi_stream_scenario"
    )
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["a.csv"],
                    "validation_policy": "Skip Record",
                    "recent_n_files_to_read_for_schema_discovery": 5,
                    "use_first_found_file_for_schema_discovery": True,
                },
                {
                    "name": "stream2",
                    "format": {"filetype": "csv"},
                    "globs": ["b.csv"],
                    "validation_policy": "Skip Record",
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col3",),
                        ("val13b",),
                        ("val23b",),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_catalog(
        CatalogBuilder()
        .with_stream("stream1", SyncMode.full_refresh)
        .with_stream("stream2", SyncMode.full_refresh)
        .build()
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "object"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                },
            ]
        }
    )
    .set_expected_check_status("FAILED")
    .set_expected_check_error(
        None, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_discover_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_read_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
).build()


schemaless_with_use_first_found_file_for_schema_discovery_fails_connection_check_multi_stream_scenario: TestScenario[
    InMemoryFilesSource
] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name(
        "schemaless_with_use_first_found_file_for_schema_discovery_fails_connection_check_multi_stream_scenario"
    )
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["a.csv"],
                    "validation_policy": "Skip Record",
                    "schemaless": True,
                    "use_first_found_file_for_schema_discovery": True,
                },
                {
                    "name": "stream2",
                    "format": {"filetype": "csv"},
                    "globs": ["b.csv"],
                    "validation_policy": "Skip Record",
                },
            ],
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11a", "val12a"),
                        ("val21a", "val22a"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
                "b.csv": {
                    "contents": [
                        ("col3",),
                        ("val13b",),
                        ("val23b",),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                },
            }
        )
        .set_file_type("csv")
    )
    .set_catalog(
        CatalogBuilder()
        .with_stream("stream1", SyncMode.full_refresh)
        .with_stream("stream2", SyncMode.full_refresh)
        .build()
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "object"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                },
            ]
        }
    )
    .set_expected_check_status("FAILED")
    .set_expected_check_error(
        None, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_discover_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
    .set_expected_read_error(
        ConfigValidationError, FileBasedSourceError.ERROR_VALIDATION_STREAM_DISCOVERY_OPTIONS.value
    )
).build()

csv_string_can_be_null_with_input_schemas_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_string_can_be_null_with_input_schema")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "input_schema": '{"col1": "string", "col2": "string"}',
                    "format": {
                        "filetype": "csv",
                        "null_values": ["null"],
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("2", "null"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "string"},
                            "col2": {"type": "string"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "2",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_string_are_not_null_if_strings_can_be_null_is_false_scenario: TestScenario[
    InMemoryFilesSource
] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_string_are_not_null_if_strings_can_be_null_is_false")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "input_schema": '{"col1": "string", "col2": "string"}',
                    "format": {
                        "filetype": "csv",
                        "null_values": ["null"],
                        "strings_can_be_null": False,
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("2", "null"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "string"},
                            "col2": {"type": "string"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "2",
                    "col2": "null",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_string_not_null_if_no_null_values_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_string_not_null_if_no_null_values")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("2", "null"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "2",
                    "col2": "null",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_strings_can_be_null_not_quoted_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_strings_can_be_null_no_input_schema")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {"filetype": "csv", "null_values": ["null"]},
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("2", "null"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "2",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_newline_in_values_quoted_value_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_newline_in_values_quoted_value")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        '''"col1","col2"''',
                        '''"2","val\n2"''',
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "2",
                    "col2": "val\n2",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_newline_in_values_not_quoted_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_newline_in_values_not_quoted")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        """col1,col2""",
                        """2,val\n2""",
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                    "is_file_based": False,
                }
            ]
        }
    )
    .set_expected_records(
        [
            # Note that the value for col2 is truncated to "val" because the newline is not escaped
            {
                "data": {
                    "col1": "2",
                    "col2": "val",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
    .set_expected_read_error(
        AirbyteTracedException,
        f"{FileBasedSourceError.ERROR_PARSING_RECORD.value} stream=stream1 file=a.csv line_no=2 n_skipped=0",
    )
    .set_expected_discover_error(
        AirbyteTracedException, FileBasedSourceError.SCHEMA_INFERENCE_ERROR.value
    )
    .set_expected_read_error(
        AirbyteTracedException,
        "Please check the logged errors for more information.",
    )
).build()

csv_escape_char_is_set_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_escape_char_is_set")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "double_quotes": False,
                        "quote_char": '"',
                        "delimiter": ",",
                        "escape_char": "\\",
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        """col1,col2""",
                        '''val11,"val\\"2"''',
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11",
                    "col2": 'val"2',
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_double_quote_is_set_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_doublequote_is_set")
    # This scenario tests that quotes are properly escaped when double_quotes is True
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "double_quotes": True,
                        "quote_char": '"',
                        "delimiter": ",",
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        """col1,col2""",
                        '''val11,"val""2"''',
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11",
                    "col2": 'val"2',
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_custom_delimiter_with_escape_char_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_custom_delimiter_with_escape_char")
    # This scenario tests that a value can contain the delimiter if it is wrapped in the quote_char
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "double_quotes": True,
                        "quote_char": "@",
                        "delimiter": "|",
                        "escape_char": "+",
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        """col1|col2""",
                        """val"1,1|val+|2""",
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": 'val"1,1',
                    "col2": "val|2",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_custom_delimiter_in_double_quotes_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_custom_delimiter_in_double_quotes")
    # This scenario tests that a value can contain the delimiter if it is wrapped in the quote_char
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "double_quotes": True,
                        "quote_char": "@",
                        "delimiter": "|",
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        """col1|col2""",
                        """val"1,1|@val|2@""",
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": 'val"1,1',
                    "col2": "val|2",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_skip_before_header_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_skip_before_header")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {"filetype": "csv", "skip_rows_before_header": 2},
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("skip_this", "skip_this"),
                        ("skip_this_too", "skip_this_too"),
                        ("col1", "col2"),
                        ("val11", "val12"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11",
                    "col2": "val12",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_skip_after_header_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_skip_after_header")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {"filetype": "csv", "skip_rows_after_header": 2},
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("skip_this", "skip_this"),
                        ("skip_this_too", "skip_this_too"),
                        ("val11", "val12"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11",
                    "col2": "val12",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_skip_before_and_after_header_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_skip_before_after_header")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "skip_rows_before_header": 1,
                        "skip_rows_after_header": 1,
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("skip_this", "skip_this"),
                        ("col1", "col2"),
                        ("skip_this_too", "skip_this_too"),
                        ("val11", "val12"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": ["null", "string"]},
                            "col2": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": "val11",
                    "col2": "val12",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_autogenerate_column_names_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_autogenerate_column_names")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "format": {
                        "filetype": "csv",
                        "header_definition": {"header_definition_type": "Autogenerated"},
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("val11", "val12"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "f0": {"type": ["null", "string"]},
                            "f1": {"type": ["null", "string"]},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "f0": "val11",
                    "f1": "val12",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_custom_bool_values_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_custom_bool_values")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "input_schema": '{"col1": "boolean", "col2": "boolean"}',
                    "format": {
                        "filetype": "csv",
                        "true_values": ["this_is_true"],
                        "false_values": ["this_is_false"],
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("this_is_true", "this_is_false"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "boolean"},
                            "col2": {"type": "boolean"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col1": True,
                    "col2": False,
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

csv_custom_null_values_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_custom_null_values")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "input_schema": '{"col1": "boolean", "col2": "string"}',
                    "format": {
                        "filetype": "csv",
                        "null_values": ["null"],
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("null", "na"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "boolean"},
                            "col2": {"type": "string"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(
        [
            {
                "data": {
                    "col2": "na",
                    "_ab_source_file_last_modified": "2023-06-05T03:54:07.000000Z",
                    "_ab_source_file_url": "a.csv",
                },
                "stream": "stream1",
            },
        ]
    )
).build()

earlier_csv_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("earlier_csv_stream")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                }
            ],
            "start_date": "2023-06-10T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [
                        ("col1", "col2"),
                        ("val11", "val12"),
                        ("val21", "val22"),
                    ],
                    "last_modified": "2023-06-05T03:54:07.000000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_check_status("FAILED")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                            "data": {"type": "object"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(None)
).build()

csv_no_records_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("csv_empty_no_records")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                    "input_schema": '{"col1": "boolean", "col2": "string"}',
                    "format": {
                        "filetype": "csv",
                        "null_values": ["null"],
                    },
                }
            ],
            "start_date": "2023-06-04T03:54:07.000000Z",
        }
    )
    .set_source_builder(
        FileBasedSourceBuilder()
        .set_files(
            {
                "a.csv": {
                    "contents": [("col1", "col2")],  # column headers, but no data rows
                    "last_modified": "2023-06-05T03:54:07.000Z",
                }
            }
        )
        .set_file_type("csv")
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "boolean"},
                            "col2": {"type": "string"},
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records([])
).build()

csv_no_files_scenario: TestScenario[InMemoryFilesSource] = (
    TestScenarioBuilder[InMemoryFilesSource]()
    .set_name("no_files_csv_stream")
    .set_config(
        {
            "delivery_method": {"delivery_type": "use_records_transfer"},
            "streams": [
                {
                    "name": "stream1",
                    "format": {"filetype": "csv"},
                    "globs": ["*"],
                    "validation_policy": "Emit Record",
                }
            ],
            "start_date": "2023-06-10T03:54:07.000000Z",
        }
    )
    .set_source_builder(FileBasedSourceBuilder().set_files({}).set_file_type("csv"))
    .set_expected_check_status("FAILED")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "_ab_source_file_last_modified": {"type": "string"},
                            "_ab_source_file_url": {"type": "string"},
                            "data": {"type": "object"},
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "is_resumable": True,
                }
            ]
        }
    )
    .set_expected_records(None)
).build()
