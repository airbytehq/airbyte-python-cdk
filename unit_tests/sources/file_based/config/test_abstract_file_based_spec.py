#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Type

import pytest
from jsonschema import ValidationError, validate
from pydantic.v1 import BaseModel

from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from airbyte_cdk.sources.file_based.config.file_based_stream_config import (
    AvroFormat,
    CsvFormat,
    ParquetFormat,
)


@pytest.mark.parametrize(
    "file_format, file_type, expected_error",
    [
        pytest.param(
            ParquetFormat, "parquet", None, id="test_parquet_format_is_a_valid_parquet_file_type"
        ),
        pytest.param(AvroFormat, "avro", None, id="test_avro_format_is_a_valid_avro_file_type"),
        pytest.param(
            CsvFormat,
            "parquet",
            ValidationError,
            id="test_csv_format_is_not_a_valid_parquet_file_type",
        ),
    ],
)
def test_parquet_file_type_is_not_a_valid_csv_file_type(
    file_format: BaseModel, file_type: str, expected_error: Type[Exception]
) -> None:
    format_config = {file_type: {"filetype": file_type, "decimal_as_float": True}}

    if expected_error:
        with pytest.raises(expected_error):
            validate(instance=format_config[file_type], schema=file_format.schema())
    else:
        validate(instance=format_config[file_type], schema=file_format.schema())


@pytest.mark.parametrize(
    "start_date, should_pass",
    [
        pytest.param("2021-01-01T00:00:00.000000Z", True, id="with_microseconds"),
        pytest.param("2021-01-01T00:00:00Z", True, id="without_microseconds"),
        pytest.param("2021-01-01T00:00:00.000Z", True, id="with_milliseconds"),
        pytest.param("2025-01-01T00:00:00Z", True, id="terraform_provider_format"),
        pytest.param("2021-01-01T00:00:00+00:00", True, id="with_timezone_offset"),
        pytest.param("2021-01-01", True, id="date_only"),
        pytest.param(None, True, id="none_value"),
        pytest.param("not-a-date", False, id="invalid_string"),
        pytest.param("", False, id="empty_string"),
    ],
)
def test_start_date_validation(start_date: str, should_pass: bool) -> None:
    """Test that start_date accepts various valid ISO8601/RFC3339 formats."""
    if should_pass:
        result = AbstractFileBasedSpec.validate_start_date(start_date)
        assert result == start_date
    else:
        with pytest.raises(ValueError, match="is not a valid datetime string"):
            AbstractFileBasedSpec.validate_start_date(start_date)


def test_spec_requires_defaulted_visible_file_fields() -> None:
    schema = AbstractFileBasedSpec.schema()
    assert "delivery_method" in schema["required"]

    stream_schema = schema["properties"]["streams"]["items"]
    assert {
        "globs",
        "validation_policy",
        "days_to_sync_if_history_is_full",
        "schemaless",
        "use_first_found_file_for_schema_discovery",
    }.issubset(stream_schema["required"])

    csv_schema = next(
        option
        for option in stream_schema["properties"]["format"]["oneOf"]
        if option["properties"]["filetype"]["default"] == "csv"
    )
    assert {
        "delimiter",
        "quote_char",
        "encoding",
        "double_quote",
        "null_values",
        "strings_can_be_null",
        "skip_rows_before_header",
        "skip_rows_after_header",
        "header_definition",
        "true_values",
        "false_values",
    }.issubset(csv_schema["required"])
