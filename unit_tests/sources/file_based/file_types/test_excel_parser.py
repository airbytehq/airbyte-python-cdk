#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import asyncio
import datetime
import warnings
from io import BytesIO
from unittest.mock import MagicMock, Mock, mock_open, patch

import pandas as pd
import pytest

from airbyte_cdk.sources.file_based.config.excel_format import SheetsToSync
from airbyte_cdk.sources.file_based.config.file_based_stream_config import (
    ExcelFormat,
    FileBasedStreamConfig,
    ValidationPolicy,
)
from airbyte_cdk.sources.file_based.exceptions import ConfigValidationError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.file_types.excel_parser import ExcelParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import SchemaType


@pytest.fixture
def mock_stream_reader():
    return Mock(spec=AbstractFileBasedStreamReader)


@pytest.fixture
def mock_logger():
    return Mock()


@pytest.fixture
def file_config():
    return FileBasedStreamConfig(
        name="test.xlsx",
        file_type="excel",
        format=ExcelFormat(),
        validation_policy=ValidationPolicy.emit_record,
    )


@pytest.fixture
def remote_file():
    return RemoteFile(uri="s3://mybucket/test.xlsx", last_modified=datetime.datetime.now())


@pytest.fixture
def setup_parser(remote_file):
    parser = ExcelParser()

    data = pd.DataFrame(
        {
            "column1": [1, 2, 3],
            "column2": ["a", "b", "c"],
            "column3": [True, False, True],
            "column4": pd.to_datetime(["2021-01-01", "2022-01-01", "2023-01-01"]),
        }
    )

    excel_bytes = BytesIO()
    with pd.ExcelWriter(excel_bytes, engine="xlsxwriter") as writer:
        data.to_excel(writer, index=False)
    excel_bytes.seek(0)

    stream_reader = MagicMock(spec=AbstractFileBasedStreamReader)
    stream_reader.open_file.return_value = BytesIO(excel_bytes.read())

    return (
        parser,
        FileBasedStreamConfig(name="test_stream", format=ExcelFormat()),
        remote_file,
        stream_reader,
        MagicMock(),
        data,
    )


@patch("pandas.ExcelFile")
def test_infer_schema(mock_excel_file, setup_parser):
    parser, config, file, stream_reader, logger, data = setup_parser

    mock_excel_file.return_value.parse.return_value = data

    loop = asyncio.get_event_loop()
    schema = loop.run_until_complete(parser.infer_schema(config, file, stream_reader, logger))

    expected_schema: SchemaType = {
        "column1": {"type": "number"},
        "column2": {"type": "string"},
        "column3": {"type": "boolean"},
        "column4": {"type": "string", "format": "date-time"},
    }

    assert schema == expected_schema

    stream_reader.open_file.assert_called_once_with(
        file, parser.file_read_mode, parser.ENCODING, logger
    )

    logger.info.assert_not_called()
    logger.error.assert_not_called()


def test_invalid_format(mock_stream_reader, mock_logger, remote_file):
    parser = ExcelParser()
    invalid_config = FileBasedStreamConfig(
        name="test.xlsx",
        file_type="csv",
        format={"filetype": "csv"},
        validation_policy=ValidationPolicy.emit_record,
    )

    with pytest.raises(ConfigValidationError):
        list(parser.parse_records(invalid_config, remote_file, mock_stream_reader, mock_logger))


def test_file_read_error(mock_stream_reader, mock_logger, file_config, remote_file):
    parser = ExcelParser()
    with patch("builtins.open", mock_open(read_data=b"corrupted data")):
        with patch("pandas.ExcelFile") as mock_excel:
            mock_excel.return_value.parse.side_effect = ValueError("Failed to parse file")

            with pytest.raises(RecordParseError):
                list(
                    parser.parse_records(file_config, remote_file, mock_stream_reader, mock_logger)
                )


class FakePanic(BaseException):
    """Simulates the PyO3 PanicException which does not inherit from Exception."""


def test_open_and_parse_file_falls_back_to_openpyxl(mock_logger):
    parser = ExcelParser()
    fp = BytesIO(b"test")
    remote_file = RemoteFile(uri="s3://mybucket/test.xlsx", last_modified=datetime.datetime.now())

    fallback_df = pd.DataFrame({"a": [1]})

    calamine_excel_file = MagicMock()

    def calamine_parse_side_effect():
        raise FakePanic(
            "failed to construct date: PyErr { type: <class 'ValueError'>, value: ValueError('year 20225 is out of range'), traceback: None }"
        )

    calamine_excel_file.parse.side_effect = calamine_parse_side_effect

    openpyxl_excel_file = MagicMock()

    def openpyxl_parse_side_effect():
        warnings.warn("Cell A146 has invalid date", UserWarning)
        return fallback_df

    openpyxl_excel_file.parse.side_effect = openpyxl_parse_side_effect

    with (
        patch("airbyte_cdk.sources.file_based.file_types.excel_parser.pd.ExcelFile") as mock_excel,
    ):
        mock_excel.side_effect = [calamine_excel_file, openpyxl_excel_file]

        result = parser.open_and_parse_file(fp, mock_logger, remote_file)

    pd.testing.assert_frame_equal(result, fallback_df)
    assert mock_logger.warning.call_count == 2
    assert "Openpyxl warning" in mock_logger.warning.call_args_list[1].args[0]


def test_open_and_parse_file_does_not_swallow_system_exit(mock_logger):
    """Test that SystemExit is not caught by the BaseException handler.

    This test ensures that critical system-level exceptions like SystemExit and KeyboardInterrupt
    are not accidentally caught and suppressed by our BaseException handler in the Calamine parsing
    method. These exceptions should always propagate up to allow proper program termination.
    """
    parser = ExcelParser()
    fp = BytesIO(b"test")
    remote_file = RemoteFile(uri="s3://mybucket/test.xlsx", last_modified=datetime.datetime.now())

    with patch("airbyte_cdk.sources.file_based.file_types.excel_parser.pd.ExcelFile") as mock_excel:
        mock_excel.return_value.parse.side_effect = SystemExit()

        with pytest.raises(SystemExit):
            parser.open_and_parse_file(fp, mock_logger, remote_file)


@pytest.mark.parametrize(
    "exc_cls",
    [
        pytest.param(OSError, id="os-error"),
    ],
)
def test_openpyxl_logs_info_when_seek_fails(mock_logger, remote_file, exc_cls):
    """Test that openpyxl logs info when seek fails on non-seekable files.

    This test ensures that when falling back to openpyxl, if the file pointer
    cannot be rewound (seek fails with OSError), an info-level log is emitted
    and parsing proceeds from the current position.
    """
    parser = ExcelParser()
    fallback_df = pd.DataFrame({"a": [1]})

    class FakeFP:
        """Fake file-like object with a seek method that raises an exception."""

        def __init__(self, exc):
            self._exc = exc

        def seek(self, *args, **kwargs):
            raise self._exc("not seekable")

    fp = FakeFP(exc_cls)

    openpyxl_excel_file = MagicMock()
    openpyxl_excel_file.parse.return_value = fallback_df

    with patch("airbyte_cdk.sources.file_based.file_types.excel_parser.pd.ExcelFile") as mock_excel:
        mock_excel.return_value = openpyxl_excel_file

        result = parser._open_and_parse_file_with_openpyxl(fp, mock_logger, remote_file)

    pd.testing.assert_frame_equal(result, fallback_df)
    mock_logger.info.assert_called_once()
    msg = mock_logger.info.call_args[0][0]
    assert "Could not rewind stream" in msg
    assert remote_file.file_uri_for_logging in msg
    mock_excel.assert_called_once_with(fp, engine="openpyxl")


def _make_multi_sheet_workbook(sheet_rows):
    excel_bytes = BytesIO()
    with pd.ExcelWriter(excel_bytes, engine="xlsxwriter") as writer:
        for sheet_name, rows in sheet_rows.items():
            pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
    excel_bytes.seek(0)
    return excel_bytes.read()


def _multi_sheet_setup(remote_file, excel_format, sheet_rows=None):
    parser = ExcelParser()
    workbook = _make_multi_sheet_workbook(
        sheet_rows
        or {
            "People": [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
            ],
            "Orders": [
                {"id": 3, "amount": 10.5},
                {"id": 4, "amount": 20.0},
                {"id": 5, "amount": 30.25},
            ],
        }
    )
    stream_reader = MagicMock(spec=AbstractFileBasedStreamReader)
    stream_reader.open_file.side_effect = lambda *args, **kwargs: BytesIO(workbook)
    config = FileBasedStreamConfig(name="test_stream", format=excel_format)
    return parser, config, remote_file, stream_reader, MagicMock()


def test_default_reads_only_first_sheet(remote_file):
    parser, config, file, stream_reader, logger = _multi_sheet_setup(remote_file, ExcelFormat())

    records = list(parser.parse_records(config, file, stream_reader, logger))

    assert len(records) == 2
    assert {record["name"] for record in records} == {"alice", "bob"}
    assert all(parser.SHEET_NAME_COLUMN not in record for record in records)


@pytest.mark.parametrize(
    "excel_format, expected_sheets, expected_count",
    [
        pytest.param(
            ExcelFormat(sheets_to_sync=SheetsToSync.ALL_SHEETS),
            {"People", "Orders"},
            5,
            id="all-sheets",
        ),
        pytest.param(
            ExcelFormat(sheet_names=["Orders"]),
            {"Orders"},
            3,
            id="explicit-sheet-names",
        ),
        pytest.param(
            ExcelFormat(sheets_to_sync=SheetsToSync.FIRST_SHEET_ONLY, sheet_names=["Orders"]),
            {"Orders"},
            3,
            id="explicit-sheet-names-take-precedence",
        ),
    ],
)
def test_multi_sheet_records_include_sheet_name(
    remote_file, excel_format, expected_sheets, expected_count
):
    parser, config, file, stream_reader, logger = _multi_sheet_setup(remote_file, excel_format)

    records = list(parser.parse_records(config, file, stream_reader, logger))

    assert len(records) == expected_count
    assert {record[parser.SHEET_NAME_COLUMN] for record in records} == expected_sheets


def test_all_sheets_schema_merges_columns(remote_file):
    parser, config, file, stream_reader, logger = _multi_sheet_setup(
        remote_file, ExcelFormat(sheets_to_sync=SheetsToSync.ALL_SHEETS)
    )

    loop = asyncio.get_event_loop()
    schema = loop.run_until_complete(parser.infer_schema(config, file, stream_reader, logger))

    assert schema == {
        "id": {"type": "number"},
        "name": {"type": "string"},
        "amount": {"type": "number"},
        parser.SHEET_NAME_COLUMN: {"type": "string"},
    }


def test_default_schema_only_first_sheet(remote_file):
    parser, config, file, stream_reader, logger = _multi_sheet_setup(remote_file, ExcelFormat())

    loop = asyncio.get_event_loop()
    schema = loop.run_until_complete(parser.infer_schema(config, file, stream_reader, logger))

    assert schema == {"id": {"type": "number"}, "name": {"type": "string"}}


def test_missing_explicit_sheet_name_raises_record_parse_error(remote_file):
    parser, config, file, stream_reader, logger = _multi_sheet_setup(
        remote_file, ExcelFormat(sheet_names=["Missing"])
    )

    with pytest.raises(RecordParseError):
        list(parser.parse_records(config, file, stream_reader, logger))


def test_reserved_sheet_name_column_raises_record_parse_error(remote_file):
    parser, config, file, stream_reader, logger = _multi_sheet_setup(
        remote_file,
        ExcelFormat(sheets_to_sync=SheetsToSync.ALL_SHEETS),
        sheet_rows={"People": [{"id": 1, ExcelParser.SHEET_NAME_COLUMN: "source value"}]},
    )

    with pytest.raises(RecordParseError):
        list(parser.parse_records(config, file, stream_reader, logger))
