#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import asyncio
import datetime
import warnings
from io import BytesIO
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, mock_open, patch

import pandas as pd
import pytest

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
        format=ExcelFormat(sheet_name="Sheet1"),
        validation_policy=ValidationPolicy.emit_record,
    )


@pytest.fixture
def remote_file():
    return RemoteFile(uri="s3://mybucket/test.xlsx", last_modified=datetime.datetime.now())


@pytest.fixture
def setup_parser(remote_file):
    parser = ExcelParser()

    # Sample data for the mock Excel file
    data = pd.DataFrame(
        {
            "column1": [1, 2, 3],
            "column2": ["a", "b", "c"],
            "column3": [True, False, True],
            "column4": pd.to_datetime(["2021-01-01", "2022-01-01", "2023-01-01"]),
        }
    )

    # Convert the DataFrame to an Excel byte stream
    excel_bytes = BytesIO()
    with pd.ExcelWriter(excel_bytes, engine="xlsxwriter") as writer:
        data.to_excel(writer, index=False)
    excel_bytes.seek(0)

    # Mock the stream_reader's open_file method to return the Excel byte stream
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
@pytest.mark.asyncio
async def test_infer_schema(mock_excel_file, setup_parser):
    parser, config, file, stream_reader, logger, data = setup_parser

    # Mock the parse method of the pandas ExcelFile object
    mock_excel_file.return_value.parse.return_value = data

    # Call infer_schema
    schema = await parser.infer_schema(config, file, stream_reader, logger)

    # Define the expected schema
    expected_schema: SchemaType = {
        "column1": {"type": "number"},
        "column2": {"type": "string"},
        "column3": {"type": "boolean"},
        "column4": {"type": "string", "format": "date-time"},
    }

    # Validate the schema
    assert schema == expected_schema

    # Assert that the stream_reader's open_file was called correctly
    stream_reader.open_file.assert_called_once_with(
        file, parser.file_read_mode, parser.ENCODING, logger
    )

    # Assert that the logger was not used for warnings/errors
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

    def calamine_parse_side_effect(**kwargs):
        raise FakePanic(
            "failed to construct date: PyErr { type: <class 'ValueError'>, value: ValueError('year 20225 is out of range'), traceback: None }"
        )

    calamine_excel_file.parse.side_effect = calamine_parse_side_effect

    openpyxl_excel_file = MagicMock()

    def openpyxl_parse_side_effect(**kwargs):
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
    openpyxl_excel_file.parse.assert_called_once_with(sheet_name=0)


SHEET_NAME_COL = ExcelParser.ab_sheet_name_col


def _make_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    """Creates an in-memory Excel workbook from a mapping of worksheet name to frame."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, index=False, sheet_name=name)
    return buf.getvalue()


def _make_multisheet_excel_bytes() -> bytes:
    """Creates an in-memory Excel workbook with two sheets for testing."""
    return _make_excel_bytes(
        {
            "First": pd.DataFrame({"col_a": ["first"], "shared": [1]}),
            "Second": pd.DataFrame({"col_b": [2.5], "shared": [2]}),
        }
    )


def _stream_reader_for(excel_bytes: bytes) -> MagicMock:
    reader = MagicMock(spec=AbstractFileBasedStreamReader)
    reader.open_file.return_value = BytesIO(excel_bytes)
    return reader


def _parse(excel_bytes: bytes, remote_file, **format_kwargs) -> List[Dict[str, Any]]:
    parser = ExcelParser()
    config = FileBasedStreamConfig(name="test_stream", format=ExcelFormat(**format_kwargs))
    return list(
        parser.parse_records(config, remote_file, _stream_reader_for(excel_bytes), MagicMock())
    )


def _infer(excel_bytes: bytes, remote_file, **format_kwargs) -> Dict[str, Any]:
    parser = ExcelParser()
    config = FileBasedStreamConfig(name="test_stream", format=ExcelFormat(**format_kwargs))
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            parser.infer_schema(config, remote_file, _stream_reader_for(excel_bytes), MagicMock())
        )
    finally:
        loop.close()


@pytest.mark.parametrize(
    "format_kwargs,expected_records",
    [
        pytest.param(
            {},
            [{"col_a": "first", "shared": 1}],
            id="unset_reads_first_sheet",
        ),
        pytest.param(
            {"sheet_name": "Second"},
            [{"col_b": 2.5, "shared": 2}],
            id="sheet_by_name",
        ),
        pytest.param(
            {"sheet_name": "*"},
            [
                {"col_a": "first", "shared": 1, SHEET_NAME_COL: "First"},
                {"col_b": 2.5, "shared": 2, SHEET_NAME_COL: "Second"},
            ],
            id="all_sheets_tagged_with_origin",
        ),
    ],
)
def test_parse_records_selects_configured_sheet(format_kwargs, expected_records, remote_file):
    assert _parse(_make_multisheet_excel_bytes(), remote_file, **format_kwargs) == expected_records


def test_parse_records_without_sheet_name_adds_no_provenance(remote_file):
    """The pre-existing single-sheet contract must be byte-for-byte unchanged."""
    records = _parse(_make_multisheet_excel_bytes(), remote_file)

    assert all(SHEET_NAME_COL not in record for record in records)


@pytest.mark.parametrize(
    "format_kwargs,expected_schema",
    [
        pytest.param(
            {},
            {"col_a": {"type": "string"}, "shared": {"type": "number"}},
            id="first_sheet_schema",
        ),
        pytest.param(
            {"sheet_name": "*"},
            {
                "col_a": {"type": "string"},
                "col_b": {"type": "number"},
                "shared": {"type": "number"},
                SHEET_NAME_COL: {"type": "string"},
            },
            id="all_sheets_merged_schema",
        ),
    ],
)
def test_infer_schema_with_sheet_selection(format_kwargs, expected_schema, remote_file):
    assert _infer(_make_multisheet_excel_bytes(), remote_file, **format_kwargs) == expected_schema


@pytest.mark.parametrize(
    "config_value,expected",
    [
        pytest.param(None, 0, id="unset_means_first_sheet"),
        pytest.param("MySheet", "MySheet", id="named_sheet"),
        pytest.param("2026", "2026", id="numeric_name_stays_a_name"),
        pytest.param("0", "0", id="zero_is_a_name_not_an_index"),
        pytest.param("*", None, id="all_sheets"),
    ],
)
def test_resolve_sheet_name(config_value, expected):
    parser = ExcelParser()
    fmt = ExcelFormat(sheet_name=config_value)
    assert parser._resolve_sheet_name(fmt) == expected


def test_parse_records_reads_worksheet_with_numeric_name(remote_file):
    """Worksheets named for fiscal years must resolve by name, not by position."""
    excel_bytes = _make_excel_bytes(
        {
            "2026": pd.DataFrame({"month": ["Jan"], "total": [10]}),
            "2025": pd.DataFrame({"month": ["Feb"], "total": [8]}),
        }
    )

    assert _parse(excel_bytes, remote_file, sheet_name="2026") == [{"month": "Jan", "total": 10}]
    assert _parse(excel_bytes, remote_file, sheet_name="2025") == [{"month": "Feb", "total": 8}]


def test_parse_records_preserves_workbook_sheet_order(remote_file):
    excel_bytes = _make_excel_bytes(
        {
            "Alpha": pd.DataFrame({"v": [1, 2]}),
            "Beta": pd.DataFrame({"v": [3]}),
            "Gamma": pd.DataFrame({"v": [4]}),
        }
    )

    records = _parse(excel_bytes, remote_file, sheet_name="*")

    assert [record[SHEET_NAME_COL] for record in records] == [
        "Alpha",
        "Alpha",
        "Beta",
        "Gamma",
    ]


def test_all_sheets_on_single_sheet_workbook(remote_file):
    excel_bytes = _make_excel_bytes({"Only": pd.DataFrame({"a": [1]})})

    assert _parse(excel_bytes, remote_file, sheet_name="*") == [{"a": 1, SHEET_NAME_COL: "Only"}]


@pytest.mark.parametrize(
    "sheet_name",
    [
        pytest.param("Missing", id="absent_worksheet"),
        pytest.param("first", id="wrong_case"),
    ],
)
def test_missing_worksheet_raises_config_error(sheet_name, remote_file):
    """A bad worksheet name is a config mistake and must not be reported as a parse failure."""
    with pytest.raises(ConfigValidationError) as exc_info:
        _parse(_make_multisheet_excel_bytes(), remote_file, sheet_name=sheet_name)

    message = str(exc_info.value)
    assert repr(sheet_name) in message
    assert "'First'" in message and "'Second'" in message
    assert "case-sensitive" in message
    assert "mismatch between the config's file type" not in message


def test_unparseable_file_still_raises_record_parse_error(remote_file):
    """Narrowing the error handling must not mask genuinely corrupt files."""
    with pytest.raises(RecordParseError):
        _parse(b"this is not an excel file at all", remote_file)


def _spy_on_sheet_parsing(monkeypatch) -> List[Any]:
    """Records the sheet_name argument of every pandas parse call, in order."""
    parsed: List[Any] = []
    original = pd.ExcelFile.parse

    def spy(self, *args, **kwargs):
        parsed.append(kwargs.get("sheet_name", args[0] if args else None))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pd.ExcelFile, "parse", spy)
    return parsed


def test_all_sheets_parses_worksheets_lazily(remote_file, monkeypatch):
    """Worksheets are parsed on demand.

    Asking pandas for every worksheet at once builds one frame per worksheet before a
    single record is emitted, so peak memory tracks the whole workbook. Parsing lazily
    keeps it at one worksheet regardless of how many the workbook holds.
    """
    excel_bytes = _make_excel_bytes(
        {
            "A": pd.DataFrame({"v": [1]}),
            "B": pd.DataFrame({"v": [2]}),
            "C": pd.DataFrame({"v": [3]}),
        }
    )
    parsed = _spy_on_sheet_parsing(monkeypatch)
    parser = ExcelParser()
    config = FileBasedStreamConfig(name="test_stream", format=ExcelFormat(sheet_name="*"))
    stream = parser.parse_records(config, remote_file, _stream_reader_for(excel_bytes), MagicMock())

    next(stream)
    assert parsed == ["A"], "later worksheets must not be parsed before they are requested"

    assert list(stream) == [
        {"v": 2, SHEET_NAME_COL: "B"},
        {"v": 3, SHEET_NAME_COL: "C"},
    ]
    assert parsed == ["A", "B", "C"]


def test_single_sheet_parses_only_the_selected_worksheet(monkeypatch, remote_file):
    """Selecting one worksheet must not walk the rest of the workbook."""
    parsed = _spy_on_sheet_parsing(monkeypatch)

    _parse(_make_multisheet_excel_bytes(), remote_file, sheet_name="Second")

    assert parsed == ["Second"]
