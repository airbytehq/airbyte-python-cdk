# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
import csv
import os
from io import BytesIO
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock

import pandas  # noqa: F401  # loaded here so limit_memory measures CSV parsing, not the extractor's lazy import
import pytest
import requests
import requests_mock

from airbyte_cdk.sources.declarative.extractors import ResponseToFileExtractor


class ResponseToFileExtractorTest(TestCase):
    def setUp(self) -> None:
        self._extractor = ResponseToFileExtractor({})
        self._preserving_extractor = ResponseToFileExtractor({}, preserve_na_values=True)
        self._http_mocker = requests_mock.Mocker()
        self._http_mocker.__enter__()

    def tearDown(self) -> None:
        self._http_mocker.__exit__(None, None, None)

    def test_compressed_response(self) -> None:
        response = self._mock_streamed_response_from_file(self._compressed_response_path())
        extracted_records = list(self._extractor.extract_records(response))
        assert len(extracted_records) == 24

    def test_text_response(self) -> None:
        response = self._mock_streamed_response_from_file(self._decompressed_response_path())
        extracted_records = list(self._extractor.extract_records(response))
        assert len(extracted_records) == 24

    def test_text_response_with_null_bytes(self) -> None:
        csv_with_null_bytes = '"FIRST_\x00NAME","LAST_NAME"\n"a first n\x00ame","a last na\x00me"\n'
        response = self._mock_streamed_response(BytesIO(csv_with_null_bytes.encode("utf-8")))

        extracted_records = list(self._extractor.extract_records(response))

        assert extracted_records == [{"FIRST_NAME": "a first name", "LAST_NAME": "a last name"}]

    def test_na_string_values_are_preserved_when_enabled(self) -> None:
        pandas_na_tokens = ["NA", "N/A", "NULL", "None", "nan", "null", "#N/A", "<NA>"]
        csv_rows = "\n".join(f'"{token}","{token} value"' for token in pandas_na_tokens)
        csv_with_na = f'"region","name"\n{csv_rows}\n'
        response = self._mock_streamed_response(BytesIO(csv_with_na.encode("utf-8")))

        extracted_records = list(self._preserving_extractor.extract_records(response))

        assert len(extracted_records) == len(pandas_na_tokens)
        for record, token in zip(extracted_records, pandas_na_tokens):
            assert record["region"] == token, (
                f"Expected '{token}' to be preserved but got {record['region']}"
            )
            assert record["name"] == f"{token} value"

    def test_na_string_values_are_converted_to_none_by_default(self) -> None:
        pandas_na_tokens = ["NA", "N/A", "NULL", "None", "nan", "null", "#N/A", "<NA>"]
        csv_rows = "\n".join(f'"{token}","preserved"' for token in pandas_na_tokens)
        csv_with_na = f'"region","name"\n{csv_rows}\n'
        response = self._mock_streamed_response(BytesIO(csv_with_na.encode("utf-8")))

        extracted_records = list(self._extractor.extract_records(response))

        assert len(extracted_records) == len(pandas_na_tokens)
        for record in extracted_records:
            assert record["region"] is None
            assert record["name"] == "preserved"

    def test_empty_fields_are_converted_to_none_when_preserving(self) -> None:
        csv_with_empty = '"region","name"\n"NA","North America"\n,""\n'
        response = self._mock_streamed_response(BytesIO(csv_with_empty.encode("utf-8")))

        extracted_records = list(self._preserving_extractor.extract_records(response))

        assert extracted_records == [
            {"region": "NA", "name": "North America"},
            {"region": None, "name": None},
        ]

    def _test_folder_path(self) -> Path:
        return Path(__file__).parent.resolve()

    def _compressed_response_path(self) -> Path:
        return self._test_folder_path() / "compressed_response"

    def _decompressed_response_path(self) -> Path:
        return self._test_folder_path() / "decompressed_response.csv"

    def _mock_streamed_response_from_file(self, path: Path) -> requests.Response:
        with path.open("rb") as f:
            return self._mock_streamed_response(f)  # type: ignore  # Could not find the right typing for file io

    def _mock_streamed_response(self, io: BytesIO) -> requests.Response:
        any_url = "https://anyurl.com"
        self._http_mocker.register_uri("GET", any_url, [{"body": io, "status_code": 200}])
        return requests.get(any_url)


@pytest.fixture(name="large_events_response")
def large_event_response_fixture():
    lines_in_response = 2_000_000  # ≈ 62 MB of response
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = f"{dir_path}/test_response.csv"
    with open(file_path, "w") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["username", "email"])  # headers
        for _ in range(lines_in_response):
            csv_writer.writerow(["a_username", "email1@example.com"])
    yield (lines_in_response, file_path)
    os.remove(file_path)


@pytest.mark.slow
@pytest.mark.limit_memory("20 MB")
def test_response_to_file_extractor_memory_usage(requests_mock, large_events_response):
    lines_in_response, file_path = large_events_response
    extractor = ResponseToFileExtractor({})

    url = "https://for-all-mankind.nasa.com/api/v1/users/users1"
    requests_mock.get(url, body=open(file_path, "rb"))

    counter = 0
    for _ in extractor.extract_records(requests.get(url, stream=True)):
        counter += 1

    assert counter == lines_in_response


@pytest.fixture
def scratch_dir(tmp_path, monkeypatch):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setenv("TMPDIR", str(scratch))
    monkeypatch.setattr("tempfile.tempdir", None)
    return scratch


def test_extractor_writes_its_scratch_file_to_the_temp_directory(
    tmp_path, monkeypatch, requests_mock, scratch_dir
):
    working_dir = tmp_path / "app"
    working_dir.mkdir()
    monkeypatch.chdir(working_dir)
    requests_mock.get("https://a-url.com", content=b"id,name\n1,first\n2,second\n")
    extractor = ResponseToFileExtractor({})
    read_paths = []
    read_with_chunks = extractor._read_with_chunks

    def spy(path, *args, **kwargs):
        read_paths.append(path)
        assert os.path.isfile(path)
        yield from read_with_chunks(path, *args, **kwargs)

    monkeypatch.setattr(extractor, "_read_with_chunks", spy)

    records = list(extractor.extract_records(requests.get("https://a-url.com", stream=True)))

    assert records == [{"id": "1", "name": "first"}, {"id": "2", "name": "second"}]
    assert [os.path.dirname(path) for path in read_paths] == [str(scratch_dir)]
    assert list(working_dir.iterdir()) == []
    assert list(scratch_dir.iterdir()) == []


def test_interrupted_download_removes_its_scratch_file(tmp_path, monkeypatch, scratch_dir):
    working_dir = tmp_path / "app"
    working_dir.mkdir()
    monkeypatch.chdir(working_dir)

    def broken_body(chunk_size):
        yield b"id,name\n1,first\n"
        raise requests.exceptions.ChunkedEncodingError("connection reset")

    response = Mock(headers={}, iter_content=broken_body)

    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        list(ResponseToFileExtractor({}).extract_records(response))

    assert list(working_dir.iterdir()) == []
    assert list(scratch_dir.iterdir()) == []
